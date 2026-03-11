"""
事件分析器

使用 LLM 对聚合后的事件进行：
1. 重要性评估
2. 摘要生成
3. 分类汇总
"""

import logging
import json
from typing import Any, Dict, List, Optional

from .types import (
    ReportItem,
    ReportSection,
    ReportCategory,
    EventImportance,
    SECTION_TITLES,
)


logger = logging.getLogger(__name__)


class EventAnalyzer:
    """
    事件分析器
    
    使用 LLM 服务分析事件重要性和生成摘要。
    
    Example:
        analyzer = EventAnalyzer()
        items = await analyzer.analyze_importance(events)
        summary = await analyzer.generate_section_summary(items, "宏观政策")
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._llm_service = None
    
    async def _get_llm_service(self):
        """延迟获取 LLM 服务"""
        if self._llm_service is None:
            from src.llm import llm_service
            if not llm_service._initialized:
                await llm_service.initialize()
            self._llm_service = llm_service
        return self._llm_service
    
    async def analyze_importance(
        self,
        events: List[Dict[str, Any]],
        trace_id: Optional[str] = None,
    ) -> List[ReportItem]:
        """
        分析事件重要性
        
        Args:
            events: 事件列表 (从 news_events 获取)
            trace_id: 追踪ID
            
        Returns:
            带重要性评估的 ReportItem 列表
        """
        if not events:
            return []
        
        items = []
        
        # 先将事件转换为 ReportItem
        for event in events:
            item = ReportItem(
                event_id=event.get("id", ""),
                title=event.get("title", ""),
                summary=event.get("summary", ""),
                importance=EventImportance.MEDIUM,  # 默认
                news_count=event.get("news_count", 1),
                ts_codes=event.get("ts_codes", []),
                event_time=event.get("first_report_time"),
                raw_event=event,
            )
            items.append(item)
        
        # 如果事件较少，使用规则评估而非 LLM
        if len(items) <= 5:
            return self._rule_based_importance(items)
        
        # 使用 LLM 服务批量评估
        try:
            llm = await self._get_llm_service()
            
            # 构建事件 JSON
            events_for_llm = []
            for item in items:
                events_for_llm.append({
                    "event_id": item.event_id,
                    "title": item.title,
                    "summary": item.summary[:200] if item.summary else "",
                    "news_count": item.news_count,
                })
            
            events_json = json.dumps(events_for_llm, ensure_ascii=False, indent=2)
            
            # 使用模板调用
            result = await llm.invoke_and_parse(
                "importance_assess",
                events_json=events_json,
            )
            
            # 解析结果
            if result and "results" in result:
                importance_map = self._parse_importance_results(result["results"])
                
                # 更新 items
                for item in items:
                    if item.event_id in importance_map:
                        item.importance = importance_map[item.event_id]
            
            self.logger.info(f"[{trace_id}] Analyzed importance for {len(items)} events")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] LLM importance analysis failed: {e}")
            # 降级到规则评估
            items = self._rule_based_importance(items)
        
        return items
    
    def _rule_based_importance(self, items: List[ReportItem]) -> List[ReportItem]:
        """基于规则的重要性评估"""
        for item in items:
            # 规则1: 新闻数量多 = 重要
            if item.news_count >= 5:
                item.importance = EventImportance.HIGH
            elif item.news_count >= 3:
                item.importance = EventImportance.MEDIUM
            else:
                item.importance = EventImportance.LOW
            
            # 规则2: 关键词匹配
            keywords_high = ["央行", "降息", "加息", "降准", "涨停", "跌停", "重大", "紧急"]
            keywords_medium = ["政策", "利好", "利空", "发布", "公告"]
            
            title_lower = item.title.lower()
            for kw in keywords_high:
                if kw in title_lower:
                    item.importance = EventImportance.HIGH
                    break
            
            if item.importance != EventImportance.HIGH:
                for kw in keywords_medium:
                    if kw in title_lower:
                        item.importance = EventImportance.MEDIUM
                        break
        
        return items
    
    def _parse_importance_results(self, results: List[Dict]) -> Dict[str, EventImportance]:
        """解析重要性评估结果"""
        importance_map = {}
        
        for result in results:
            event_id = result.get("event_id", "")
            importance_str = result.get("importance", "medium").lower()
            
            if importance_str == "high":
                importance = EventImportance.HIGH
            elif importance_str == "low":
                importance = EventImportance.LOW
            else:
                importance = EventImportance.MEDIUM
            
            importance_map[event_id] = importance
        
        return importance_map
    
    async def generate_section_summary(
        self,
        items: List[ReportItem],
        category: ReportCategory,
        trace_id: Optional[str] = None,
    ) -> str:
        """
        生成分类摘要
        
        Args:
            items: 该分类下的事件列表
            category: 分类
            trace_id: 追踪ID
            
        Returns:
            摘要文本
        """
        if not items:
            return ""
        
        # 如果只有1-2条，不需要 LLM 汇总
        if len(items) <= 2:
            return items[0].title if items else ""
        
        try:
            llm = await self._get_llm_service()
            
            # 构建事件文本
            events_text = "\n".join([
                f"- {item.title}" + (f" ({item.news_count}条相关)" if item.news_count > 1 else "")
                for item in items[:10]  # 最多10条
            ])
            
            category_name = SECTION_TITLES.get(category, str(category))
            
            # 使用模板调用
            summary = await llm.invoke_template(
                "report_summary",
                category=category_name,
                events_text=events_text,
            )
            
            # 清理结果
            summary = summary.strip()
            if summary.startswith('"') and summary.endswith('"'):
                summary = summary[1:-1]
            
            return summary[:200]  # 限制长度
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Generate section summary failed: {e}")
            # 降级：返回第一条标题
            return items[0].title if items else ""
    
    async def generate_overview(
        self,
        sections: List[ReportSection],
        report_type: str = "早报",
        trace_id: Optional[str] = None,
    ) -> str:
        """
        生成报告总体概述
        
        Args:
            sections: 所有分节
            report_type: 报告类型 (早报/午报)
            trace_id: 追踪ID
            
        Returns:
            概述文本
        """
        if not sections:
            return "今日暂无重大财经事件。"
        
        # 提取各分类摘要
        summaries = {
            ReportCategory.MACRO: "暂无",
            ReportCategory.INDUSTRY: "暂无",
            ReportCategory.STOCK: "暂无",
            ReportCategory.HOT: "暂无",
        }
        
        for section in sections:
            if section.items:
                summaries[section.category] = section.summary or section.items[0].title
        
        try:
            llm = await self._get_llm_service()
            
            # 使用模板调用
            overview = await llm.invoke_template(
                "report_overview",
                report_type=report_type,
                macro_summary=summaries[ReportCategory.MACRO],
                industry_summary=summaries[ReportCategory.INDUSTRY],
                stock_summary=summaries[ReportCategory.STOCK],
                hot_summary=summaries[ReportCategory.HOT],
            )
            
            return overview.strip()[:300]
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Generate overview failed: {e}")
            # 降级
            top_items = []
            for section in sections:
                if section.items:
                    top_items.append(section.items[0].title)
            return "今日要点: " + "; ".join(top_items[:3]) if top_items else ""
