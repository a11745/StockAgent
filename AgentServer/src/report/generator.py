"""
报告生成器

协调事件获取、分析、格式化，生成完整报告。
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .types import (
    Report,
    ReportType,
    ReportSection,
    ReportItem,
    ReportStats,
    ReportCategory,
    ReportGenerateResult,
    EventImportance,
    EVENT_CATEGORY_MAP,
    SECTION_TITLES,
    SECTION_ORDER,
)
from .analyzer import EventAnalyzer
from .formatter import ReportFormatter, create_report_id, create_report_title


logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    报告生成器
    
    主要功能:
    1. 从 news_events 获取聚合后的事件
    2. 使用 LLM 分析重要性和生成摘要
    3. 格式化输出 Markdown 和企业微信格式
    4. 存储到 MongoDB
    5. 推送到前端和企业微信
    
    Example:
        generator = ReportGenerator()
        result = await generator.generate(
            report_type=ReportType.MORNING,
            trace_id="report_20260306"
        )
        if result.success:
            print(result.report.content_markdown)
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.analyzer = EventAnalyzer()
        self.formatter = ReportFormatter()
        
        self._mongo = None
        self._notification = None
        self._websocket = None
    
    async def _get_mongo(self):
        if self._mongo is None:
            from core.managers import mongo_manager
            if not mongo_manager.is_initialized:
                await mongo_manager.initialize()
            self._mongo = mongo_manager
        return self._mongo
    
    async def _get_notification(self):
        if self._notification is None:
            from core.managers import notification_manager
            if not notification_manager.is_initialized:
                await notification_manager.initialize()
            self._notification = notification_manager
        return self._notification
    
    async def _get_websocket(self):
        if self._websocket is None:
            from nodes.web.websocket import connection_manager
            self._websocket = connection_manager
        return self._websocket
    
    async def generate(
        self,
        report_type: ReportType,
        date: Optional[str] = None,
        hours_back: int = 12,
        trace_id: Optional[str] = None,
    ) -> ReportGenerateResult:
        """
        生成报告
        
        Args:
            report_type: 报告类型 (早报/午报)
            date: 日期 (默认今天)
            hours_back: 往前追溯小时数
            trace_id: 追踪ID
            
        Returns:
            ReportGenerateResult
        """
        start_time = time.time()
        result = ReportGenerateResult()
        
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        report_id = create_report_id(report_type, date)
        trace_id = trace_id or report_id
        
        self.logger.info(f"[{trace_id}] Generating {report_type.value} report for {date}")
        
        try:
            # 1. 获取聚合后的事件
            events = await self._fetch_events(hours_back, trace_id)
            
            if not events:
                self.logger.warning(f"[{trace_id}] No events found in the past {hours_back} hours")
                result.report = self._create_empty_report(report_type, date)
                return result
            
            self.logger.info(f"[{trace_id}] Found {len(events)} events")
            
            # 2. 分析事件重要性
            items = await self.analyzer.analyze_importance(events, trace_id)
            
            # 3. 按类别分组
            sections = self._group_by_category(items)
            
            # 4. 为每个分类生成摘要
            for section in sections:
                if section.items:
                    section.summary = await self.analyzer.generate_section_summary(
                        section.items, section.category, trace_id
                    )
            
            # 5. 生成总体概述
            report_type_name = "早报" if report_type == ReportType.MORNING else "午报"
            overview = await self.analyzer.generate_overview(
                sections, report_type_name, trace_id
            )
            
            # 6. 构建报告
            stats = self._calculate_stats(items, sections)
            
            report = Report(
                id=report_id,
                type=report_type,
                date=date,
                title=create_report_title(report_type, date),
                overview=overview,
                sections=sections,
                stats=stats,
            )
            
            # 7. 格式化
            self.formatter.format_report(report)
            
            # 8. 存储
            await self._save_report(report, trace_id)
            
            result.report = report
            result.elapsed_ms = (time.time() - start_time) * 1000
            
            self.logger.info(
                f"[{trace_id}] Report generated: {len(items)} events, "
                f"{stats.high_importance_count} important, "
                f"elapsed={result.elapsed_ms:.0f}ms"
            )
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Report generation failed: {e}", exc_info=True)
            result.success = False
            result.errors.append(str(e))
        
        return result
    
    async def _fetch_events(
        self,
        hours_back: int,
        trace_id: str,
    ) -> List[Dict[str, Any]]:
        """从 MongoDB 获取聚合后的事件"""
        mongo = await self._get_mongo()
        
        cutoff = datetime.utcnow() - timedelta(hours=hours_back)
        
        events = await mongo.find_many(
            "news_events",
            {
                "last_update_time": {"$gte": cutoff},
            },
            limit=200,
            sort=[("news_count", -1), ("last_update_time", -1)],
        )
        
        return events
    
    def _group_by_category(self, items: List[ReportItem]) -> List[ReportSection]:
        """按类别分组事件"""
        category_items: Dict[ReportCategory, List[ReportItem]] = {
            ReportCategory.MACRO: [],
            ReportCategory.INDUSTRY: [],
            ReportCategory.STOCK: [],
            ReportCategory.HOT: [],
        }
        
        for item in items:
            # 从原始事件获取分类
            event_category = item.raw_event.get("category", "general")
            report_category = EVENT_CATEGORY_MAP.get(event_category, ReportCategory.HOT)
            
            category_items[report_category].append(item)
        
        # 每个分类内按重要性和新闻数排序
        sections = []
        for category, cat_items in category_items.items():
            # 排序: 重要性 > 新闻数量
            sorted_items = sorted(
                cat_items,
                key=lambda x: (
                    0 if x.importance == EventImportance.HIGH else (
                        1 if x.importance == EventImportance.MEDIUM else 2
                    ),
                    -x.news_count,
                ),
            )
            
            section = ReportSection(
                category=category,
                title=SECTION_TITLES.get(category, str(category)),
                items=sorted_items[:15],  # 每类最多15条
                item_count=len(sorted_items),
            )
            sections.append(section)
        
        # 按分类权重排序
        sections.sort(key=lambda s: SECTION_ORDER.get(s.category, 99))
        
        return sections
    
    def _calculate_stats(
        self,
        items: List[ReportItem],
        sections: List[ReportSection],
    ) -> ReportStats:
        """计算统计信息"""
        stats = ReportStats(
            event_count=len(items),
            news_count=sum(item.news_count for item in items),
            high_importance_count=sum(
                1 for item in items if item.importance == EventImportance.HIGH
            ),
        )
        
        # 各分类数量
        for section in sections:
            if section.category == ReportCategory.MACRO:
                stats.macro_count = len(section.items)
            elif section.category == ReportCategory.INDUSTRY:
                stats.industry_count = len(section.items)
            elif section.category == ReportCategory.STOCK:
                stats.stock_count = len(section.items)
            elif section.category == ReportCategory.HOT:
                stats.hot_count = len(section.items)
        
        # 时间范围
        event_times = [
            item.event_time for item in items if item.event_time
        ]
        if event_times:
            stats.time_range_start = min(event_times)
            stats.time_range_end = max(event_times)
        
        return stats
    
    def _create_empty_report(
        self,
        report_type: ReportType,
        date: str,
    ) -> Report:
        """创建空报告"""
        return Report(
            id=create_report_id(report_type, date),
            type=report_type,
            date=date,
            title=create_report_title(report_type, date),
            overview="今日暂无重大财经事件。",
            sections=[],
            stats=ReportStats(),
            content_markdown="# " + create_report_title(report_type, date) + "\n\n暂无内容",
            content_wechat="### " + create_report_title(report_type, date) + "\n\n暂无内容",
        )
    
    async def _save_report(
        self,
        report: Report,
        trace_id: str,
    ) -> None:
        """保存报告到 MongoDB"""
        mongo = await self._get_mongo()
        
        try:
            await mongo.update_one(
                "reports",
                {"_id": report.id},
                {"$set": report.to_mongo_doc()},
                upsert=True,
            )
            self.logger.info(f"[{trace_id}] Report saved: {report.id}")
        except Exception as e:
            self.logger.error(f"[{trace_id}] Save report failed: {e}")
            raise
    
    async def push_to_wechat(
        self,
        report: Report,
        trace_id: Optional[str] = None,
    ) -> bool:
        """推送到企业微信"""
        try:
            notification = await self._get_notification()
            
            await notification.send_markdown(
                content=report.content_wechat,
                mentioned_list=["@all"],  # 可配置
            )
            
            # 更新推送状态
            mongo = await self._get_mongo()
            await mongo.update_one(
                "reports",
                {"_id": report.id},
                {"$set": {"pushed.wechat": True, "pushed_wechat_at": datetime.utcnow()}}
            )
            
            self.logger.info(f"[{trace_id}] Report pushed to WeChat: {report.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Push to WeChat failed: {e}")
            return False
    
    async def push_to_websocket(
        self,
        report: Report,
        trace_id: Optional[str] = None,
    ) -> bool:
        """推送到前端 WebSocket"""
        try:
            ws = await self._get_websocket()
            
            message = {
                "type": "report",
                "report_type": report.type.value,
                "report_id": report.id,
                "title": report.title,
                "overview": report.overview,
                "content": report.content_markdown,
                "stats": report.stats.model_dump(),
                "created_at": report.created_at.isoformat(),
            }
            
            # 广播给所有连接的用户
            await ws.broadcast(message)
            
            # 更新推送状态
            mongo = await self._get_mongo()
            await mongo.update_one(
                "reports",
                {"_id": report.id},
                {"$set": {"pushed.websocket": True, "pushed_websocket_at": datetime.utcnow()}}
            )
            
            self.logger.info(f"[{trace_id}] Report pushed to WebSocket: {report.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Push to WebSocket failed: {e}")
            return False
    
    async def generate_and_push(
        self,
        report_type: ReportType,
        date: Optional[str] = None,
        hours_back: int = 12,
        push_wechat: bool = True,
        push_websocket: bool = True,
        trace_id: Optional[str] = None,
    ) -> ReportGenerateResult:
        """
        生成并推送报告
        
        一站式方法，生成报告后推送到各渠道。
        """
        result = await self.generate(
            report_type=report_type,
            date=date,
            hours_back=hours_back,
            trace_id=trace_id,
        )
        
        if not result.success or not result.report:
            return result
        
        report = result.report
        
        # 推送到企业微信
        if push_wechat:
            result.pushed_wechat = await self.push_to_wechat(report, trace_id)
        
        # 推送到 WebSocket
        if push_websocket:
            result.pushed_websocket = await self.push_to_websocket(report, trace_id)
        
        return result
    
    async def get_report(
        self,
        report_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[Report]:
        """获取已生成的报告"""
        mongo = await self._get_mongo()
        
        doc = await mongo.find_one("reports", {"_id": report_id})
        if doc:
            return Report.from_mongo_doc(doc)
        return None
    
    async def list_reports(
        self,
        limit: int = 20,
        report_type: Optional[ReportType] = None,
        trace_id: Optional[str] = None,
    ) -> List[Report]:
        """列出最近的报告"""
        mongo = await self._get_mongo()
        
        query = {}
        if report_type:
            query["type"] = report_type.value
        
        docs = await mongo.find_many(
            "reports",
            query,
            limit=limit,
            sort=[("created_at", -1)],
        )
        
        return [Report.from_mongo_doc(doc) for doc in docs]
