"""
报告格式化器

将结构化报告转换为不同格式:
1. Markdown - 用于前端展示
2. WeChat - 企业微信适配格式
"""

from datetime import datetime
from typing import List

from .types import (
    Report,
    ReportSection,
    ReportItem,
    ReportType,
    EventImportance,
    SECTION_TITLES,
    SECTION_ORDER,
)


# 重要性标记
IMPORTANCE_MARKS = {
    EventImportance.HIGH: "🔴",
    EventImportance.MEDIUM: "🟡",
    EventImportance.LOW: "⚪",
}


class ReportFormatter:
    """
    报告格式化器
    
    Example:
        formatter = ReportFormatter()
        report.content_markdown = formatter.to_markdown(report)
        report.content_wechat = formatter.to_wechat(report)
    """
    
    def to_markdown(self, report: Report) -> str:
        """
        转换为 Markdown 格式
        
        用于前端 WebSocket 推送和存储展示。
        """
        lines = []
        
        # 标题
        lines.append(f"# {report.title}")
        lines.append("")
        
        # 发布时间
        time_str = report.created_at.strftime("%Y-%m-%d %H:%M")
        lines.append(f"> 发布时间: {time_str}")
        lines.append("")
        
        # 概述
        if report.overview:
            lines.append("## 📋 概述")
            lines.append("")
            lines.append(report.overview)
            lines.append("")
        
        # 统计
        stats = report.stats
        lines.append(f"---")
        lines.append(f"*本期共收录 {stats.event_count} 条事件 (来自 {stats.news_count} 条新闻)，其中重要事件 {stats.high_importance_count} 条*")
        lines.append("")
        
        # 各分节
        sorted_sections = sorted(
            report.sections,
            key=lambda s: SECTION_ORDER.get(s.category, 99)
        )
        
        for section in sorted_sections:
            if not section.items:
                continue
            
            lines.append(f"## {section.title}")
            lines.append("")
            
            # 分类摘要
            if section.summary:
                lines.append(f"*{section.summary}*")
                lines.append("")
            
            # 事件列表
            for item in section.items:
                mark = IMPORTANCE_MARKS.get(item.importance, "")
                time_str = ""
                if item.event_time:
                    time_str = f"[{item.event_time.strftime('%H:%M')}] "
                
                line = f"- {mark} {time_str}**{item.title}**"
                
                # 关联股票
                if item.ts_codes:
                    codes_str = ", ".join(item.ts_codes[:5])
                    if len(item.ts_codes) > 5:
                        codes_str += f"等{len(item.ts_codes)}只"
                    line += f" ({codes_str})"
                
                lines.append(line)
                
                # 摘要 (仅重要事件展示)
                if item.importance == EventImportance.HIGH and item.summary:
                    summary_preview = item.summary[:100]
                    if len(item.summary) > 100:
                        summary_preview += "..."
                    lines.append(f"  > {summary_preview}")
            
            lines.append("")
        
        # 页脚
        lines.append("---")
        lines.append("*本报告由 AI 自动生成，仅供参考*")
        
        return "\n".join(lines)
    
    def to_wechat(self, report: Report) -> str:
        """
        转换为企业微信 Markdown 格式
        
        企业微信支持的 Markdown 是简化版，需要适配:
        - 不支持 ** 加粗，使用 > 引用代替强调
        - 链接格式: [text](url)
        - 支持颜色: <font color="info">绿色</font>
        """
        lines = []
        
        # 标题
        report_type_name = "早报" if report.type == ReportType.MORNING else "午报"
        lines.append(f"### 📰 {report.date} {report_type_name}")
        lines.append("")
        
        # 概述
        if report.overview:
            lines.append(f"> {report.overview}")
            lines.append("")
        
        # 统计摘要
        stats = report.stats
        lines.append(f"本期: {stats.event_count}条事件 | 重要: {stats.high_importance_count}条")
        lines.append("")
        
        # 各分节 (简化版)
        sorted_sections = sorted(
            report.sections,
            key=lambda s: SECTION_ORDER.get(s.category, 99)
        )
        
        for section in sorted_sections:
            if not section.items:
                continue
            
            lines.append(f"**{section.title}**")
            
            # 只显示重要和中等事件
            important_items = [
                item for item in section.items
                if item.importance in (EventImportance.HIGH, EventImportance.MEDIUM)
            ][:5]  # 每类最多5条
            
            for item in important_items:
                if item.importance == EventImportance.HIGH:
                    lines.append(f"> 🔴 {item.title}")
                else:
                    lines.append(f"• {item.title}")
            
            # 显示还有多少条
            remaining = len(section.items) - len(important_items)
            if remaining > 0:
                lines.append(f"  <font color=\"comment\">...还有{remaining}条</font>")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def format_report(self, report: Report) -> Report:
        """
        一次性生成所有格式
        
        Args:
            report: 报告对象
            
        Returns:
            填充了格式化内容的报告对象
        """
        report.content_markdown = self.to_markdown(report)
        report.content_wechat = self.to_wechat(report)
        return report


def create_report_title(report_type: ReportType, date: str) -> str:
    """生成报告标题"""
    type_name = "早报" if report_type == ReportType.MORNING else "午报"
    return f"📰 {date} 财经{type_name}"


def create_report_id(report_type: ReportType, date: str) -> str:
    """生成报告ID"""
    return f"{report_type.value}_{date.replace('-', '')}"
