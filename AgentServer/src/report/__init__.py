"""
报告生成模块

提供早报/午报生成功能:
- 从聚合后的新闻事件生成报告
- LLM 分析重要性和生成摘要
- 格式化输出 (Markdown/企业微信)
- 多渠道推送 (WebSocket/企业微信)

Example:
    from src.report import ReportGenerator, ReportType
    
    generator = ReportGenerator()
    result = await generator.generate_and_push(
        report_type=ReportType.MORNING,
        push_wechat=True,
        push_websocket=True,
    )
"""

from .types import (
    ReportType,
    ReportCategory,
    EventImportance,
    ReportItem,
    ReportSection,
    ReportStats,
    Report,
    ReportGenerateResult,
    EVENT_CATEGORY_MAP,
    SECTION_TITLES,
    SECTION_ORDER,
)
from .analyzer import EventAnalyzer
from .formatter import ReportFormatter, create_report_id, create_report_title
from .generator import ReportGenerator


__all__ = [
    # Types
    "ReportType",
    "ReportCategory",
    "EventImportance",
    "ReportItem",
    "ReportSection",
    "ReportStats",
    "Report",
    "ReportGenerateResult",
    
    # Constants
    "EVENT_CATEGORY_MAP",
    "SECTION_TITLES",
    "SECTION_ORDER",
    
    # Classes
    "EventAnalyzer",
    "ReportFormatter",
    "ReportGenerator",
    
    # Functions
    "create_report_id",
    "create_report_title",
]
