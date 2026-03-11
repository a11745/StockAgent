"""
报告类型定义

定义报告相关的数据结构，用于早报/午报生成。
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class ReportType(str, Enum):
    """报告类型"""
    MORNING = "morning"    # 早报 (8:50)
    NOON = "noon"          # 午报 (13:50)


class ReportCategory(str, Enum):
    """报告分类"""
    MACRO = "macro"        # 宏观政策
    INDUSTRY = "industry"  # 行业动态
    STOCK = "stock"        # 个股异动
    HOT = "hot"            # 热点事件


# 事件分类 -> 报告分类映射
EVENT_CATEGORY_MAP = {
    # 政策类 -> 宏观政策
    "policy_release": ReportCategory.MACRO,
    "policy_interpret": ReportCategory.MACRO,
    "policy_notice": ReportCategory.MACRO,
    "policy_standard": ReportCategory.MACRO,
    
    # 行业类 -> 行业动态
    "industry_news": ReportCategory.INDUSTRY,
    "industry_analysis": ReportCategory.INDUSTRY,
    
    # 公司类 -> 个股异动
    "company_announce": ReportCategory.STOCK,
    "company_news": ReportCategory.STOCK,
    
    # 财经类 -> 热点事件
    "finance_flash": ReportCategory.HOT,
    "finance_article": ReportCategory.HOT,
    "finance_report": ReportCategory.HOT,
    "general": ReportCategory.HOT,
}

# 分类标题
SECTION_TITLES = {
    ReportCategory.MACRO: "🏛️ 宏观政策",
    ReportCategory.INDUSTRY: "📊 行业动态",
    ReportCategory.STOCK: "📈 个股异动",
    ReportCategory.HOT: "🔥 热点事件",
}

# 分类排序权重 (数字越小越靠前)
SECTION_ORDER = {
    ReportCategory.MACRO: 1,
    ReportCategory.INDUSTRY: 2,
    ReportCategory.STOCK: 3,
    ReportCategory.HOT: 4,
}


class EventImportance(str, Enum):
    """事件重要性"""
    HIGH = "high"        # 重大事件
    MEDIUM = "medium"    # 一般事件
    LOW = "low"          # 次要事件


class ReportItem(BaseModel):
    """报告条目 (单个事件)"""
    event_id: str = Field(description="事件ID")
    title: str = Field(description="事件标题")
    summary: str = Field(default="", description="事件摘要")
    importance: EventImportance = Field(default=EventImportance.MEDIUM)
    news_count: int = Field(default=1, description="相关新闻数量")
    ts_codes: List[str] = Field(default_factory=list, description="关联股票")
    event_time: Optional[datetime] = Field(default=None, description="事件时间")
    
    # 原始事件数据 (用于 LLM 分析)
    raw_event: Dict[str, Any] = Field(default_factory=dict)


class ReportSection(BaseModel):
    """报告分节"""
    category: ReportCategory = Field(description="分类")
    title: str = Field(description="分类标题")
    items: List[ReportItem] = Field(default_factory=list, description="事件列表")
    summary: str = Field(default="", description="LLM 生成的段落摘要")
    item_count: int = Field(default=0, description="条目数量")
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.item_count:
            self.item_count = len(self.items)


class ReportStats(BaseModel):
    """报告统计"""
    event_count: int = Field(default=0, description="事件数量")
    news_count: int = Field(default=0, description="新闻数量")
    high_importance_count: int = Field(default=0, description="重要事件数量")
    time_range_start: Optional[datetime] = Field(default=None)
    time_range_end: Optional[datetime] = Field(default=None)
    
    # 各分类数量
    macro_count: int = 0
    industry_count: int = 0
    stock_count: int = 0
    hot_count: int = 0


class Report(BaseModel):
    """报告"""
    # 标识
    id: str = Field(description="报告ID (如 morning_20260306)")
    type: ReportType = Field(description="报告类型")
    date: str = Field(description="日期 (YYYY-MM-DD)")
    
    # 内容
    title: str = Field(description="报告标题")
    overview: str = Field(default="", description="总体概述 (LLM 生成)")
    sections: List[ReportSection] = Field(default_factory=list)
    
    # 格式化内容
    content_markdown: str = Field(default="", description="完整 Markdown")
    content_wechat: str = Field(default="", description="企业微信适配格式")
    
    # 统计
    stats: ReportStats = Field(default_factory=ReportStats)
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.utcnow)
    pushed: Dict[str, bool] = Field(
        default_factory=lambda: {"wechat": False, "websocket": False}
    )
    
    def to_mongo_doc(self) -> Dict[str, Any]:
        """转换为 MongoDB 文档"""
        doc = self.model_dump()
        doc["_id"] = self.id
        return doc
    
    @classmethod
    def from_mongo_doc(cls, doc: Dict[str, Any]) -> "Report":
        """从 MongoDB 文档创建"""
        if "_id" in doc:
            doc["id"] = doc.pop("_id")
        return cls(**doc)


class ReportGenerateResult(BaseModel):
    """报告生成结果"""
    success: bool = True
    report: Optional[Report] = None
    errors: List[str] = Field(default_factory=list)
    elapsed_ms: float = 0
    
    # 推送结果
    pushed_wechat: bool = False
    pushed_websocket: bool = False
