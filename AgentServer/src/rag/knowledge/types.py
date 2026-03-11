"""
知识库类型定义
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class KnowledgeCategory(str, Enum):
    """知识大类"""
    MARKET_REVIEW = "market_review"              # 大盘复盘
    FACTOR = "factor"                            # 策略因子
    TECHNICAL = "technical"                      # 技术分析
    FUNDAMENTAL = "fundamental"                  # 基本面
    TRADING_PSYCHOLOGY = "trading_psychology"    # 交易心理


class FixedKnowledgeCategory(str, Enum):
    """固定知识细分类"""
    # 大盘复盘
    MARKET_REVIEW_OPEN = "market_review_open"           # 开盘分析
    MARKET_REVIEW_INTRADAY = "market_review_intraday"   # 盘中观察
    MARKET_REVIEW_CLOSE = "market_review_close"         # 收盘复盘
    MARKET_REVIEW_WEEKLY = "market_review_weekly"       # 周复盘
    
    # 策略因子
    FACTOR_VOLUME_PRICE = "factor_volume_price"         # 量价因子
    FACTOR_MOMENTUM = "factor_momentum"                 # 动量因子
    FACTOR_SENTIMENT = "factor_sentiment"               # 情绪因子
    FACTOR_FUNDAMENTAL = "factor_fundamental"           # 基本面因子
    FACTOR_TECHNICAL = "factor_technical"               # 技术因子
    
    # 技术分析 - 筹码
    TECH_CHIP_PEAK = "tech_chip_peak"                   # 筹码峰
    TECH_CHIP_DISTRIBUTION = "tech_chip_distribution"   # 筹码分布
    TECH_CHIP_COST = "tech_chip_cost"                   # 成本分析
    
    # 技术分析 - K线
    TECH_CANDLESTICK_SINGLE = "tech_candlestick_single"     # 单根K线
    TECH_CANDLESTICK_DOUBLE = "tech_candlestick_double"     # 双K组合
    TECH_CANDLESTICK_PATTERN = "tech_candlestick_pattern"   # K线形态
    
    # 技术分析 - 其他
    TECH_MOVING_AVERAGE = "tech_moving_average"         # 均线系统
    TECH_SUPPORT_RESISTANCE = "tech_support_resistance" # 支撑阻力
    TECH_TREND = "tech_trend"                           # 趋势分析
    TECH_VOLUME = "tech_volume"                         # 成交量分析
    
    # 交易心理
    PSYCHOLOGY_BIAS = "psychology_bias"                 # 心理偏差
    PSYCHOLOGY_DISCIPLINE = "psychology_discipline"     # 交易纪律


class UserKnowledgeType(str, Enum):
    """用户知识类型"""
    TRADING_RULE = "trading_rule"           # 交易规则 (入场/出场/仓位)
    STRATEGY = "strategy"                   # 个人策略
    REVIEW_TEMPLATE = "review_template"     # 复盘模板
    CHECKLIST = "checklist"                 # 检查清单
    LESSON = "lesson"                       # 心得教训
    NOTE = "note"                           # 学习笔记


# ==================== 知识项模型 ====================

class KnowledgeItem(BaseModel):
    """知识项基类"""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    title: str = Field(description="标题")
    content: str = Field(description="内容")
    
    # 分类
    category: str = Field(description="分类")
    tags: List[str] = Field(default_factory=list, description="标签")
    
    # 向量
    vector: List[float] = Field(default_factory=list)
    
    # 元数据
    importance: str = Field(default="medium", description="重要性: low/medium/high")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # 检索相关
    score: Optional[float] = Field(default=None, description="相似度分数")
    
    class Config:
        extra = "allow"


class FixedKnowledgeItem(KnowledgeItem):
    """固定知识项"""
    category: FixedKnowledgeCategory
    
    # 来源
    source_file: Optional[str] = Field(default=None, description="来源文件")
    source_url: Optional[str] = Field(default=None, description="参考链接")
    
    # 结构化内容
    summary: Optional[str] = Field(default=None, description="摘要")
    key_points: List[str] = Field(default_factory=list, description="要点")
    examples: List[str] = Field(default_factory=list, description="示例")
    
    # 关联
    related_ids: List[str] = Field(default_factory=list, description="相关知识ID")
    prerequisites: List[str] = Field(default_factory=list, description="前置知识ID")


class UserKnowledgeItem(KnowledgeItem):
    """用户知识项"""
    user_id: str = Field(description="用户ID")
    knowledge_type: UserKnowledgeType
    
    # 结构化规则
    conditions: List[str] = Field(default_factory=list, description="触发条件")
    actions: List[str] = Field(default_factory=list, description="执行动作")
    
    # 关联
    related_stocks: List[str] = Field(default_factory=list, description="相关股票")
    related_patterns: List[str] = Field(default_factory=list, description="关联交易模式")
    
    # 状态
    is_active: bool = Field(default=True, description="是否启用")
    use_count: int = Field(default=0, description="使用次数")
    last_used_at: Optional[datetime] = Field(default=None, description="最后使用时间")
    
    # 来源
    source: str = Field(default="manual", description="来源: manual/ai_assisted/imported")
    
    def touch(self) -> None:
        """更新使用统计"""
        self.use_count += 1
        self.last_used_at = datetime.utcnow()


# ==================== 操作结果 ====================

class KnowledgeSearchResult(BaseModel):
    """知识检索结果"""
    items: List[KnowledgeItem] = Field(default_factory=list)
    fixed_items: List[FixedKnowledgeItem] = Field(default_factory=list)
    user_items: List[UserKnowledgeItem] = Field(default_factory=list)
    total_count: int = 0
    query_time_ms: float = 0


class KnowledgeLoadResult(BaseModel):
    """知识加载结果"""
    success: bool
    loaded_count: int = 0
    failed_count: int = 0
    errors: List[str] = Field(default_factory=list)
