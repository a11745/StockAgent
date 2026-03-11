"""
记忆系统类型定义

定义三层记忆架构的核心类型。
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class MemoryType(str, Enum):
    """记忆类型"""
    SENSORY = "sensory"           # 感觉记忆
    WORKING = "working"           # 工作记忆
    LONG_TERM = "long_term"       # 长期记忆


class LongTermMemoryType(str, Enum):
    """长期记忆子类型"""
    PROCEDURAL = "procedural"     # 程序性记忆 (交易体系、策略模式)
    SEMANTIC = "semantic"         # 语义记忆 (知识图谱、文本语料)
    EPISODIC = "episodic"         # 情景记忆 (历史分析、用户交互)


class MemoryVisibility(str, Enum):
    """记忆可见性"""
    PRIVATE = "private"           # 仅用户可见
    SHARED = "shared"             # 可分享给其他用户
    PUBLIC = "public"             # 所有用户可见 (如公共新闻)


class DecayStrategy(str, Enum):
    """遗忘策略"""
    TTL = "ttl"                   # 时间过期
    ACCESS_DECAY = "access_decay" # 访问频率衰减
    IMPORTANCE_DECAY = "importance_decay"  # 重要性衰减
    NEVER = "never"               # 永不遗忘


# ==================== 基础元数据 ====================

class MemoryMetadata(BaseModel):
    """记忆元数据基类"""
    # 隔离字段
    user_id: str = Field(description="用户ID")
    session_id: Optional[str] = Field(default=None, description="会话ID")
    visibility: MemoryVisibility = Field(default=MemoryVisibility.PRIVATE)
    
    # 时间字段
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    accessed_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = Field(default=None, description="过期时间")
    
    # 遗忘机制
    decay_strategy: DecayStrategy = Field(default=DecayStrategy.ACCESS_DECAY)
    access_count: int = Field(default=0, description="访问次数")
    importance_score: float = Field(default=0.5, ge=0, le=1, description="重要性分数")
    
    # 业务字段
    ts_code: Optional[str] = Field(default=None, description="股票代码")
    ts_codes: List[str] = Field(default_factory=list, description="关联股票列表")
    source: Optional[str] = Field(default=None, description="数据来源")
    category: Optional[str] = Field(default=None, description="分类")
    tags: List[str] = Field(default_factory=list, description="标签")
    
    # 扩展字段
    extra: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        extra = "allow"


# ==================== 股票相关元数据 ====================

class StockMetadata(MemoryMetadata):
    """股票相关记忆元数据"""
    publish_date: Optional[str] = Field(default=None, description="发布日期 YYYYMMDD")
    trade_date: Optional[str] = Field(default=None, description="交易日期 YYYYMMDD")
    source_url: Optional[str] = Field(default=None, description="原文链接")
    sentiment: Optional[str] = Field(default=None, description="情感: positive/negative/neutral")


class HoldingMetadata(MemoryMetadata):
    """持仓相关记忆元数据"""
    position_type: str = Field(default="long", description="持仓方向: long/short")
    entry_price: Optional[float] = Field(default=None, description="入场价格")
    entry_date: Optional[str] = Field(default=None, description="入场日期")
    quantity: Optional[int] = Field(default=None, description="持仓数量")
    current_price: Optional[float] = Field(default=None, description="当前价格")
    profit_loss_pct: Optional[float] = Field(default=None, description="盈亏比例")


class TradingPatternMetadata(MemoryMetadata):
    """交易模式元数据 (程序性记忆)"""
    pattern_type: str = Field(description="模式类型: entry/exit/risk_management/position_sizing")
    success_rate: float = Field(default=0.0, ge=0, le=1, description="成功率")
    sample_count: int = Field(default=0, description="样本数量")
    avg_return: Optional[float] = Field(default=None, description="平均收益率")
    max_drawdown: Optional[float] = Field(default=None, description="最大回撤")
    description: str = Field(default="", description="模式描述")


# ==================== 记忆项 ====================

class BaseMemoryItem(BaseModel):
    """记忆项基类"""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    memory_type: MemoryType
    content: str = Field(description="文本内容")
    metadata: MemoryMetadata
    
    # 向量表示 (长期记忆使用)
    vector: List[float] = Field(default_factory=list)
    
    # 检索相关
    score: Optional[float] = Field(default=None, description="相似度/相关性分数")
    
    class Config:
        extra = "allow"
    
    def touch(self) -> None:
        """更新访问时间和计数"""
        self.metadata.accessed_at = datetime.utcnow()
        self.metadata.access_count += 1
    
    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.metadata.expires_at is None:
            return False
        return datetime.utcnow() > self.metadata.expires_at
    
    def calculate_decay_score(self) -> float:
        """计算衰减后的重要性分数"""
        if self.metadata.decay_strategy == DecayStrategy.NEVER:
            return self.metadata.importance_score
        
        # 时间衰减因子
        hours_since_access = (datetime.utcnow() - self.metadata.accessed_at).total_seconds() / 3600
        time_decay = 0.95 ** (hours_since_access / 24)  # 每天衰减 5%
        
        # 访问频率因子
        access_factor = min(1.0, self.metadata.access_count / 10)
        
        # 综合分数
        return self.metadata.importance_score * time_decay * (0.5 + 0.5 * access_factor)


class SensoryMemoryItem(BaseMemoryItem):
    """感觉记忆项"""
    memory_type: MemoryType = MemoryType.SENSORY
    stream_id: Optional[str] = Field(default=None, description="Redis Stream ID")
    raw_data: Optional[Dict[str, Any]] = Field(default=None, description="原始数据")


class WorkingMemoryItem(BaseMemoryItem):
    """工作记忆项"""
    memory_type: MemoryType = MemoryType.WORKING
    task_id: Optional[str] = Field(default=None, description="关联任务ID")
    reasoning_step: Optional[int] = Field(default=None, description="推理步骤")
    is_conclusion: bool = Field(default=False, description="是否为结论")


class LongTermMemoryItem(BaseMemoryItem):
    """长期记忆项"""
    memory_type: MemoryType = MemoryType.LONG_TERM
    subtype: LongTermMemoryType = Field(description="长期记忆子类型")
    
    # Neo4j 图相关
    node_id: Optional[str] = Field(default=None, description="Neo4j 节点ID")
    relationships: List[Dict[str, Any]] = Field(default_factory=list, description="关联关系")


# ==================== 操作结果 ====================

class InsertResult(BaseModel):
    """插入结果"""
    success: bool
    inserted_ids: List[str] = Field(default_factory=list)
    failed_count: int = 0
    message: str = ""


class SearchResult(BaseModel):
    """检索结果"""
    items: List[BaseMemoryItem] = Field(default_factory=list)
    total_count: int = 0
    query_time_ms: float = 0
    

class ConsolidationResult(BaseModel):
    """巩固结果 (工作记忆 → 长期记忆)"""
    consolidated_count: int = 0
    to_semantic: int = 0
    to_episodic: int = 0
    to_procedural: int = 0
    patterns_detected: List[str] = Field(default_factory=list)


class DecayResult(BaseModel):
    """遗忘结果"""
    expired_count: int = 0
    decayed_count: int = 0
    preserved_count: int = 0
