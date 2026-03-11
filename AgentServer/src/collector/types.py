"""
新闻采集类型定义
"""

import hashlib
from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class NewsCategory(str, Enum):
    """新闻分类"""
    # 财经
    FINANCE_FLASH = "finance_flash"         # 财经快讯
    FINANCE_ARTICLE = "finance_article"     # 财经文章
    FINANCE_REPORT = "finance_report"       # 研究报告
    
    # 政策
    POLICY_RELEASE = "policy_release"       # 政策发布
    POLICY_INTERPRET = "policy_interpret"   # 政策解读
    POLICY_NOTICE = "policy_notice"         # 通知公告
    POLICY_STANDARD = "policy_standard"     # 行业标准
    
    # 公司
    COMPANY_ANNOUNCE = "company_announce"   # 公司公告
    COMPANY_NEWS = "company_news"           # 公司新闻
    
    # 行业
    INDUSTRY_NEWS = "industry_news"         # 行业新闻
    INDUSTRY_ANALYSIS = "industry_analysis" # 行业分析
    
    # 其他
    GENERAL = "general"                     # 一般新闻


class NewsSource(str, Enum):
    """新闻来源"""
    # 财经媒体
    CLS = "cls"                     # 财联社
    EASTMONEY = "eastmoney"         # 东方财富
    XUEQIU = "xueqiu"               # 雪球
    WALLSTREETCN = "wallstreetcn"   # 华尔街见闻
    JIN10 = "jin10"                 # 金十数据
    GELONGHUI = "gelonghui"         # 格隆汇
    CAIXIN = "caixin"               # 财新
    
    # 政府机构
    MIIT = "miit"                   # 工信部
    CSRC = "csrc"                   # 证监会
    PBC = "pbc"                     # 央行
    MOF = "mof"                     # 财政部
    GOV = "gov"                     # 国务院
    
    # 交易所
    SSE = "sse"                     # 上交所
    SZSE = "szse"                   # 深交所
    
    # 科技/综合
    JUEJIN = "juejin"               # 稀土掘金
    THEPAPER = "thepaper"           # 澎湃新闻
    
    # 其他
    SINA = "sina"                   # 新浪财经
    TOUTIAO = "toutiao"             # 今日头条
    OTHER = "other"                 # 其他


class NewsItem(BaseModel):
    """
    新闻项
    
    统一的新闻数据结构，所有来源的新闻都转换为此格式。
    """
    # 唯一标识 (基于内容哈希生成)
    id: str = Field(default="")
    
    # 基本信息
    title: str = Field(description="标题")
    content: str = Field(default="", description="正文内容")
    summary: str = Field(default="", description="摘要")
    url: str = Field(default="", description="原文链接")
    
    # 分类
    source: NewsSource = Field(description="来源")
    category: NewsCategory = Field(default=NewsCategory.GENERAL, description="分类")
    
    # 时间
    publish_time: Optional[datetime] = Field(default=None, description="发布时间")
    collect_time: datetime = Field(default_factory=datetime.utcnow, description="采集时间")
    
    # 关联
    ts_codes: List[str] = Field(default_factory=list, description="关联股票代码")
    tags: List[str] = Field(default_factory=list, description="标签")
    keywords: List[str] = Field(default_factory=list, description="关键词")
    
    # 元数据
    author: str = Field(default="", description="作者")
    source_id: str = Field(default="", description="来源系统中的原始ID")
    extra: Dict[str, Any] = Field(default_factory=dict, description="额外信息")
    
    # 向量 (入库时生成)
    vector: List[float] = Field(default_factory=list)
    
    # 去重相关
    content_hash: str = Field(default="", description="内容哈希")
    title_hash: str = Field(default="", description="标题哈希")
    
    # 事件聚类相关 (深度去重后填充)
    event_id: Optional[str] = Field(default=None, description="关联事件ID")
    is_primary: bool = Field(default=False, description="是否为主新闻")
    clustered_at: Optional[datetime] = Field(default=None, description="聚类时间")
    
    def __init__(self, **data):
        super().__init__(**data)
        # 自动生成哈希
        if not self.content_hash:
            self.content_hash = self._compute_content_hash()
        if not self.title_hash:
            self.title_hash = self._compute_title_hash()
        # 自动生成 ID
        if not self.id:
            self.id = self._generate_id()
    
    def _compute_content_hash(self) -> str:
        """计算内容哈希"""
        text = f"{self.title}:{self.content[:500]}" if self.content else self.title
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _compute_title_hash(self) -> str:
        """计算标题哈希"""
        return hashlib.md5(self.title.encode('utf-8')).hexdigest()
    
    def _generate_id(self) -> str:
        """
        生成唯一 ID
        
        规则: source + 日期 + 内容哈希前8位
        这样同一篇文章无论哪个源抓到，ID 都相同
        """
        date_str = ""
        if self.publish_time:
            date_str = self.publish_time.strftime("%Y%m%d")
        else:
            date_str = datetime.utcnow().strftime("%Y%m%d")
        
        return f"{self.source.value}_{date_str}_{self.content_hash[:12]}"
    
    def get_text_for_embedding(self) -> str:
        """获取用于生成向量的文本"""
        parts = [self.title]
        if self.summary:
            parts.append(self.summary)
        if self.content:
            parts.append(self.content[:1000])
        if self.keywords:
            parts.append(" ".join(self.keywords))
        return "\n".join(parts)
    
    class Config:
        extra = "allow"


class CollectResult(BaseModel):
    """采集结果"""
    source: str = ""
    success: bool = True
    
    # 统计
    total_fetched: int = 0          # 抓取总数
    new_count: int = 0              # 新增数
    duplicate_count: int = 0        # 重复数 (完全重复)
    similar_count: int = 0          # 相似数 (跨源重复)
    failed_count: int = 0           # 失败数
    
    # 详情
    new_ids: List[str] = Field(default_factory=list)
    duplicate_ids: List[str] = Field(default_factory=list)
    similar_ids: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    
    # 耗时
    elapsed_ms: float = 0
    
    def merge(self, other: "CollectResult") -> "CollectResult":
        """合并两个结果"""
        return CollectResult(
            source=f"{self.source},{other.source}" if self.source else other.source,
            success=self.success and other.success,
            total_fetched=self.total_fetched + other.total_fetched,
            new_count=self.new_count + other.new_count,
            duplicate_count=self.duplicate_count + other.duplicate_count,
            similar_count=self.similar_count + other.similar_count,
            failed_count=self.failed_count + other.failed_count,
            new_ids=self.new_ids + other.new_ids,
            errors=self.errors + other.errors,
            elapsed_ms=max(self.elapsed_ms, other.elapsed_ms),
        )
