"""
记忆项数据模型

定义记忆系统的核心数据结构。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class MemoryMetadata(BaseModel):
    """
    记忆元数据
    
    新闻入库必须携带的关键元数据字段。
    """
    # 股票相关
    ts_code: Optional[str] = Field(default=None, description="股票代码")
    ts_codes: List[str] = Field(default_factory=list, description="关联的股票代码列表")
    
    # 时间相关
    publish_date: Optional[str] = Field(default=None, description="发布日期 (YYYYMMDD)")
    publish_time: Optional[datetime] = Field(default=None, description="发布时间")
    
    # 来源相关
    source: Optional[str] = Field(default=None, description="数据来源 (sina, eastmoney, etc.)")
    source_url: Optional[str] = Field(default=None, description="原文链接")
    
    # 分类相关
    category: Optional[str] = Field(default=None, description="分类 (news, announcement, etc.)")
    tags: List[str] = Field(default_factory=list, description="标签列表")
    
    # 额外字段
    extra: Dict[str, Any] = Field(default_factory=dict, description="额外元数据")


class MemoryItem(BaseModel):
    """
    记忆项
    
    记忆系统的基本单元，包含内容、向量和元数据。
    
    Attributes:
        id: 唯一标识符
        content: 文本内容
        vector: 向量表示
        metadata: 结构化元数据
        score: 相似度分数（仅检索结果中有值）
        created_at: 创建时间
    """
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    content: str = Field(description="文本内容")
    vector: List[float] = Field(default_factory=list, description="向量表示")
    metadata: MemoryMetadata = Field(default_factory=MemoryMetadata, description="元数据")
    score: Optional[float] = Field(default=None, description="相似度分数")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="创建时间")
    
    class Config:
        extra = "allow"
    
    def to_milvus_dict(self) -> Dict[str, Any]:
        """转换为 Milvus 插入格式"""
        return {
            "id": self.id,
            "content": self.content,
            "vector": self.vector,
            "ts_code": self.metadata.ts_code or "",
            "publish_date": self.metadata.publish_date or "",
            "source": self.metadata.source or "",
            "category": self.metadata.category or "",
            "created_at": self.created_at.isoformat(),
            "metadata_json": self.metadata.model_dump_json(),
        }
    
    @classmethod
    def from_milvus_hit(cls, hit: Dict[str, Any], score: float) -> "MemoryItem":
        """从 Milvus 检索结果构造"""
        import json
        
        metadata_json = hit.get("metadata_json", "{}")
        try:
            metadata_dict = json.loads(metadata_json)
            metadata = MemoryMetadata(**metadata_dict)
        except Exception:
            metadata = MemoryMetadata(
                ts_code=hit.get("ts_code"),
                publish_date=hit.get("publish_date"),
                source=hit.get("source"),
                category=hit.get("category"),
            )
        
        return cls(
            id=hit.get("id", ""),
            content=hit.get("content", ""),
            vector=hit.get("vector", []),
            metadata=metadata,
            score=score,
            created_at=datetime.fromisoformat(hit.get("created_at", datetime.utcnow().isoformat())),
        )
