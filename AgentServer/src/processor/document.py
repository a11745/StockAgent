"""
文档处理器

处理带元数据的文档，支持从新闻/公告等结构化数据中提取内容并处理。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from .base import BaseProcessor
from .cleaner import TextCleaner
from .splitter import TextSplitter


@dataclass
class Document:
    """
    文档数据结构
    
    用于在处理流程中传递带元数据的文本。
    
    Attributes:
        content: 文本内容
        metadata: 元数据字典
        chunks: 切割后的文本块（由 splitter 填充）
        vectors: 向量化结果（由 vectorizer 填充）
    """
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    chunks: List[str] = field(default_factory=list)
    vectors: List[List[float]] = field(default_factory=list)
    
    @classmethod
    def from_news(cls, news: Dict[str, Any]) -> "Document":
        """从新闻数据构造文档"""
        content = news.get("content") or news.get("title", "")
        
        metadata = {
            "ts_code": news.get("ts_code", ""),
            "ts_codes": news.get("ts_codes", []),
            "publish_date": cls._normalize_date(news.get("datetime") or news.get("publish_date")),
            "source": news.get("source", ""),
            "source_url": news.get("url", ""),
            "title": news.get("title", ""),
            "category": "news",
        }
        
        return cls(content=content, metadata=metadata)
    
    @classmethod
    def from_announcement(cls, ann: Dict[str, Any]) -> "Document":
        """从公告数据构造文档"""
        content = ann.get("content") or ann.get("title", "")
        
        metadata = {
            "ts_code": ann.get("ts_code", ""),
            "publish_date": cls._normalize_date(ann.get("ann_date")),
            "source": "announcement",
            "title": ann.get("title", ""),
            "category": "announcement",
        }
        
        return cls(content=content, metadata=metadata)
    
    @staticmethod
    def _normalize_date(date_value: Any) -> str:
        """标准化日期为 YYYYMMDD 格式"""
        if not date_value:
            return ""
        
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y%m%d")
        
        date_str = str(date_value)
        
        # 移除常见的分隔符
        for sep in ["-", "/", " ", ":", "."]:
            date_str = date_str.replace(sep, "")
        
        # 取前8位 (YYYYMMDD)
        return date_str[:8] if len(date_str) >= 8 else date_str
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "content": self.content,
            "metadata": self.metadata,
            "chunks": self.chunks,
            "vectors": self.vectors,
        }


class DocumentProcessor(BaseProcessor):
    """
    文档处理器
    
    处理带元数据的文档，支持清洗、切割等操作。
    自动保留并传递元数据。
    
    Args:
        cleaner: 文本清洗器（可选）
        splitter: 文本切割器（可选）
        content_field: 内容字段名（处理字典时使用）
        metadata_fields: 要保留的元数据字段
    
    Example:
        processor = DocumentProcessor(
            cleaner=TextCleaner(remove_html=True),
            splitter=TextSplitter(chunk_size=512),
        )
        
        # 处理新闻列表
        documents = await processor.process(news_list, trace_id="xxx")
    """
    
    DEFAULT_METADATA_FIELDS = [
        "ts_code", "ts_codes", "publish_date", "source", 
        "source_url", "title", "category", "datetime", "url"
    ]
    
    def __init__(
        self,
        cleaner: Optional[TextCleaner] = None,
        splitter: Optional[TextSplitter] = None,
        content_field: str = "content",
        metadata_fields: Optional[List[str]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.cleaner = cleaner
        self.splitter = splitter
        self.content_field = content_field
        self.metadata_fields = metadata_fields or self.DEFAULT_METADATA_FIELDS
    
    async def process(
        self,
        data: Union[Dict, List[Dict], Document, List[Document]],
        trace_id: Optional[str] = None,
        **kwargs
    ) -> List[Document]:
        """
        处理文档
        
        Args:
            data: 单个文档/字典或文档/字典列表
            trace_id: 分布式追踪 ID
            
        Returns:
            处理后的文档列表
        """
        # 统一转换为 Document 列表
        documents = self._to_documents(data)
        
        if not documents:
            return []
        
        self._log("debug", f"Processing {len(documents)} documents", trace_id)
        
        results = []
        for doc in documents:
            processed = await self._process_document(doc, trace_id)
            if processed:
                results.extend(processed)
        
        self._log("info", f"Processed {len(documents)} docs into {len(results)} chunks", trace_id)
        
        return results
    
    def _to_documents(
        self, 
        data: Union[Dict, List[Dict], Document, List[Document]]
    ) -> List[Document]:
        """转换为 Document 列表"""
        if isinstance(data, Document):
            return [data]
        
        if isinstance(data, dict):
            return [self._dict_to_document(data)]
        
        if isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, Document):
                    result.append(item)
                elif isinstance(item, dict):
                    result.append(self._dict_to_document(item))
            return result
        
        return []
    
    def _dict_to_document(self, data: Dict[str, Any]) -> Document:
        """将字典转换为 Document"""
        # 提取内容
        content = data.get(self.content_field, "")
        if not content and "title" in data:
            content = data.get("title", "")
        
        # 提取元数据
        metadata = {}
        for field_name in self.metadata_fields:
            if field_name in data:
                metadata[field_name] = data[field_name]
        
        # 标准化 publish_date
        if "datetime" in metadata and "publish_date" not in metadata:
            metadata["publish_date"] = Document._normalize_date(metadata.get("datetime"))
        
        return Document(content=content, metadata=metadata)
    
    async def _process_document(
        self, 
        doc: Document, 
        trace_id: Optional[str]
    ) -> List[Document]:
        """处理单个文档"""
        content = doc.content
        
        if not content or not content.strip():
            return []
        
        # 1. 清洗
        if self.cleaner:
            content = await self.cleaner.process(content, trace_id=trace_id)
            if isinstance(content, list):
                content = content[0] if content else ""
        
        if not content:
            return []
        
        # 2. 切割
        if self.splitter:
            chunks = await self.splitter.process(content, trace_id=trace_id)
        else:
            chunks = [content]
        
        # 3. 为每个 chunk 创建新文档
        results = []
        for i, chunk in enumerate(chunks):
            chunk_doc = Document(
                content=chunk,
                metadata={
                    **doc.metadata,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                },
            )
            results.append(chunk_doc)
        
        return results


class DocumentVectorizer(BaseProcessor):
    """
    文档向量化器
    
    为 Document 对象生成向量。
    
    Args:
        batch_size: 批量处理大小
    
    Example:
        vectorizer = DocumentVectorizer(batch_size=32)
        documents = await vectorizer.process(documents, trace_id="xxx")
        # documents 的 vectors 字段会被填充
    """
    
    def __init__(
        self,
        batch_size: int = 32,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.batch_size = batch_size
        self._llm_manager = None
    
    async def _get_llm_manager(self):
        """延迟导入 llm_manager"""
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def process(
        self,
        data: Union[Document, List[Document]],
        trace_id: Optional[str] = None,
        **kwargs
    ) -> List[Document]:
        """
        为文档生成向量
        
        Args:
            data: 单个文档或文档列表
            trace_id: 分布式追踪 ID
            
        Returns:
            填充了 vectors 字段的文档列表
        """
        documents = [data] if isinstance(data, Document) else data
        
        if not documents:
            return []
        
        self._log("debug", f"Vectorizing {len(documents)} documents", trace_id)
        
        llm_manager = await self._get_llm_manager()
        
        # 提取所有内容
        contents = [doc.content for doc in documents]
        
        # 批量向量化
        all_vectors = []
        for i in range(0, len(contents), self.batch_size):
            batch = contents[i:i + self.batch_size]
            try:
                vectors = await llm_manager.embedding(batch)
                all_vectors.extend(vectors)
            except Exception as e:
                self._log("error", f"Embedding failed: {e}", trace_id)
                # 为失败的批次填充空向量
                all_vectors.extend([[] for _ in batch])
        
        # 填充向量
        for doc, vector in zip(documents, all_vectors):
            doc.vectors = [vector] if vector else []
        
        self._log(
            "info", 
            f"Vectorized {len(documents)} documents (dim={len(all_vectors[0]) if all_vectors and all_vectors[0] else 0})", 
            trace_id
        )
        
        return documents
