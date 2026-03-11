"""
数据处理流水线 (Processor)

提供文本清洗、切割、向量化等数据预处理功能。
支持将多个处理节点串联成流水线异步执行。

Example:
    from src.processor import ProcessingPipeline, TextCleaner, TextSplitter, Vectorizer
    
    # 简单文本流水线
    pipeline = ProcessingPipeline([
        TextCleaner(),
        TextSplitter(chunk_size=512, overlap=50),
        Vectorizer(),
    ])
    results = await pipeline.process(documents, trace_id="xxx")
    
    # 文档处理流水线（带元数据）
    from src.processor import DocumentProcessor, DocumentVectorizer, Document
    
    doc_processor = DocumentProcessor(
        cleaner=TextCleaner(remove_html=True),
        splitter=TextSplitter(chunk_size=512),
    )
    documents = await doc_processor.process(news_list, trace_id="xxx")
"""

from .base import BaseProcessor
from .cleaner import TextCleaner
from .splitter import TextSplitter
from .vectorizer import Vectorizer
from .pipeline import ProcessingPipeline
from .document import Document, DocumentProcessor, DocumentVectorizer

__all__ = [
    # 基础类
    "BaseProcessor",
    # 文本处理器
    "TextCleaner",
    "TextSplitter",
    "Vectorizer",
    "ProcessingPipeline",
    # 文档处理器
    "Document",
    "DocumentProcessor",
    "DocumentVectorizer",
]
