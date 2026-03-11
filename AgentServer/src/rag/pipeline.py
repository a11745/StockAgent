"""
RAG 流水线

实现标准的 RAG 流程：检索 -> 上下文组装。
"""

import logging
from typing import Any, Dict, List, Optional, Callable

from ..memory import MemoryItem
from .retriever import VectorRetriever


class RAGPipeline:
    """
    RAG 流水线
    
    实现检索增强生成的完整流程。
    
    Args:
        retriever: 向量检索器实例
        context_formatter: 自定义上下文格式化函数
        max_context_length: 最大上下文长度（字符数）
        include_metadata: 是否在上下文中包含元数据
    
    Example:
        pipeline = RAGPipeline(retriever=retriever)
        
        # 获取组装好的上下文
        context = await pipeline.process("用户问题", trace_id="xxx")
        
        # 或获取详细结果
        result = await pipeline.retrieve_with_context(
            query="用户问题",
            top_k=10,
            trace_id="xxx"
        )
    """
    
    DEFAULT_CONTEXT_TEMPLATE = """
相关信息 {index}:
来源: {source}
日期: {date}
内容: {content}
"""
    
    def __init__(
        self,
        retriever: VectorRetriever,
        context_formatter: Optional[Callable[[MemoryItem, int], str]] = None,
        max_context_length: int = 8000,
        include_metadata: bool = True,
    ):
        self.retriever = retriever
        self.context_formatter = context_formatter or self._default_formatter
        self.max_context_length = max_context_length
        self.include_metadata = include_metadata
        self.logger = logging.getLogger("src.rag.RAGPipeline")
    
    def _log(self, level: str, message: str, trace_id: Optional[str] = None) -> None:
        """带 trace_id 的日志记录"""
        prefix = f"[{trace_id}] " if trace_id else ""
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(f"{prefix}{message}")
    
    async def process(
        self,
        query: str,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """
        执行 RAG 流程，返回组装好的上下文
        
        Args:
            query: 用户查询
            top_k: 检索数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            组装好的上下文字符串
        """
        result = await self.retrieve_with_context(
            query=query,
            top_k=top_k,
            filters=filters,
            trace_id=trace_id,
        )
        return result["context"]
    
    async def retrieve_with_context(
        self,
        query: str,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        执行 RAG 流程，返回详细结果
        
        Args:
            query: 用户查询
            top_k: 检索数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            包含 items, context, metadata 的字典
        """
        self._log("debug", f"RAG process for query: {query[:50]}...", trace_id)
        
        # 1. 检索
        items = await self.retriever.retrieve(
            query=query,
            top_k=top_k,
            filters=filters,
            trace_id=trace_id,
        )
        
        # 2. 组装上下文
        context = self._assemble_context(items)
        
        # 3. 收集元数据
        metadata = self._collect_metadata(items)
        
        self._log(
            "info", 
            f"RAG completed: {len(items)} items, context length: {len(context)}", 
            trace_id
        )
        
        return {
            "items": items,
            "context": context,
            "metadata": metadata,
            "query": query,
            "item_count": len(items),
        }
    
    def _assemble_context(self, items: List[MemoryItem]) -> str:
        """组装上下文"""
        if not items:
            return ""
        
        context_parts = []
        current_length = 0
        
        for i, item in enumerate(items):
            formatted = self.context_formatter(item, i + 1)
            
            # 检查长度限制
            if current_length + len(formatted) > self.max_context_length:
                break
            
            context_parts.append(formatted)
            current_length += len(formatted)
        
        return "\n".join(context_parts)
    
    def _default_formatter(self, item: MemoryItem, index: int) -> str:
        """默认的上下文格式化"""
        if self.include_metadata:
            return self.DEFAULT_CONTEXT_TEMPLATE.format(
                index=index,
                source=item.metadata.source or "未知",
                date=item.metadata.publish_date or "未知",
                content=item.content,
            )
        else:
            return f"[{index}] {item.content}"
    
    def _collect_metadata(self, items: List[MemoryItem]) -> Dict[str, Any]:
        """收集检索结果的元数据"""
        sources = set()
        ts_codes = set()
        date_range = {"min": None, "max": None}
        
        for item in items:
            if item.metadata.source:
                sources.add(item.metadata.source)
            if item.metadata.ts_code:
                ts_codes.add(item.metadata.ts_code)
            if item.metadata.publish_date:
                date = item.metadata.publish_date
                if date_range["min"] is None or date < date_range["min"]:
                    date_range["min"] = date
                if date_range["max"] is None or date > date_range["max"]:
                    date_range["max"] = date
        
        return {
            "sources": list(sources),
            "ts_codes": list(ts_codes),
            "date_range": date_range,
            "avg_score": sum(i.score or 0 for i in items) / len(items) if items else 0,
        }
    
    async def retrieve_and_generate(
        self,
        query: str,
        system_prompt: Optional[str] = None,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        完整的 RAG 流程：检索 + LLM 生成
        
        Args:
            query: 用户查询
            system_prompt: 系统提示词
            top_k: 检索数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            包含 answer, items, context, metadata 的字典
        """
        # 获取上下文
        result = await self.retrieve_with_context(
            query=query,
            top_k=top_k,
            filters=filters,
            trace_id=trace_id,
        )
        
        # 构建提示词
        from core.managers import llm_manager
        
        default_system = """你是一个专业的金融分析助手。请基于以下检索到的信息回答用户问题。
如果信息不足以回答问题，请明确说明。

{context}
"""
        
        system = (system_prompt or default_system).format(context=result["context"])
        
        # 调用 LLM
        answer = await llm_manager.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": query},
            ]
        )
        
        result["answer"] = answer
        return result
