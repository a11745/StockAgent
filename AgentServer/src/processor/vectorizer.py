"""
向量化处理器

封装 Embedding 生成逻辑，对接 core.managers.llm_manager。
"""

from typing import Any, List, Optional, Union

from .base import BaseProcessor


class Vectorizer(BaseProcessor):
    """
    向量化处理器
    
    将文本转换为向量表示，使用 llm_manager 的 embedding 功能。
    
    Args:
        batch_size: 批量处理大小
    
    Example:
        vectorizer = Vectorizer(batch_size=32)
        vectors = await vectorizer.process(["text1", "text2"], trace_id="xxx")
        # Returns: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
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
        """延迟导入 llm_manager，避免循环依赖"""
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def process(
        self,
        data: Union[str, List[str]],
        trace_id: Optional[str] = None,
        **kwargs
    ) -> List[List[float]]:
        """
        生成文本向量
        
        Args:
            data: 单个文本字符串或文本列表
            trace_id: 分布式追踪 ID
            
        Returns:
            向量列表，每个向量对应一个输入文本
        """
        texts = [data] if isinstance(data, str) else data
        
        if not texts:
            return []
        
        self._log("debug", f"Vectorizing {len(texts)} texts", trace_id)
        
        llm_manager = await self._get_llm_manager()
        
        # 批量处理
        all_vectors = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_vectors = await self._embed_batch(llm_manager, batch, trace_id)
            all_vectors.extend(batch_vectors)
        
        self._log(
            "debug", 
            f"Generated {len(all_vectors)} vectors (dim={len(all_vectors[0]) if all_vectors else 0})", 
            trace_id
        )
        
        return all_vectors
    
    async def _embed_batch(
        self, 
        llm_manager, 
        texts: List[str], 
        trace_id: Optional[str]
    ) -> List[List[float]]:
        """批量生成向量"""
        try:
            vectors = await llm_manager.embedding(texts)
            return vectors
        except Exception as e:
            self._log("error", f"Embedding failed: {e}", trace_id)
            raise
