"""
处理流水线编排器

支持将多个处理节点以列表形式串联并异步执行。
"""

from typing import Any, List, Optional

from .base import BaseProcessor


class ProcessingPipeline(BaseProcessor):
    """
    处理流水线
    
    将多个处理器串联执行，前一个处理器的输出作为后一个的输入。
    
    Args:
        processors: 处理器列表，按执行顺序排列
        stop_on_empty: 当中间结果为空时是否停止
    
    Example:
        pipeline = ProcessingPipeline([
            TextCleaner(),
            TextSplitter(chunk_size=512),
            Vectorizer(),
        ])
        
        # 输入文本，输出向量列表
        vectors = await pipeline.process(documents, trace_id="xxx")
    """
    
    def __init__(
        self,
        processors: List[BaseProcessor],
        stop_on_empty: bool = True,
        name: Optional[str] = None,
    ):
        super().__init__(name or "Pipeline")
        self.processors = processors
        self.stop_on_empty = stop_on_empty
    
    async def process(
        self,
        data: Any,
        trace_id: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        执行流水线
        
        Args:
            data: 初始输入数据
            trace_id: 分布式追踪 ID
            **kwargs: 传递给所有处理器的额外参数
            
        Returns:
            最后一个处理器的输出
        """
        result = data
        
        for i, processor in enumerate(self.processors):
            self._log(
                "debug",
                f"Step {i+1}/{len(self.processors)}: {processor.name}",
                trace_id
            )
            
            result = await processor.process(result, trace_id=trace_id, **kwargs)
            
            # 检查是否为空
            if self.stop_on_empty and self._is_empty(result):
                self._log(
                    "warning",
                    f"Pipeline stopped at step {i+1}: empty result from {processor.name}",
                    trace_id
                )
                return result
        
        return result
    
    def _is_empty(self, data: Any) -> bool:
        """检查数据是否为空"""
        if data is None:
            return True
        if isinstance(data, (list, dict, str)):
            return len(data) == 0
        return False
    
    def add(self, processor: BaseProcessor) -> "ProcessingPipeline":
        """添加处理器到流水线末尾"""
        self.processors.append(processor)
        return self
    
    def insert(self, index: int, processor: BaseProcessor) -> "ProcessingPipeline":
        """在指定位置插入处理器"""
        self.processors.insert(index, processor)
        return self
