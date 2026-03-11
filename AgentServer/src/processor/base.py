"""
处理器基类

定义所有处理器的抽象接口。
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional
import logging


class BaseProcessor(ABC):
    """
    处理器抽象基类
    
    所有数据处理节点必须继承此类并实现 process() 方法。
    
    Attributes:
        name: 处理器名称，用于日志和追踪
        logger: 日志记录器
    """
    
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(f"src.processor.{self.name}")
    
    @abstractmethod
    async def process(
        self, 
        data: Any, 
        trace_id: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        处理数据
        
        Args:
            data: 输入数据，类型由具体处理器定义
            trace_id: 分布式追踪 ID
            **kwargs: 额外参数
            
        Returns:
            处理后的数据，类型由具体处理器定义
        """
        pass
    
    def _log(self, level: str, message: str, trace_id: Optional[str] = None) -> None:
        """
        带 trace_id 的日志记录
        
        Args:
            level: 日志级别 (debug, info, warning, error)
            message: 日志消息
            trace_id: 分布式追踪 ID
        """
        prefix = f"[{trace_id}] " if trace_id else ""
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(f"{prefix}{message}")
