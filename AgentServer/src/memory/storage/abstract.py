"""
存储抽象接口

定义记忆存储后端必须实现的接口。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..types import BaseMemoryItem as MemoryItem


class AbstractStore(ABC):
    """
    存储后端抽象基类
    
    所有存储实现（Milvus, FAISS, etc.）必须继承此类。
    """
    
    @abstractmethod
    async def upsert(
        self,
        items: List[MemoryItem],
        trace_id: Optional[str] = None,
    ) -> int:
        """
        插入或更新记忆项
        
        Args:
            items: 记忆项列表
            trace_id: 分布式追踪 ID
            
        Returns:
            成功插入/更新的数量
        """
        pass
    
    @abstractmethod
    async def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """
        向量相似度搜索
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            filters: 过滤条件
            trace_id: 分布式追踪 ID
            
        Returns:
            相似度最高的记忆项列表
        """
        pass
    
    @abstractmethod
    async def delete(
        self,
        ids: List[str],
        trace_id: Optional[str] = None,
    ) -> int:
        """
        删除记忆项
        
        Args:
            ids: 要删除的记忆项 ID 列表
            trace_id: 分布式追踪 ID
            
        Returns:
            成功删除的数量
        """
        pass
    
    @abstractmethod
    async def get(
        self,
        ids: List[str],
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """
        根据 ID 获取记忆项
        
        Args:
            ids: 记忆项 ID 列表
            trace_id: 分布式追踪 ID
            
        Returns:
            记忆项列表
        """
        pass
    
    async def count(self, trace_id: Optional[str] = None) -> int:
        """获取记忆项总数"""
        return 0
    
    async def clear(self, trace_id: Optional[str] = None) -> bool:
        """清空所有记忆项"""
        return False
