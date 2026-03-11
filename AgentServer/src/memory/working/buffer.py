"""
工作记忆缓冲区

使用 Redis 实现分钟级 TTL 的有限容量工作记忆。
"""

import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from ..types import WorkingMemoryItem, MemoryMetadata


class WorkingBuffer:
    """
    工作记忆缓冲区
    
    模拟人类工作记忆的有限容量特性 (7±2 个项目)。
    使用 Redis 实现：
    - Hash: 存储记忆项详情
    - Sorted Set: 按重要性排序，维护容量限制
    
    Args:
        buffer_prefix: 键前缀
        capacity: 缓冲区容量 (默认 7)
        default_ttl_seconds: 默认 TTL (秒)
    
    Example:
        buffer = WorkingBuffer(capacity=9)
        
        # 添加工作记忆
        await buffer.add(user_id, item)
        
        # 获取当前所有工作记忆
        items = await buffer.get_all(user_id)
        
        # 获取与任务相关的记忆
        items = await buffer.get_by_task(user_id, task_id)
    """
    
    def __init__(
        self,
        buffer_prefix: str = "working",
        capacity: int = 7,
        default_ttl_seconds: int = 1800,  # 30 分钟
    ):
        self.buffer_prefix = buffer_prefix
        self.capacity = capacity
        self.default_ttl_seconds = default_ttl_seconds
        self.logger = logging.getLogger("src.memory.working.WorkingBuffer")
        self._redis_manager = None
    
    async def _get_redis(self):
        """延迟导入 redis_manager"""
        if self._redis_manager is None:
            from core.managers import redis_manager
            self._redis_manager = redis_manager
        return self._redis_manager
    
    def _build_hash_key(self, user_id: str) -> str:
        """构建 Hash 键名 (存储项详情)"""
        return f"{self.buffer_prefix}:items:{user_id}"
    
    def _build_zset_key(self, user_id: str) -> str:
        """构建 Sorted Set 键名 (按重要性排序)"""
        return f"{self.buffer_prefix}:rank:{user_id}"
    
    def _build_task_key(self, user_id: str, task_id: str) -> str:
        """构建任务关联 Set 键名"""
        return f"{self.buffer_prefix}:task:{user_id}:{task_id}"
    
    async def add(
        self,
        user_id: str,
        item: WorkingMemoryItem,
        trace_id: Optional[str] = None,
    ) -> bool:
        """
        添加工作记忆项
        
        如果超出容量，会移除重要性最低的项。
        
        Args:
            user_id: 用户ID
            item: 工作记忆项
            trace_id: 追踪ID
            
        Returns:
            是否成功
        """
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            # 序列化项
            item_data = item.model_dump_json()
            
            # 添加到 Hash
            await client.hset(hash_key, item.id, item_data)
            
            # 添加到 Sorted Set (分数 = 重要性)
            score = item.metadata.importance_score
            await client.zadd(zset_key, {item.id: score})
            
            # 如果有任务ID，添加到任务索引
            if item.task_id:
                task_key = self._build_task_key(user_id, item.task_id)
                await client.sadd(task_key, item.id)
                await client.expire(task_key, self.default_ttl_seconds)
            
            # 检查容量
            await self._enforce_capacity(user_id, trace_id)
            
            # 设置 TTL
            await client.expire(hash_key, self.default_ttl_seconds)
            await client.expire(zset_key, self.default_ttl_seconds)
            
            self.logger.debug(f"[{trace_id}] Added working memory: {item.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Add failed: {e}")
            return False
    
    async def add_batch(
        self,
        user_id: str,
        items: List[WorkingMemoryItem],
        trace_id: Optional[str] = None,
    ) -> int:
        """批量添加工作记忆项"""
        success_count = 0
        for item in items:
            if await self.add(user_id, item, trace_id):
                success_count += 1
        return success_count
    
    async def get(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[WorkingMemoryItem]:
        """获取单个工作记忆项"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        
        try:
            data = await client.hget(hash_key, item_id)
            if data:
                item = WorkingMemoryItem.model_validate_json(data)
                item.touch()  # 更新访问时间
                return item
            return None
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get failed: {e}")
            return None
    
    async def get_all(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> List[WorkingMemoryItem]:
        """获取所有工作记忆项 (按重要性排序)"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            # 获取排序后的 ID 列表
            item_ids = await client.zrevrange(zset_key, 0, -1)
            if not item_ids:
                return []
            
            # 批量获取详情
            items = []
            for item_id in item_ids:
                data = await client.hget(hash_key, item_id)
                if data:
                    item = WorkingMemoryItem.model_validate_json(data)
                    items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get all failed: {e}")
            return []
    
    async def get_by_task(
        self,
        user_id: str,
        task_id: str,
        trace_id: Optional[str] = None,
    ) -> List[WorkingMemoryItem]:
        """获取与特定任务相关的工作记忆"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        task_key = self._build_task_key(user_id, task_id)
        
        try:
            item_ids = await client.smembers(task_key)
            if not item_ids:
                return []
            
            items = []
            for item_id in item_ids:
                data = await client.hget(hash_key, item_id)
                if data:
                    item = WorkingMemoryItem.model_validate_json(data)
                    items.append(item)
            
            return sorted(items, key=lambda x: x.metadata.importance_score, reverse=True)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get by task failed: {e}")
            return []
    
    async def update_importance(
        self,
        user_id: str,
        item_id: str,
        new_importance: float,
        trace_id: Optional[str] = None,
    ) -> bool:
        """更新记忆项的重要性分数"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            # 获取并更新项
            data = await client.hget(hash_key, item_id)
            if not data:
                return False
            
            item = WorkingMemoryItem.model_validate_json(data)
            item.metadata.importance_score = new_importance
            item.metadata.updated_at = datetime.utcnow()
            
            # 保存更新
            await client.hset(hash_key, item_id, item.model_dump_json())
            await client.zadd(zset_key, {item_id: new_importance})
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Update importance failed: {e}")
            return False
    
    async def remove(
        self,
        user_id: str,
        item_id: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """移除工作记忆项"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            await client.hdel(hash_key, item_id)
            await client.zrem(zset_key, item_id)
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Remove failed: {e}")
            return False
    
    async def clear(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """清空用户的所有工作记忆"""
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            await client.delete(hash_key, zset_key)
            self.logger.info(f"[{trace_id}] Cleared working memory for user: {user_id}")
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Clear failed: {e}")
            return False
    
    async def count(self, user_id: str) -> int:
        """获取当前工作记忆项数量"""
        redis = await self._get_redis()
        client = redis._client
        
        zset_key = self._build_zset_key(user_id)
        try:
            return await client.zcard(zset_key)
        except Exception:
            return 0
    
    async def _enforce_capacity(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> int:
        """
        强制执行容量限制
        
        移除超出容量的最不重要项。
        
        Returns:
            移除的项数
        """
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            current_count = await client.zcard(zset_key)
            
            if current_count <= self.capacity:
                return 0
            
            # 获取需要移除的项 (重要性最低)
            excess_count = current_count - self.capacity
            items_to_remove = await client.zrange(zset_key, 0, excess_count - 1)
            
            for item_id in items_to_remove:
                await client.hdel(hash_key, item_id)
                await client.zrem(zset_key, item_id)
            
            self.logger.debug(
                f"[{trace_id}] Removed {len(items_to_remove)} items due to capacity limit"
            )
            return len(items_to_remove)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Enforce capacity failed: {e}")
            return 0
    
    async def get_for_consolidation(
        self,
        user_id: str,
        min_importance: float = 0.6,
        trace_id: Optional[str] = None,
    ) -> List[WorkingMemoryItem]:
        """
        获取需要巩固到长期记忆的项
        
        Args:
            user_id: 用户ID
            min_importance: 最小重要性阈值
            trace_id: 追踪ID
            
        Returns:
            需要巩固的工作记忆项
        """
        redis = await self._get_redis()
        client = redis._client
        
        hash_key = self._build_hash_key(user_id)
        zset_key = self._build_zset_key(user_id)
        
        try:
            # 获取高重要性项
            item_ids = await client.zrangebyscore(
                zset_key, min_importance, float("inf")
            )
            
            items = []
            for item_id in item_ids:
                data = await client.hget(hash_key, item_id)
                if data:
                    item = WorkingMemoryItem.model_validate_json(data)
                    # 检查是否为结论性内容
                    if item.is_conclusion or item.metadata.access_count >= 2:
                        items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get for consolidation failed: {e}")
            return []
