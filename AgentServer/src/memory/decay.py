"""
记忆遗忘机制

模拟人脑的遗忘曲线，清理不常用或过期的记忆。

遗忘策略:
1. TTL 过期: 设定过期时间，到期自动删除
2. 访问衰减: 长时间未访问的记忆逐渐降低重要性
3. 重要性衰减: 低重要性记忆随时间衰减
4. 容量淘汰: 达到容量上限时淘汰低优先级记忆
"""

import logging
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from .types import DecayStrategy, DecayResult, MemoryType


class DecayEngine:
    """
    记忆遗忘引擎
    
    定期清理和衰减记忆，保持记忆系统的健康状态。
    
    遗忘规则:
    1. 感觉记忆: 秒级 TTL，自动过期
    2. 工作记忆: 分钟级 TTL，容量限制淘汰
    3. 长期记忆: 访问频率 + 重要性衰减
    
    Args:
        access_decay_rate: 访问衰减率 (每天衰减的比例)
        importance_decay_rate: 重要性衰减率
        max_long_term_per_user: 每用户长期记忆上限
    
    Example:
        engine = DecayEngine()
        
        # 执行一次衰减
        result = await engine.run_decay(user_id)
        
        # 启动定时衰减
        await engine.start_scheduled_decay(interval_hours=24)
    """
    
    def __init__(
        self,
        access_decay_rate: float = 0.05,      # 每天衰减 5%
        importance_decay_rate: float = 0.02,   # 每天衰减 2%
        max_long_term_per_user: int = 10000,   # 每用户最多 1 万条
        min_importance_threshold: float = 0.1, # 低于此阈值删除
    ):
        self.access_decay_rate = access_decay_rate
        self.importance_decay_rate = importance_decay_rate
        self.max_long_term_per_user = max_long_term_per_user
        self.min_importance_threshold = min_importance_threshold
        
        self.logger = logging.getLogger("src.memory.DecayEngine")
        
        self._mongo_manager = None
        self._milvus_manager = None
        self._redis_manager = None
        self._is_running = False
    
    async def _get_mongo(self):
        if self._mongo_manager is None:
            from core.managers import mongo_manager
            self._mongo_manager = mongo_manager
        return self._mongo_manager
    
    async def _get_milvus(self):
        if self._milvus_manager is None:
            from core.managers import milvus_manager
            self._milvus_manager = milvus_manager
        return self._milvus_manager
    
    async def _get_redis(self):
        if self._redis_manager is None:
            from core.managers import redis_manager
            self._redis_manager = redis_manager
        return self._redis_manager
    
    async def run_decay(
        self,
        user_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> DecayResult:
        """
        执行记忆衰减
        
        Args:
            user_id: 用户ID，为空则处理所有用户
            trace_id: 追踪ID
            
        Returns:
            衰减结果
        """
        result = DecayResult()
        
        try:
            # 1. 清理过期的 TTL 记忆 (Redis 自动处理，这里只做日志)
            expired = await self._cleanup_expired_ttl(user_id, trace_id)
            result.expired_count = expired
            
            # 2. 衰减长期记忆的重要性
            decayed = await self._decay_importance(user_id, trace_id)
            result.decayed_count = decayed
            
            # 3. 清理低重要性记忆
            cleaned = await self._cleanup_low_importance(user_id, trace_id)
            result.expired_count += cleaned
            
            # 4. 执行容量淘汰
            evicted = await self._evict_by_capacity(user_id, trace_id)
            result.expired_count += evicted
            
            self.logger.info(
                f"[{trace_id}] Decay complete: expired={result.expired_count}, "
                f"decayed={result.decayed_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Decay failed: {e}")
            return result
    
    async def _cleanup_expired_ttl(
        self,
        user_id: Optional[str],
        trace_id: Optional[str],
    ) -> int:
        """清理过期的 TTL 记忆"""
        # Redis 的 TTL 是自动处理的
        # 这里只需要清理 MongoDB 中有 expires_at 字段的记录
        mongo = await self._get_mongo()
        
        try:
            now = datetime.utcnow()
            
            # 清理情景记忆中的过期项
            query = {
                "metadata.expires_at": {"$lt": now, "$ne": None}
            }
            
            if user_id:
                query["user_id"] = user_id
            
            result = await mongo.delete_many("episodic_memory", query)
            
            count = result.deleted_count if hasattr(result, "deleted_count") else 0
            
            if count > 0:
                self.logger.info(f"[{trace_id}] Cleaned {count} expired TTL memories")
            
            return count
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Cleanup TTL failed: {e}")
            return 0
    
    async def _decay_importance(
        self,
        user_id: Optional[str],
        trace_id: Optional[str],
    ) -> int:
        """衰减记忆的重要性分数"""
        mongo = await self._get_mongo()
        
        try:
            now = datetime.utcnow()
            decay_count = 0
            
            # 获取需要衰减的记忆 (24 小时未访问)
            cutoff_time = now - timedelta(hours=24)
            
            query = {
                "metadata.accessed_at": {"$lt": cutoff_time},
                "metadata.decay_strategy": {"$ne": DecayStrategy.NEVER.value},
            }
            
            if user_id:
                query["user_id"] = user_id
            
            # 批量更新
            for collection in ["episodic_memory", "trading_patterns"]:
                try:
                    cursor = mongo.find(collection, query, limit=1000)
                    
                    async for doc in cursor:
                        item_id = doc["_id"]
                        current_importance = doc.get("metadata", {}).get("importance_score", 0.5)
                        
                        # 计算衰减
                        hours_since_access = (now - doc["metadata"]["accessed_at"]).total_seconds() / 3600
                        days = hours_since_access / 24
                        
                        # 指数衰减
                        decay_factor = (1 - self.access_decay_rate) ** days
                        new_importance = current_importance * decay_factor
                        
                        # 更新
                        await mongo.update_one(
                            collection,
                            {"_id": item_id},
                            {"$set": {
                                "metadata.importance_score": new_importance,
                                "metadata.updated_at": now,
                            }}
                        )
                        
                        decay_count += 1
                        
                except Exception as e:
                    self.logger.warning(f"[{trace_id}] Decay in {collection} failed: {e}")
            
            return decay_count
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Decay importance failed: {e}")
            return 0
    
    async def _cleanup_low_importance(
        self,
        user_id: Optional[str],
        trace_id: Optional[str],
    ) -> int:
        """清理低重要性记忆"""
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        
        try:
            cleaned = 0
            
            # 查找低重要性记忆
            query = {
                "metadata.importance_score": {"$lt": self.min_importance_threshold},
                "metadata.decay_strategy": {"$ne": DecayStrategy.NEVER.value},
            }
            
            if user_id:
                query["user_id"] = user_id
            
            for collection in ["episodic_memory"]:
                try:
                    docs = await mongo.find(collection, query, limit=500)
                    
                    ids_to_delete = [doc["_id"] for doc in docs]
                    
                    if ids_to_delete:
                        # 从 MongoDB 删除
                        await mongo.delete_many(collection, {"_id": {"$in": ids_to_delete}})
                        
                        # 从 Milvus 删除
                        try:
                            await milvus.delete(collection, ids_to_delete)
                        except Exception:
                            pass  # Milvus 删除失败不影响流程
                        
                        cleaned += len(ids_to_delete)
                        
                except Exception as e:
                    self.logger.warning(f"[{trace_id}] Cleanup in {collection} failed: {e}")
            
            if cleaned > 0:
                self.logger.info(f"[{trace_id}] Cleaned {cleaned} low importance memories")
            
            return cleaned
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Cleanup low importance failed: {e}")
            return 0
    
    async def _evict_by_capacity(
        self,
        user_id: Optional[str],
        trace_id: Optional[str],
    ) -> int:
        """容量淘汰"""
        if not user_id:
            # 需要遍历所有用户，暂不实现
            return 0
        
        mongo = await self._get_mongo()
        milvus = await self._get_milvus()
        
        try:
            evicted = 0
            
            # 统计用户的长期记忆数量
            count = await mongo.count("episodic_memory", {"user_id": user_id})
            
            if count <= self.max_long_term_per_user:
                return 0
            
            # 需要淘汰的数量
            excess = count - self.max_long_term_per_user
            
            # 按重要性排序，淘汰最不重要的
            docs = await mongo.find(
                "episodic_memory",
                {"user_id": user_id},
                sort=[("metadata.importance_score", 1)],
                limit=excess,
            )
            
            ids_to_evict = [doc["_id"] for doc in docs]
            
            if ids_to_evict:
                await mongo.delete_many("episodic_memory", {"_id": {"$in": ids_to_evict}})
                
                try:
                    await milvus.delete("episodic_memory", ids_to_evict)
                except Exception:
                    pass
                
                evicted = len(ids_to_evict)
                self.logger.info(f"[{trace_id}] Evicted {evicted} memories due to capacity")
            
            return evicted
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Capacity eviction failed: {e}")
            return 0
    
    async def boost_importance(
        self,
        user_id: str,
        item_id: str,
        collection: str = "episodic_memory",
        boost_amount: float = 0.1,
        trace_id: Optional[str] = None,
    ) -> bool:
        """
        提升记忆的重要性 (被访问时调用)
        
        Args:
            user_id: 用户ID
            item_id: 记忆项ID
            collection: 集合名
            boost_amount: 提升量
            trace_id: 追踪ID
        """
        mongo = await self._get_mongo()
        
        try:
            now = datetime.utcnow()
            
            # 获取当前重要性
            doc = await mongo.find_one(collection, {"_id": item_id, "user_id": user_id})
            
            if not doc:
                return False
            
            current = doc.get("metadata", {}).get("importance_score", 0.5)
            new_importance = min(1.0, current + boost_amount)
            access_count = doc.get("metadata", {}).get("access_count", 0) + 1
            
            await mongo.update_one(
                collection,
                {"_id": item_id},
                {"$set": {
                    "metadata.importance_score": new_importance,
                    "metadata.access_count": access_count,
                    "metadata.accessed_at": now,
                    "metadata.updated_at": now,
                }}
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Boost importance failed: {e}")
            return False
    
    async def mark_as_permanent(
        self,
        user_id: str,
        item_id: str,
        collection: str = "episodic_memory",
        trace_id: Optional[str] = None,
    ) -> bool:
        """标记记忆为永久保留"""
        mongo = await self._get_mongo()
        
        try:
            await mongo.update_one(
                collection,
                {"_id": item_id, "user_id": user_id},
                {"$set": {
                    "metadata.decay_strategy": DecayStrategy.NEVER.value,
                    "metadata.updated_at": datetime.utcnow(),
                }}
            )
            
            self.logger.info(f"[{trace_id}] Marked {item_id} as permanent")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Mark permanent failed: {e}")
            return False
    
    async def start_scheduled_decay(
        self,
        interval_hours: float = 24,
        trace_id: Optional[str] = None,
    ) -> None:
        """启动定时衰减任务"""
        if self._is_running:
            self.logger.warning("Scheduled decay already running")
            return
        
        self._is_running = True
        self.logger.info(f"Starting scheduled decay with interval={interval_hours}h")
        
        while self._is_running:
            try:
                await self.run_decay(trace_id=trace_id)
            except Exception as e:
                self.logger.error(f"Scheduled decay error: {e}")
            
            await asyncio.sleep(interval_hours * 3600)
    
    def stop_scheduled_decay(self) -> None:
        """停止定时衰减任务"""
        self._is_running = False
        self.logger.info("Stopped scheduled decay")
