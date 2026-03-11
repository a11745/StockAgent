"""
LLM 缓存层

支持:
- 内存缓存 (TTL)
- Redis 缓存 (分布式)
- Embedding 缓存
"""

import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    value: Any
    created_at: float
    ttl: int  # seconds
    hit_count: int = 0
    
    @property
    def is_expired(self) -> bool:
        return time.time() > self.created_at + self.ttl
    
    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at


class CacheBackend(ABC):
    """缓存后端接口"""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """设置缓存"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """删除缓存"""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """清空缓存"""
        pass
    
    @abstractmethod
    async def stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        pass


class MemoryCache(CacheBackend):
    """内存缓存"""
    
    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._hits = 0
        self._misses = 0
    
    async def get(self, key: str) -> Optional[Any]:
        entry = self._cache.get(key)
        
        if entry is None:
            self._misses += 1
            return None
        
        if entry.is_expired:
            del self._cache[key]
            self._misses += 1
            return None
        
        entry.hit_count += 1
        self._hits += 1
        return entry.value
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        # 清理过期条目
        if len(self._cache) >= self._max_size:
            self._evict()
        
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            ttl=ttl,
        )
    
    async def delete(self, key: str) -> None:
        self._cache.pop(key, None)
    
    async def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
    
    async def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "type": "memory",
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
        }
    
    def _evict(self) -> None:
        """驱逐过期和最少使用的条目"""
        now = time.time()
        
        # 先删除过期的
        expired = [k for k, v in self._cache.items() if v.is_expired]
        for key in expired:
            del self._cache[key]
        
        # 如果还是满了，删除最少使用的
        if len(self._cache) >= self._max_size:
            # 按 hit_count 排序，删除最少使用的 10%
            items = sorted(self._cache.items(), key=lambda x: x[1].hit_count)
            to_remove = max(1, len(items) // 10)
            for key, _ in items[:to_remove]:
                del self._cache[key]


class RedisCache(CacheBackend):
    """Redis 缓存"""
    
    def __init__(self, prefix: str = "llm_cache:"):
        self._prefix = prefix
        self._redis = None
        self._hits = 0
        self._misses = 0
    
    async def _get_redis(self):
        if self._redis is None:
            from core.managers import redis_manager
            self._redis = redis_manager
        return self._redis
    
    def _make_key(self, key: str) -> str:
        return f"{self._prefix}{key}"
    
    async def get(self, key: str) -> Optional[Any]:
        redis = await self._get_redis()
        
        try:
            value = await redis.get(self._make_key(key))
            if value is None:
                self._misses += 1
                return None
            
            self._hits += 1
            return json.loads(value)
        except Exception as e:
            logger.error(f"Redis cache get error: {e}")
            self._misses += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        redis = await self._get_redis()
        
        try:
            await redis.setex(
                self._make_key(key),
                ttl,
                json.dumps(value, ensure_ascii=False),
            )
        except Exception as e:
            logger.error(f"Redis cache set error: {e}")
    
    async def delete(self, key: str) -> None:
        redis = await self._get_redis()
        
        try:
            await redis.delete(self._make_key(key))
        except Exception as e:
            logger.error(f"Redis cache delete error: {e}")
    
    async def clear(self) -> None:
        redis = await self._get_redis()
        
        try:
            # 使用 SCAN 删除所有匹配的 key
            cursor = 0
            while True:
                cursor, keys = await redis.scan(cursor, match=f"{self._prefix}*")
                if keys:
                    await redis.delete(*keys)
                if cursor == 0:
                    break
            
            self._hits = 0
            self._misses = 0
        except Exception as e:
            logger.error(f"Redis cache clear error: {e}")
    
    async def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "type": "redis",
            "prefix": self._prefix,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
        }


class LLMCache:
    """
    LLM 缓存管理器
    
    支持:
    - Chat 响应缓存
    - Embedding 缓存
    - 多级缓存 (内存 + Redis)
    
    Example:
        cache = LLMCache()
        
        # 检查缓存
        cached = await cache.get_chat(messages, model)
        if cached:
            return cached
        
        # 调用 LLM
        response = await llm.chat(messages)
        
        # 存入缓存
        await cache.set_chat(messages, model, response)
    """
    
    def __init__(
        self,
        use_redis: bool = False,
        chat_ttl: int = 3600,      # 1 小时
        embedding_ttl: int = 86400, # 24 小时
    ):
        self._chat_cache: CacheBackend = RedisCache("llm:chat:") if use_redis else MemoryCache()
        self._embedding_cache: CacheBackend = RedisCache("llm:emb:") if use_redis else MemoryCache()
        
        self._chat_ttl = chat_ttl
        self._embedding_ttl = embedding_ttl
        
        self._enabled = True
    
    def enable(self) -> None:
        """启用缓存"""
        self._enabled = True
    
    def disable(self) -> None:
        """禁用缓存"""
        self._enabled = False
    
    # ==================== Chat 缓存 ====================
    
    def _chat_cache_key(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
    ) -> str:
        """生成 Chat 缓存 key"""
        content = json.dumps({
            "messages": messages,
            "model": model,
            "temperature": temperature,
        }, sort_keys=True, ensure_ascii=False)
        
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    async def get_chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
    ) -> Optional[str]:
        """获取 Chat 缓存"""
        if not self._enabled:
            return None
        
        key = self._chat_cache_key(messages, model, temperature)
        result = await self._chat_cache.get(key)
        
        if result:
            logger.debug(f"Chat cache hit: {key[:8]}...")
        
        return result
    
    async def set_chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        response: str,
        temperature: float = 0.7,
        ttl: Optional[int] = None,
    ) -> None:
        """设置 Chat 缓存"""
        if not self._enabled:
            return
        
        # 温度为 0 时缓存更长时间（确定性输出）
        if temperature == 0:
            ttl = ttl or self._chat_ttl * 24
        else:
            ttl = ttl or self._chat_ttl
        
        key = self._chat_cache_key(messages, model, temperature)
        await self._chat_cache.set(key, response, ttl)
        
        logger.debug(f"Chat cached: {key[:8]}... (ttl={ttl}s)")
    
    # ==================== Embedding 缓存 ====================
    
    def _embedding_cache_key(self, text: str, model: str) -> str:
        """生成 Embedding 缓存 key"""
        content = f"{model}:{text}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    async def get_embedding(
        self,
        text: str,
        model: str,
    ) -> Optional[List[float]]:
        """获取 Embedding 缓存"""
        if not self._enabled:
            return None
        
        key = self._embedding_cache_key(text, model)
        return await self._embedding_cache.get(key)
    
    async def get_embeddings_batch(
        self,
        texts: List[str],
        model: str,
    ) -> Tuple[List[Optional[List[float]]], List[int]]:
        """
        批量获取 Embedding 缓存
        
        Returns:
            (cached_embeddings, missing_indices)
        """
        if not self._enabled:
            return [None] * len(texts), list(range(len(texts)))
        
        results = []
        missing = []
        
        for i, text in enumerate(texts):
            cached = await self.get_embedding(text, model)
            results.append(cached)
            if cached is None:
                missing.append(i)
        
        return results, missing
    
    async def set_embedding(
        self,
        text: str,
        model: str,
        embedding: List[float],
        ttl: Optional[int] = None,
    ) -> None:
        """设置 Embedding 缓存"""
        if not self._enabled:
            return
        
        key = self._embedding_cache_key(text, model)
        await self._embedding_cache.set(key, embedding, ttl or self._embedding_ttl)
    
    async def set_embeddings_batch(
        self,
        texts: List[str],
        model: str,
        embeddings: List[List[float]],
        ttl: Optional[int] = None,
    ) -> None:
        """批量设置 Embedding 缓存"""
        for text, embedding in zip(texts, embeddings):
            await self.set_embedding(text, model, embedding, ttl)
    
    # ==================== 管理 ====================
    
    async def clear(self) -> None:
        """清空所有缓存"""
        await self._chat_cache.clear()
        await self._embedding_cache.clear()
        logger.info("LLM cache cleared")
    
    async def stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        return {
            "enabled": self._enabled,
            "chat": await self._chat_cache.stats(),
            "embedding": await self._embedding_cache.stats(),
        }


# 全局单例
llm_cache = LLMCache()
