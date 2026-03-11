"""
感觉记忆流

使用 Redis Stream 实现秒级 TTL 的高吞吐量数据流。
"""

import json
import logging
from typing import Any, Dict, List, Optional, AsyncIterator
from datetime import datetime

from ..types import SensoryMemoryItem, MemoryMetadata


class SensoryStream:
    """
    感觉记忆流
    
    使用 Redis Stream 存储实时数据流，自动过期。
    
    Args:
        stream_prefix: Stream 键前缀
        default_ttl_seconds: 默认 TTL (秒)
        max_length: Stream 最大长度
    
    Example:
        stream = SensoryStream(stream_prefix="sensory")
        
        # 写入实时行情
        await stream.push(user_id, "quote", {"ts_code": "000001.SZ", "price": 10.5})
        
        # 读取最新数据
        items = await stream.read(user_id, "quote", count=10)
    """
    
    def __init__(
        self,
        stream_prefix: str = "sensory",
        default_ttl_seconds: int = 30,
        max_length: int = 1000,
    ):
        self.stream_prefix = stream_prefix
        self.default_ttl_seconds = default_ttl_seconds
        self.max_length = max_length
        self.logger = logging.getLogger("src.memory.sensory.SensoryStream")
        self._redis_manager = None
    
    async def _get_redis(self):
        """延迟导入 redis_manager"""
        if self._redis_manager is None:
            from core.managers import redis_manager
            self._redis_manager = redis_manager
        return self._redis_manager
    
    def _build_stream_key(self, user_id: str, stream_type: str) -> str:
        """构建 Stream 键名"""
        return f"{self.stream_prefix}:{user_id}:{stream_type}"
    
    async def push(
        self,
        user_id: str,
        stream_type: str,
        data: Dict[str, Any],
        ttl_seconds: Optional[int] = None,
        trace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        推送数据到感觉记忆流
        
        Args:
            user_id: 用户ID
            stream_type: 流类型 (quote/news/input)
            data: 数据内容
            ttl_seconds: TTL 秒数
            trace_id: 追踪ID
            
        Returns:
            Stream entry ID
        """
        redis = await self._get_redis()
        stream_key = self._build_stream_key(user_id, stream_type)
        
        try:
            # 序列化数据
            entry = {
                "data": json.dumps(data, ensure_ascii=False, default=str),
                "timestamp": datetime.utcnow().isoformat(),
                "trace_id": trace_id or "",
            }
            
            # 添加到 Stream (使用 XADD)
            client = redis._client
            stream_id = await client.xadd(
                stream_key,
                entry,
                maxlen=self.max_length,
            )
            
            # 设置 TTL
            ttl = ttl_seconds or self.default_ttl_seconds
            await client.expire(stream_key, ttl)
            
            self.logger.debug(f"[{trace_id}] Pushed to {stream_key}: {stream_id}")
            return stream_id
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Push failed: {e}")
            return None
    
    async def read(
        self,
        user_id: str,
        stream_type: str,
        count: int = 10,
        last_id: str = "0",
        trace_id: Optional[str] = None,
    ) -> List[SensoryMemoryItem]:
        """
        读取感觉记忆流
        
        Args:
            user_id: 用户ID
            stream_type: 流类型
            count: 读取数量
            last_id: 从此 ID 之后读取
            trace_id: 追踪ID
            
        Returns:
            感觉记忆项列表
        """
        redis = await self._get_redis()
        stream_key = self._build_stream_key(user_id, stream_type)
        
        try:
            client = redis._client
            entries = await client.xrange(stream_key, min=last_id, count=count)
            
            items = []
            for entry_id, fields in entries:
                item = self._entry_to_item(entry_id, fields, user_id, stream_type)
                if item:
                    items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Read failed: {e}")
            return []
    
    async def read_latest(
        self,
        user_id: str,
        stream_type: str,
        count: int = 10,
        trace_id: Optional[str] = None,
    ) -> List[SensoryMemoryItem]:
        """读取最新的 N 条数据"""
        redis = await self._get_redis()
        stream_key = self._build_stream_key(user_id, stream_type)
        
        try:
            client = redis._client
            entries = await client.xrevrange(stream_key, count=count)
            
            items = []
            for entry_id, fields in reversed(entries):  # 恢复时间顺序
                item = self._entry_to_item(entry_id, fields, user_id, stream_type)
                if item:
                    items.append(item)
            
            return items
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Read latest failed: {e}")
            return []
    
    async def subscribe(
        self,
        user_id: str,
        stream_type: str,
        trace_id: Optional[str] = None,
    ) -> AsyncIterator[SensoryMemoryItem]:
        """
        订阅感觉记忆流 (阻塞读取新数据)
        
        Yields:
            新的感觉记忆项
        """
        redis = await self._get_redis()
        stream_key = self._build_stream_key(user_id, stream_type)
        last_id = "$"  # 只读取新数据
        
        client = redis._client
        
        while True:
            try:
                # 阻塞读取，超时 5 秒
                result = await client.xread(
                    {stream_key: last_id},
                    count=1,
                    block=5000,
                )
                
                if result:
                    for stream_name, entries in result:
                        for entry_id, fields in entries:
                            last_id = entry_id
                            item = self._entry_to_item(entry_id, fields, user_id, stream_type)
                            if item:
                                yield item
                                
            except Exception as e:
                self.logger.error(f"[{trace_id}] Subscribe error: {e}")
                break
    
    async def clear(
        self,
        user_id: str,
        stream_type: str,
        trace_id: Optional[str] = None,
    ) -> bool:
        """清空指定流"""
        redis = await self._get_redis()
        stream_key = self._build_stream_key(user_id, stream_type)
        
        try:
            client = redis._client
            await client.delete(stream_key)
            self.logger.info(f"[{trace_id}] Cleared stream: {stream_key}")
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Clear failed: {e}")
            return False
    
    def _entry_to_item(
        self,
        entry_id: str,
        fields: Dict[bytes, bytes],
        user_id: str,
        stream_type: str,
    ) -> Optional[SensoryMemoryItem]:
        """将 Stream entry 转换为 SensoryMemoryItem"""
        try:
            # 解码字段
            data_str = fields.get(b"data", b"{}").decode()
            timestamp_str = fields.get(b"timestamp", b"").decode()
            
            data = json.loads(data_str)
            
            return SensoryMemoryItem(
                id=entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                content=json.dumps(data, ensure_ascii=False),
                stream_id=entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
                raw_data=data,
                metadata=MemoryMetadata(
                    user_id=user_id,
                    category=stream_type,
                    created_at=datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.utcnow(),
                ),
            )
        except Exception as e:
            self.logger.warning(f"Failed to parse entry {entry_id}: {e}")
            return None
