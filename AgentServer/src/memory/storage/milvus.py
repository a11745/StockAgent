"""
Milvus 存储实现

封装 core.managers.milvus_manager，提供记忆存储功能。
"""

import logging
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

from .abstract import AbstractStore, MemoryItem
from ..types import MemoryMetadata


class MilvusStore(AbstractStore):
    """
    Milvus 向量存储
    
    使用 milvus_manager 单例进行向量存储和检索。
    
    Args:
        collection_name: Milvus 集合名称
        dim: 向量维度（默认从 llm_manager 获取）
    
    Example:
        store = MilvusStore(collection_name="news_memory")
        await store.upsert([item1, item2], trace_id="xxx")
        results = await store.search(query_vector, top_k=10)
    """
    
    def __init__(
        self,
        collection_name: str,
        dim: int = 1536,  # OpenAI text-embedding-3-small 默认维度
    ):
        self.collection_name = collection_name
        self.dim = dim
        self.logger = logging.getLogger(f"src.memory.MilvusStore.{collection_name}")
        self._milvus_manager = None
    
    async def _get_milvus_manager(self):
        """延迟导入 milvus_manager，避免循环依赖"""
        if self._milvus_manager is None:
            from core.managers import milvus_manager
            self._milvus_manager = milvus_manager
        return self._milvus_manager
    
    def _log(self, level: str, message: str, trace_id: Optional[str] = None) -> None:
        """带 trace_id 的日志记录"""
        prefix = f"[{trace_id}] " if trace_id else ""
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(f"{prefix}{message}")
    
    def _item_to_milvus_dict(self, item: MemoryItem) -> Dict[str, Any]:
        """将 MemoryItem 转换为 Milvus 插入格式"""
        return {
            "id": item.id,
            "content": item.content[:65000] if item.content else "",
            "vector": item.vector,
            "ts_code": item.metadata.ts_code or "",
            "publish_date": getattr(item.metadata, "publish_date", "") or "",
            "source": item.metadata.source or "",
            "category": item.metadata.category or "",
            "created_at": item.metadata.created_at.isoformat(),
            "metadata_json": json.dumps(item.metadata.model_dump(), default=str),
        }
    
    def _milvus_hit_to_item(self, hit: Dict[str, Any], score: float) -> MemoryItem:
        """将 Milvus 检索结果转换为 MemoryItem"""
        entity = hit.get("entity", hit)
        
        metadata_json = entity.get("metadata_json", "{}")
        try:
            metadata_dict = json.loads(metadata_json) if metadata_json else {}
        except json.JSONDecodeError:
            metadata_dict = {}
        
        return MemoryItem(
            id=entity.get("id", ""),
            content=entity.get("content", ""),
            vector=entity.get("vector", []),
            score=score,
            metadata=MemoryMetadata(
                user_id=metadata_dict.get("user_id", "system"),
                ts_code=entity.get("ts_code") or metadata_dict.get("ts_code"),
                source=entity.get("source") or metadata_dict.get("source"),
                category=entity.get("category") or metadata_dict.get("category"),
                importance_score=metadata_dict.get("importance_score", 0.5),
            ),
        )
    
    async def upsert(
        self,
        items: List[MemoryItem],
        trace_id: Optional[str] = None,
    ) -> int:
        """插入或更新记忆项"""
        if not items:
            return 0
        
        self._log("debug", f"Upserting {len(items)} items", trace_id)
        
        milvus = await self._get_milvus_manager()
        
        # 确保集合存在
        await self._ensure_collection(milvus)
        
        # 转换为 Milvus 格式
        data = [self._item_to_milvus_dict(item) for item in items]
        
        try:
            # 使用 milvus_manager 的插入方法
            result = await milvus.insert(
                collection_name=self.collection_name,
                data=data,
            )
            count = result.get("insert_count", len(items)) if isinstance(result, dict) else len(items)
            self._log("info", f"Upserted {count} items", trace_id)
            return count
        except Exception as e:
            self._log("error", f"Upsert failed: {e}", trace_id)
            raise
    
    async def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """向量相似度搜索"""
        self._log("debug", f"Searching top_k={top_k}", trace_id)
        
        milvus = await self._get_milvus_manager()
        
        # 构建过滤表达式
        filter_expr = self._build_filter_expr(filters) if filters else None
        
        try:
            results = await milvus.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                output_fields=["id", "content", "ts_code", "publish_date", "source", "category", "created_at", "metadata_json"],
            )
            
            items = []
            for hit in results:
                score = hit.get("score", hit.get("distance", 0.0))
                item = self._milvus_hit_to_item(hit, score)
                items.append(item)
            
            self._log("debug", f"Found {len(items)} items", trace_id)
            return items
        except Exception as e:
            self._log("error", f"Search failed: {e}", trace_id)
            raise
    
    async def delete(
        self,
        ids: List[str],
        trace_id: Optional[str] = None,
    ) -> int:
        """删除记忆项"""
        if not ids:
            return 0
        
        self._log("debug", f"Deleting {len(ids)} items", trace_id)
        
        milvus = await self._get_milvus_manager()
        
        try:
            expr = f'id in {ids}'
            result = await milvus.delete(
                collection_name=self.collection_name,
                expr=expr,
            )
            count = result.get("delete_count", len(ids)) if isinstance(result, dict) else len(ids)
            self._log("info", f"Deleted {count} items", trace_id)
            return count
        except Exception as e:
            self._log("error", f"Delete failed: {e}", trace_id)
            raise
    
    async def get(
        self,
        ids: List[str],
        trace_id: Optional[str] = None,
    ) -> List[MemoryItem]:
        """根据 ID 获取记忆项"""
        if not ids:
            return []
        
        self._log("debug", f"Getting {len(ids)} items", trace_id)
        
        milvus = await self._get_milvus_manager()
        
        try:
            expr = f'id in {ids}'
            results = await milvus.query(
                collection_name=self.collection_name,
                expr=expr,
                output_fields=["id", "content", "vector", "ts_code", "publish_date", "source", "category", "created_at", "metadata_json"],
            )
            
            items = [self._milvus_hit_to_item(hit, 0.0) for hit in results]
            return items
        except Exception as e:
            self._log("error", f"Get failed: {e}", trace_id)
            raise
    
    async def count(self, trace_id: Optional[str] = None) -> int:
        """获取记忆项总数"""
        milvus = await self._get_milvus_manager()
        
        try:
            result = await milvus.count(collection_name=self.collection_name)
            return result
        except Exception as e:
            self._log("error", f"Count failed: {e}", trace_id)
            return 0
    
    async def clear(self, trace_id: Optional[str] = None) -> bool:
        """清空所有记忆项"""
        self._log("warning", "Clearing all items", trace_id)
        
        milvus = await self._get_milvus_manager()
        
        try:
            await milvus.drop_collection(self.collection_name)
            await self._ensure_collection(milvus)
            return True
        except Exception as e:
            self._log("error", f"Clear failed: {e}", trace_id)
            return False
    
    async def _ensure_collection(self, milvus) -> None:
        """确保集合存在，不存在则创建"""
        try:
            exists = await milvus.has_collection(self.collection_name)
            if not exists:
                await milvus.create_collection(
                    collection_name=self.collection_name,
                    dim=self.dim,
                    fields=[
                        {"name": "id", "type": "VARCHAR", "max_length": 64, "is_primary": True},
                        {"name": "content", "type": "VARCHAR", "max_length": 65535},
                        {"name": "vector", "type": "FLOAT_VECTOR", "dim": self.dim},
                        {"name": "ts_code", "type": "VARCHAR", "max_length": 20},
                        {"name": "publish_date", "type": "VARCHAR", "max_length": 10},
                        {"name": "source", "type": "VARCHAR", "max_length": 50},
                        {"name": "category", "type": "VARCHAR", "max_length": 50},
                        {"name": "created_at", "type": "VARCHAR", "max_length": 30},
                        {"name": "metadata_json", "type": "VARCHAR", "max_length": 65535},
                    ],
                )
                self._log("info", f"Created collection: {self.collection_name}")
        except Exception as e:
            self._log("warning", f"Collection check/create failed: {e}")
    
    def _build_filter_expr(self, filters: Dict[str, Any]) -> str:
        """构建 Milvus 过滤表达式"""
        conditions = []
        
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(f'{key} == "{value}"')
            elif isinstance(value, list):
                values_str = ", ".join(f'"{v}"' for v in value)
                conditions.append(f'{key} in [{values_str}]')
            elif isinstance(value, dict):
                # 支持范围查询: {"publish_date": {"$gte": "20240101", "$lte": "20240131"}}
                for op, val in value.items():
                    if op == "$gte":
                        conditions.append(f'{key} >= "{val}"')
                    elif op == "$lte":
                        conditions.append(f'{key} <= "{val}"')
                    elif op == "$gt":
                        conditions.append(f'{key} > "{val}"')
                    elif op == "$lt":
                        conditions.append(f'{key} < "{val}"')
            else:
                conditions.append(f'{key} == {value}')
        
        return " and ".join(conditions) if conditions else ""
