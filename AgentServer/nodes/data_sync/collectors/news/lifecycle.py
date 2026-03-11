"""
新闻数据生命周期管理任务

定期执行:
1. 压缩过期的热数据 (Hot → Warm)
2. 删除过期的冷数据 (Cold)
3. 清理 Milvus 孤儿向量

调度策略:
- 每天凌晨 3:00 执行
"""

import uuid
from typing import Dict, Any, Optional

from core.base import BaseCollector
from core.settings import settings
from src.collector.lifecycle import NewsLifecycleManager


class NewsLifecycleCollector(BaseCollector):
    """
    新闻生命周期管理任务
    
    分层存储策略:
    - Hot:  完整数据 (1-30天，按类型不同)
    - Warm: 压缩数据，删除 content/vector (7-180天)
    - Cold: 删除 (超过保留期限)
    
    Example:
        collector = NewsLifecycleCollector()
        result = await collector.run()
    """
    
    name = "news_lifecycle"
    description = "新闻数据生命周期管理"
    default_schedule = "0 3 * * *"  # 每天凌晨 3:00
    run_at_startup = False  # 启动时不立即运行，等待调度时间
    
    def __init__(self):
        super().__init__()
        self._manager = NewsLifecycleManager()
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间"""
        return getattr(settings.data_sync, "news_lifecycle_schedule", None) or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """
        执行生命周期管理
        """
        trace_id = uuid.uuid4().hex[:8]
        
        self.logger.info(f"[{trace_id}] Starting news lifecycle management...")
        
        result = await self._manager.process_lifecycle(
            batch_size=500,
            trace_id=trace_id,
        )
        
        self.logger.info(
            f"[{trace_id}] Lifecycle done: "
            f"compressed={result.compressed}, deleted={result.deleted}"
        )
        
        return {
            "count": result.total_processed,
            "compressed": result.compressed,
            "deleted": result.deleted,
            "archived": result.archived,
        }
    
    async def get_stats(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        """获取数据统计"""
        return await self._manager.get_stats(trace_id=trace_id)
    
    @classmethod
    def get_retention_policies(cls) -> Dict[str, Dict[str, int]]:
        """获取保留策略"""
        return NewsLifecycleManager.get_retention_policies()


# 全局实例
news_lifecycle_collector = NewsLifecycleCollector()
