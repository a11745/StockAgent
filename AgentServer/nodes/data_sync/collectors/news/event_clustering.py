"""
事件聚类采集器

深度去重阶段：定期处理未聚类的新闻，使用 LLM 进行事件识别和聚类。

调度策略:
- 每 30 分钟执行一次
- 每次处理最多 100 条未聚类新闻
"""

import uuid
from typing import Dict, Any, Optional

from core.base import BaseCollector
from core.settings import settings
from src.collector.event_cluster import EventClusterEngine


class EventClusteringCollector(BaseCollector):
    """
    事件聚类采集器
    
    深度去重阶段，使用 LLM 进行:
    1. 事件指纹提取 (主体 + 动作 + 关键词)
    2. 相似事件聚类
    3. 主新闻/相关新闻标记
    
    Example:
        collector = EventClusteringCollector()
        result = await collector.run()
    """
    
    name = "event_clustering"
    description = "新闻事件聚类 (深度去重)"
    default_schedule = "*/30 * * * *"  # 每 30 分钟
    run_at_startup = False  # 启动时不立即运行，等待调度时间
    
    def __init__(self):
        super().__init__()
        self._engine = EventClusterEngine()
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间"""
        return getattr(settings.data_sync, "event_clustering_schedule", None) or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """
        执行事件聚类
        """
        trace_id = uuid.uuid4().hex[:8]
        
        self.logger.info(f"[{trace_id}] Starting event clustering...")
        
        result = await self._engine.process_pending_news(
            batch_size=100,
            trace_id=trace_id,
        )
        
        self.logger.info(
            f"[{trace_id}] Event clustering done: "
            f"processed={result.total_processed}, "
            f"new_events={result.new_events}, "
            f"merged={result.merged_news}"
        )
        
        return {
            "count": result.total_processed,
            "new_events": result.new_events,
            "merged_news": result.merged_news,
        }
    
    async def get_recent_events(
        self,
        hours: int = 24,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ):
        """获取最近的事件"""
        return await self._engine.get_recent_events(
            hours=hours,
            limit=limit,
            trace_id=trace_id,
        )


# 全局实例
event_clustering_collector = EventClusteringCollector()
