"""
处理任务基类

用于数据处理、分析、维护等任务。
"""

from abc import abstractmethod
from typing import Dict, Any

from .scheduled_job import ScheduledJob


class BaseTask(ScheduledJob):
    """
    处理任务基类
    
    用于数据处理、聚类、统计、清理等非采集类任务。
    
    Example:
        class EventClusteringTask(BaseTask):
            name = "event_clustering"
            description = "事件聚类"
            default_schedule = "*/10 * * * *"
            
            async def execute(self) -> dict:
                result = await cluster_engine.process_pending()
                return {"count": result.new_events}
    """
    
    _log_prefix = "task"
    
    @abstractmethod
    async def execute(self) -> Dict[str, Any]:
        """
        执行任务
        
        Returns:
            执行结果，至少包含 count 字段
        """
        raise NotImplementedError
    
    async def _do_work(self) -> Dict[str, Any]:
        """内部调用 execute()"""
        return await self.execute()
