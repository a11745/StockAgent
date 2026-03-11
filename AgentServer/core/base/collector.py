"""
采集器基类

用于从外部数据源采集数据的任务。
"""

from abc import abstractmethod
from typing import Dict, Any

from .scheduled_job import ScheduledJob


class BaseCollector(ScheduledJob):
    """
    采集器基类
    
    用于从外部数据源采集数据。
    
    Example:
        class StockBasicCollector(BaseCollector):
            name = "stock_basic"
            description = "采集股票基础信息"
            default_schedule = "0 9 * * 1-5"
            
            async def collect(self) -> dict:
                data = await data_source_manager.get_stock_basic()
                await mongo_manager.bulk_upsert("stock_basic", data)
                return {"count": len(data)}
    """
    
    _log_prefix = "collector"
    
    @abstractmethod
    async def collect(self) -> Dict[str, Any]:
        """
        执行采集
        
        Returns:
            采集结果，至少包含 count 字段
        """
        raise NotImplementedError
    
    async def _do_work(self) -> Dict[str, Any]:
        """内部调用 collect()"""
        return await self.collect()
