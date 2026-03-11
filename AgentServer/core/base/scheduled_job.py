"""
定时任务基类

所有定时调度任务的公共基类，提供：
- 调度时间管理
- 运行状态追踪
- 统一的 run() 入口

子类：
- BaseCollector: 数据采集，实现 collect()
- BaseTask: 处理任务，实现 execute()
- BaseGenerator: 生成任务，实现 generate()
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime
import time
import logging


class ScheduledJob(ABC):
    """
    定时任务基类
    
    所有定时调度任务继承此类。
    
    Attributes:
        name: 任务名称
        description: 任务描述
        default_schedule: 默认 cron 表达式
        run_at_startup: 启动时是否立即运行
    """
    
    name: str
    description: str
    default_schedule: str
    run_at_startup: bool = True
    
    # 子类需要设置的日志前缀
    _log_prefix: str = "job"
    
    @property
    def schedule(self) -> str:
        """获取调度时间，子类可重写从配置读取"""
        return self.default_schedule
    
    def __init__(self):
        self.logger = logging.getLogger(f"{self._log_prefix}.{self.name}")
        self._last_run: Optional[datetime] = None
        self._last_result: Optional[dict] = None
    
    @abstractmethod
    async def _do_work(self) -> Dict[str, Any]:
        """
        执行具体工作
        
        子类必须实现，返回至少包含 count 字段的 dict。
        """
        raise NotImplementedError
    
    async def run(self) -> dict:
        """运行任务"""
        start_time = time.time()
        
        try:
            result = await self._do_work()
            
            duration_ms = (time.time() - start_time) * 1000
            
            self._last_run = datetime.utcnow()
            self._last_result = {
                "success": True,
                "count": result.get("count", 0),
                "duration_ms": duration_ms,
                **result,
            }
            
            return self._last_result
            
        except Exception as e:
            self.logger.exception(f"{self.__class__.__name__} failed: {e}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            self._last_run = datetime.utcnow()
            self._last_result = {
                "success": False,
                "error": str(e),
                "duration_ms": duration_ms,
            }
            
            return self._last_result
    
    @property
    def status(self) -> dict:
        """获取任务状态"""
        return {
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_result": self._last_result,
        }
