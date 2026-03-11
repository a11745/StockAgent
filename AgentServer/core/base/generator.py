"""
生成器基类

用于报告生成、内容输出等任务。
"""

from abc import abstractmethod
from typing import Dict, Any

from .scheduled_job import ScheduledJob


class BaseGenerator(ScheduledJob):
    """
    生成器基类
    
    用于报告生成、内容输出等任务。
    
    Example:
        class MorningReportGenerator(BaseGenerator):
            name = "morning_report"
            description = "生成早报"
            default_schedule = "50 8 * * 1-5"
            run_at_startup = False
            
            async def generate(self) -> dict:
                result = await report_generator.generate_and_push(
                    report_type=ReportType.MORNING
                )
                return {"count": result.report.stats.event_count}
    """
    
    _log_prefix = "generator"
    
    @abstractmethod
    async def generate(self) -> Dict[str, Any]:
        """
        执行生成
        
        Returns:
            生成结果，至少包含 count 字段
        """
        raise NotImplementedError
    
    async def _do_work(self) -> Dict[str, Any]:
        """内部调用 generate()"""
        return await self.generate()
