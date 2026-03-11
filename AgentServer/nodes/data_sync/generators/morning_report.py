"""
早报生成器

每个交易日 8:50 触发，生成并推送早报。
"""

from typing import Dict, Any
from datetime import datetime

from core.base import BaseGenerator
from src.report import ReportGenerator, ReportType


class MorningReportGenerator(BaseGenerator):
    """
    早报生成器
    
    - 调度时间: 每个交易日 8:50
    - 数据范围: 过去 16 小时的新闻事件 (覆盖前一晚到早上)
    - 推送渠道: 企业微信 + 前端 WebSocket
    """
    
    name = "morning_report"
    description = "生成早报"
    default_schedule = "50 8 * * 1-5"  # 周一至周五 8:50
    run_at_startup = False
    
    def __init__(self):
        super().__init__()
        self._generator = ReportGenerator()
    
    async def generate(self) -> Dict[str, Any]:
        """生成并推送早报"""
        today = datetime.now().strftime("%Y-%m-%d")
        trace_id = f"morning_{today.replace('-', '')}"
        
        self.logger.info(f"[{trace_id}] Starting morning report generation")
        
        result = await self._generator.generate_and_push(
            report_type=ReportType.MORNING,
            date=today,
            hours_back=16,  # 覆盖前一晚 5pm 到今早 9am
            push_wechat=True,
            push_websocket=True,
            trace_id=trace_id,
        )
        
        if not result.success:
            self.logger.error(f"[{trace_id}] Morning report generation failed: {result.errors}")
            return {
                "success": False,
                "count": 0,
                "errors": result.errors,
            }
        
        report = result.report
        
        return {
            "success": True,
            "count": report.stats.event_count if report else 0,
            "report_id": report.id if report else None,
            "event_count": report.stats.event_count if report else 0,
            "high_importance_count": report.stats.high_importance_count if report else 0,
            "pushed_wechat": result.pushed_wechat,
            "pushed_websocket": result.pushed_websocket,
            "elapsed_ms": result.elapsed_ms,
        }
