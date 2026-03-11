"""
生成任务

报告生成、内容输出等任务。
"""

from .morning_report import MorningReportGenerator
from .noon_report import NoonReportGenerator


__all__ = [
    "MorningReportGenerator",
    "NoonReportGenerator",
]
