"""
数据统计模块

基于采集的原始数据进行统计分析。
"""

from .daily_stats import DailyStatsCollector

__all__ = [
    "DailyStatsCollector",
]
