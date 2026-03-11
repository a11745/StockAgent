"""
处理任务

数据处理、聚类、统计、清理等非采集类任务。
"""

from .event_clustering import EventClusteringTask
from .news_lifecycle import NewsLifecycleTask
from .daily_stats import DailyStatsTask


__all__ = [
    "EventClusteringTask",
    "NewsLifecycleTask",
    "DailyStatsTask",
]
