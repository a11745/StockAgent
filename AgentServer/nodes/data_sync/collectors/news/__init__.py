"""
新闻采集器

采集财经新闻、政策文件、热点事件等。

注意: 
- 事件聚类已迁移到 tasks/event_clustering.py
- 生命周期管理已迁移到 tasks/news_lifecycle.py
"""

from .stock_news import StockNewsCollector
from .hot_news import HotNewsCollector
from .multi_source import MultiSourceCollector, multi_source_collector

__all__ = [
    # 新闻采集
    "StockNewsCollector",          # 涨跌停股票新闻 (AKShare)
    "HotNewsCollector",            # 热点新闻看板
    "MultiSourceCollector",        # 多源新闻聚合 (财联社/华尔街见闻/金十等)
    # 全局实例
    "multi_source_collector",
]
