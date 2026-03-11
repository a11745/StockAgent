"""
data_sync 数据采集器

只包含真正的数据采集任务（从外部获取数据）。
处理任务和生成任务分别在 tasks/ 和 generators/ 目录。

目录结构:
- stock/  股票数据采集 (行情、财务、资金流向)
- news/   新闻采集 (财经新闻、政策文件)
"""

# 股票数据采集
from .stock import (
    StockBasicCollector,
    StockDailyCollector,
    DailyBasicCollector,
    FinaIndicatorCollector,
    IndexBasicCollector,
    IndexDailyCollector,
    LimitListCollector,
    MoneyflowConceptCollector,
    MoneyflowIndustryCollector,
)

# 新闻采集
from .news import (
    StockNewsCollector,
    HotNewsCollector,
    MultiSourceCollector,
)

__all__ = [
    # 股票数据
    "StockBasicCollector",
    "StockDailyCollector",
    "DailyBasicCollector",
    "FinaIndicatorCollector",
    "IndexBasicCollector",
    "IndexDailyCollector",
    "LimitListCollector",
    "MoneyflowConceptCollector",
    "MoneyflowIndustryCollector",
    # 新闻采集
    "StockNewsCollector",
    "HotNewsCollector",
    "MultiSourceCollector",
]
