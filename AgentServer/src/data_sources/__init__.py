"""
数据源采集模块

提供统一的数据源适配器接口，支持多数据源自动切换和降级。

使用方式:
    from src.data_sources import TushareAdapter, AKShareAdapter, BaoStockAdapter
    
    # 创建适配器
    adapter = TushareAdapter(token="your_token")
    await adapter.initialize()
    
    # 获取数据
    stocks = await adapter.get_stock_basic()
    daily = await adapter.get_daily(ts_code="000001.SZ")
    
    # 关闭
    await adapter.shutdown()

数据源能力对比:
    | 功能          | Tushare | AKShare | BaoStock |
    |---------------|---------|---------|----------|
    | 股票列表      | ✅      | ✅      | ✅       |
    | 日线行情      | ✅      | ✅      | ✅       |
    | 实时行情      | ✅      | ✅      | ❌       |
    | 每日指标      | ✅      | ⚠️慢    | ⚠️部分   |
    | 财务数据      | ✅      | ❌      | ❌       |
    | 资金流向      | ✅      | ✅      | ❌       |
    | 涨跌停        | ✅      | ✅      | ❌       |
    | 新闻公告      | ⚠️付费  | ✅      | ❌       |
    | K线数据       | ✅      | ✅      | ✅       |
"""

from .base import (
    AsyncDataSourceAdapter,
    DataSourceType,
    DataSourceCapability,
    TokenBucket,
    # 标准数据结构
    StockBasicRecord,
    DailyRecord,
    DailyBasicRecord,
    RealtimeQuoteRecord,
    MoneyflowRecord,
    LimitListRecord,
    IndexBasicRecord,
    IndexDailyRecord,
    KlineRecord,
    NewsRecord,
)

from .tushare_adapter import TushareAdapter
from .akshare_adapter import AKShareAdapter
from .baostock_adapter import BaoStockAdapter


__all__ = [
    # 基类
    "AsyncDataSourceAdapter",
    "DataSourceType",
    "DataSourceCapability",
    "TokenBucket",
    # 标准数据结构
    "StockBasicRecord",
    "DailyRecord",
    "DailyBasicRecord",
    "RealtimeQuoteRecord",
    "MoneyflowRecord",
    "LimitListRecord",
    "IndexBasicRecord",
    "IndexDailyRecord",
    "KlineRecord",
    "NewsRecord",
    # 适配器
    "TushareAdapter",
    "AKShareAdapter",
    "BaoStockAdapter",
]
