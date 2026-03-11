"""
节点相关枚举
"""

from enum import Enum


class NodeType(str, Enum):
    """节点类型"""
    WEB = "web"
    DATA_SYNC = "data_sync"
    MCP = "mcp"
    INFERENCE = "inference"
    LISTENER = "listener"
    BACKTEST = "backtest"


class TaskType(str, Enum):
    """任务类型"""
    STOCK_ANALYSIS = "stock_analysis"
    MARKET_OVERVIEW = "market_overview"
    NEWS_SENTIMENT = "news_sentiment"
    STRATEGY_BACKTEST = "strategy_backtest"
    CUSTOM_QUERY = "custom_query"


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SignalType(str, Enum):
    """交易信号"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"
