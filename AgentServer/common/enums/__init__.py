"""
枚举定义 (core/nodes 层使用)

仅包含 core/ 和 nodes/ 共用的枚举。
src/ 模块的枚举留在 src/ 内部，保持 src/ 可独立迁移。
"""

from .node import NodeType, TaskType, TaskStatus, SignalType
from .market import MarketCycle, ThemeStatus
from .trade import TradeDirection, StrategyType
from .backtest import UniverseType, ExcludeRule, FactorCategory

__all__ = [
    # 节点/任务
    "NodeType",
    "TaskType",
    "TaskStatus",
    "SignalType",
    # 市场/板块
    "MarketCycle",
    "ThemeStatus",
    # 交易/策略
    "TradeDirection",
    "StrategyType",
    # 回测
    "UniverseType",
    "ExcludeRule",
    "FactorCategory",
]
