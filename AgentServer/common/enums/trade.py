"""
交易/策略相关枚举 (core/nodes 层使用)

注: PatternType, TradeOutcome 属于 src/memory，定义在 src/memory/longterm/procedural.py
"""

from enum import Enum


class TradeDirection(str, Enum):
    """交易方向 (回测引擎使用)"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class StrategyType(str, Enum):
    """监听策略类型 (Listener 节点使用)"""
    LIMIT_OPEN = "limit_open"           # 涨跌停打开
    PRICE_CHANGE = "price_change"       # 涨跌幅阈值
    VOLUME_SURGE = "volume_surge"       # 放量突破
    MA_CROSS = "ma_cross"               # 均线交叉
    MA5_BUY = "ma5_buy"                 # 5日线低吸
    CUSTOM = "custom"                   # 自定义策略
