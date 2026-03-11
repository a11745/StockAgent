"""
回测相关枚举
"""

from enum import Enum


class UniverseType(str, Enum):
    """股票池类型"""
    ALL_A = "all_a"          # 全A股


class ExcludeRule(str, Enum):
    """排除规则"""
    ST = "st"                # ST股票
    NEW_STOCK = "new_stock"  # 次新股 (上市不满1年)
    LIMIT_UP = "limit_up"    # 涨停股 (一字板)
    LIMIT_DOWN = "limit_down" # 跌停股


class FactorCategory(str, Enum):
    """因子分类"""
    MOMENTUM = "momentum"      # 动量因子
    VALUE = "value"            # 价值因子
    QUALITY = "quality"        # 质量因子
    GROWTH = "growth"          # 成长因子
    VOLATILITY = "volatility"  # 波动因子
    LIQUIDITY = "liquidity"    # 流动性因子
    TECHNICAL = "technical"    # 技术因子
