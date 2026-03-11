"""
市场相关枚举
"""

from enum import Enum


class MarketCycle(str, Enum):
    """市场周期"""
    ICE_POINT = "ice_point"        # 冰点期
    DECLINE = "decline"            # 退潮期
    CHAOS = "chaos"                # 混沌期 (无主线轮动)
    INCUBATION = "incubation"      # 萌芽期
    MAIN_UPWARD = "main_upward"    # 主升期
    ROTATION = "rotation"          # 分歧/轮动期
    UNKNOWN = "unknown"            # 未知


class ThemeStatus(str, Enum):
    """板块状态"""
    MAIN_THEME = "main_theme"       # 当前主线
    STRONG_FOCUS = "strong_focus"   # 强势关注
    RISING = "rising"               # 上升中
    ROTATING = "rotating"           # 轮动中
    FADING = "fading"               # 衰退中
    NORMAL = "normal"               # 普通
