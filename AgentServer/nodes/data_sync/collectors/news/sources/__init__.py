"""
新闻采集源实现

每个源负责从特定渠道抓取新闻。
"""

from .cls import CLSSource
from .miit import MIITSource
from .wallstreetcn import WallstreetcnSource
from .jin10 import Jin10Source
from .xueqiu import XueqiuSource
from .eastmoney import EastMoneySource
from .gov import GovSource
from .juejin import JuejinSource
from .thepaper import ThePaperSource

__all__ = [
    # 财经快讯
    "CLSSource",              # 财联社
    "WallstreetcnSource",     # 华尔街见闻
    "Jin10Source",            # 金十数据
    # 财经讨论
    "XueqiuSource",           # 雪球
    "EastMoneySource",        # 东方财富
    # 政策文件
    "MIITSource",             # 工信部
    "GovSource",              # 国务院
    # 其他
    "JuejinSource",           # 稀土掘金
    "ThePaperSource",         # 澎湃新闻
]
