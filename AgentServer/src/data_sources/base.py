"""
数据源适配器基类

定义统一的异步接口，所有具体数据源适配器必须实现这些接口。
支持的数据源: Tushare, AKShare, BaoStock

标准数据结构:
- 所有 Adapter 返回的数据必须符合下面定义的 TypedDict 格式
- 字段命名统一使用 Tushare 风格 (ts_code, trade_date, etc.)
- 数值字段统一使用 float，日期字段统一使用 str (YYYYMMDD)
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, TypedDict
from dataclasses import dataclass
from enum import Enum
import asyncio
import time
import logging

import pandas as pd


logger = logging.getLogger(__name__)


# ==================== 标准数据结构定义 ====================

class StockBasicRecord(TypedDict, total=False):
    """股票基础信息 - 标准格式"""
    ts_code: str           # 必须: 股票代码 (600000.SH)
    symbol: str            # 必须: 纯数字代码 (600000)
    name: str              # 必须: 股票名称
    area: str              # 地区
    industry: str          # 行业
    market: str            # 市场 (主板/创业板/科创板/北交所)
    list_date: str         # 上市日期 (YYYYMMDD)
    list_status: str       # 上市状态 (L/D/P)
    # 可选的估值字段 (来自 daily_basic 合并)
    pe: float              # 市盈率
    pb: float              # 市净率
    total_mv: float        # 总市值 (亿元)
    circ_mv: float         # 流通市值 (亿元)


class DailyRecord(TypedDict, total=False):
    """日线行情 - 标准格式"""
    ts_code: str           # 必须: 股票代码
    trade_date: str        # 必须: 交易日期 (YYYYMMDD)
    open: float            # 开盘价
    high: float            # 最高价
    low: float             # 最低价
    close: float           # 收盘价
    pre_close: float       # 昨收价
    change: float          # 涨跌额
    pct_chg: float         # 涨跌幅 (%)
    vol: float             # 成交量 (手)
    amount: float          # 成交额 (千元)


class DailyBasicRecord(TypedDict, total=False):
    """每日指标 - 标准格式"""
    ts_code: str           # 必须: 股票代码
    trade_date: str        # 必须: 交易日期 (YYYYMMDD)
    close: float           # 收盘价
    turnover_rate: float   # 换手率 (%)
    turnover_rate_f: float # 换手率(自由流通)
    volume_ratio: float    # 量比
    pe: float              # 市盈率
    pe_ttm: float          # 市盈率TTM
    pb: float              # 市净率
    ps: float              # 市销率
    ps_ttm: float          # 市销率TTM
    dv_ratio: float        # 股息率 (%)
    dv_ttm: float          # 股息率TTM (%)
    total_share: float     # 总股本 (万股)
    float_share: float     # 流通股本 (万股)
    total_mv: float        # 总市值 (亿元) - 注意: 已转换单位
    circ_mv: float         # 流通市值 (亿元) - 注意: 已转换单位


class RealtimeQuoteRecord(TypedDict, total=False):
    """实时行情 - 标准格式"""
    ts_code: str           # 股票代码 (可选，key 已经是代码)
    close: float           # 必须: 当前价/最新价
    open: float            # 开盘价
    high: float            # 最高价
    low: float             # 最低价
    pre_close: float       # 昨收价
    pct_chg: float         # 涨跌幅 (%)
    vol: float             # 成交量 (股)
    amount: float          # 成交额 (元)


class MoneyflowRecord(TypedDict, total=False):
    """
    资金流向 - 标准格式 (行业/概念通用)
    
    数据来源: 东方财富板块资金流向 (moneyflow_ind_dc)
    - buy_*_amount: 各档净流入金额 (元)
    """
    ts_code: str           # 必须: 板块代码
    trade_date: str        # 必须: 交易日期 (YYYYMMDD)
    name: str              # 必须: 板块名称
    pct_change: float      # 涨跌幅 (%)
    close: float           # 收盘价/指数
    net_amount: float      # 主力净流入金额 (元)
    net_amount_rate: float # 主力净流入占比 (%)
    buy_elg_amount: float  # 特大单净流入 (元)
    buy_lg_amount: float   # 大单净流入 (元)
    buy_md_amount: float   # 中单净流入 (元)
    buy_sm_amount: float   # 小单净流入 (元)


class LimitListRecord(TypedDict, total=False):
    """涨跌停 - 标准格式"""
    ts_code: str           # 必须: 股票代码
    trade_date: str        # 必须: 交易日期 (YYYYMMDD)
    name: str              # 股票名称
    industry: str          # 行业
    close: float           # 收盘价
    pct_chg: float         # 涨跌幅 (%)
    amount: float          # 成交额 (千元)
    limit_amount: float    # 板上成交额 (千元)
    float_mv: float        # 流通市值
    total_mv: float        # 总市值
    turnover_ratio: float  # 换手率
    fd_amount: float       # 封单金额
    first_time: str        # 首次涨停时间
    last_time: str         # 最后涨停时间
    open_times: int        # 打开次数
    up_stat: str           # 涨停统计 (N/T)
    limit_times: int       # 连续涨停次数
    limit: str             # 必须: 涨跌停类型 (U-涨停 D-跌停)


class IndexBasicRecord(TypedDict, total=False):
    """指数基础信息 - 标准格式"""
    ts_code: str           # 必须: 指数代码
    name: str              # 必须: 指数名称
    fullname: str          # 全称
    market: str            # 市场
    publisher: str         # 发布商
    index_type: str        # 类型
    category: str          # 分类
    base_date: str         # 基期
    base_point: float      # 基点
    list_date: str         # 发布日期


class IndexDailyRecord(TypedDict, total=False):
    """指数日线 - 标准格式"""
    ts_code: str           # 必须: 指数代码
    trade_date: str        # 必须: 交易日期 (YYYYMMDD)
    open: float            # 开盘
    high: float            # 最高
    low: float             # 最低
    close: float           # 收盘
    pre_close: float       # 昨收
    change: float          # 涨跌点
    pct_chg: float         # 涨跌幅 (%)
    vol: float             # 成交量 (手)
    amount: float          # 成交额 (千元)


class KlineRecord(TypedDict, total=False):
    """K线数据 - 标准格式"""
    time: str              # 必须: 时间 (日期或日期时间)
    open: float            # 必须: 开盘价
    high: float            # 必须: 最高价
    low: float             # 必须: 最低价
    close: float           # 必须: 收盘价
    volume: float          # 成交量
    amount: float          # 成交额


class NewsRecord(TypedDict, total=False):
    """新闻 - 标准格式"""
    ts_code: str           # 相关股票代码 (可选)
    title: str             # 必须: 标题
    content: str           # 内容摘要
    datetime: str          # 必须: 发布时间
    url: str               # 原文链接
    source: str            # 来源
    type: str              # 类型 (news/announcement)


class DataSourceType(Enum):
    """数据源类型"""
    TUSHARE = "tushare"
    AKSHARE = "akshare"
    BAOSTOCK = "baostock"


@dataclass
class DataSourceCapability:
    """数据源能力描述"""
    stock_basic: bool = False          # 股票基础信息
    daily_quotes: bool = False         # 日线行情
    daily_basic: bool = False          # 每日指标 (PE/PB/市值)
    realtime_quotes: bool = False      # 实时行情
    financial_data: bool = False       # 财务数据 (三大报表+指标)
    money_flow: bool = False           # 资金流向
    limit_data: bool = False           # 涨跌停数据
    index_data: bool = False           # 指数数据
    trade_calendar: bool = False       # 交易日历
    news: bool = False                 # 新闻公告
    kline: bool = False                # K线数据


class TokenBucket:
    """
    令牌桶算法实现，用于 API 频率控制
    """
    
    def __init__(self, rate: float, capacity: int):
        """
        Args:
            rate: 每秒产生的令牌数
            capacity: 令牌桶最大容量
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_time = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> float:
        """获取令牌，返回需要等待的时间"""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_time
            
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_time = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            
            return (tokens - self.tokens) / self.rate
    
    async def wait_and_acquire(self, tokens: int = 1) -> None:
        """等待并获取令牌"""
        wait_time = await self.acquire(tokens)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
            await self.acquire(tokens)


class AsyncDataSourceAdapter(ABC):
    """
    异步数据源适配器基类
    
    所有具体数据源适配器必须继承此类并实现相应方法。
    不支持的功能应返回 None 或空列表。
    """
    
    def __init__(self):
        self._initialized = False
        self._priority: Optional[int] = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    # ==================== 元信息 ====================
    
    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def source_type(self) -> DataSourceType:
        """数据源类型"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def capability(self) -> DataSourceCapability:
        """数据源能力"""
        raise NotImplementedError
    
    @property
    def priority(self) -> int:
        """优先级 (数字越大优先级越高)"""
        if self._priority is not None:
            return self._priority
        return self._get_default_priority()
    
    @abstractmethod
    def _get_default_priority(self) -> int:
        """默认优先级"""
        raise NotImplementedError
    
    # ==================== 生命周期 ====================
    
    @abstractmethod
    async def initialize(self) -> None:
        """初始化适配器"""
        raise NotImplementedError
    
    @abstractmethod
    async def shutdown(self) -> None:
        """关闭适配器"""
        raise NotImplementedError
    
    @abstractmethod
    async def is_available(self) -> bool:
        """检查适配器是否可用"""
        raise NotImplementedError
    
    async def health_check(self) -> bool:
        """健康检查"""
        return await self.is_available()
    
    # ==================== 股票基础信息 ====================
    
    async def get_stock_basic(
        self,
        ts_code: Optional[str] = None,
        list_status: str = "L",
    ) -> List[Dict[str, Any]]:
        """获取股票基础信息"""
        return []
    
    # ==================== 日线数据 ====================
    
    async def get_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: str = "qfq",
    ) -> List[Dict[str, Any]]:
        """获取日线行情数据"""
        return []
    
    async def get_daily_basic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取每日指标 (PE/PB/换手率/市值等)"""
        return []
    
    # ==================== 实时行情 ====================
    
    async def get_realtime_quotes(
        self,
        ts_codes: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        获取实时行情
        
        Returns:
            { "000001": {"close": 10.0, "pct_chg": 1.2, ...}, ... }
        """
        return {}
    
    async def get_realtime_index_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取指数实时行情"""
        return {}
    
    # ==================== 财务数据 ====================
    
    async def get_financial_indicator(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取财务指标"""
        return []
    
    async def get_income_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取利润表"""
        return []
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取资产负债表"""
        return []
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取现金流量表"""
        return []
    
    async def get_financial_data(
        self,
        ts_code: str,
        limit: int = 4,
    ) -> Dict[str, Any]:
        """获取完整财务数据 (三大报表 + 指标)"""
        return {}
    
    # ==================== 资金流向 ====================
    
    async def get_moneyflow_industry(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取行业资金流向"""
        return []
    
    async def get_moneyflow_concept(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取概念板块资金流向"""
        return []
    
    async def get_moneyflow_hsgt(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取沪深港通资金流向"""
        return []
    
    # ==================== 涨跌停 ====================
    
    async def get_limit_list(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取涨跌停统计"""
        return []
    
    async def get_stk_limit(
        self,
        trade_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取涨跌停价格"""
        return []
    
    # ==================== 指数 ====================
    
    async def get_index_basic(
        self,
        market: str = "SSE",
        ts_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取指数基础信息"""
        return []
    
    async def get_index_daily(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取指数日线数据"""
        return []
    
    # ==================== 交易日历 ====================
    
    async def get_trade_calendar(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """获取交易日列表"""
        return []
    
    async def get_latest_trade_date(self) -> Optional[str]:
        """获取最近交易日"""
        return None
    
    async def is_trading_time(self) -> bool:
        """检查当前是否为交易时间"""
        from datetime import datetime
        now = datetime.now()
        current_time = now.hour * 100 + now.minute
        
        # 上午 09:30 - 11:30
        if 930 <= current_time <= 1130:
            return True
        # 下午 13:00 - 15:00
        if 1300 <= current_time <= 1500:
            return True
        return False
    
    # ==================== 新闻公告 ====================
    
    async def get_news(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """获取新闻"""
        return []
    
    async def get_stock_news(
        self,
        symbol: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """获取个股新闻"""
        return []
    
    # ==================== K线数据 ====================
    
    async def get_kline(
        self,
        code: str,
        period: str = "day",
        limit: int = 120,
        adj: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取K线数据
        
        Args:
            code: 股票代码
            period: 周期 (day/week/month/5m/15m/30m/60m)
            limit: 返回数量
            adj: 复权类型 (None/qfq/hfq)
        """
        return []
    
    # ==================== 辅助方法 ====================
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        try:
            if value is None or value == '' or value == 'None' or pd.isna(value):
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value, default: int = 0) -> int:
        """安全转换为整数，处理 NaN 和 None"""
        try:
            if value is None or value == '' or value == 'None' or pd.isna(value):
                return default
            return int(float(value))  # 先转 float 再转 int，避免 "1.0" 这类字符串
        except (ValueError, TypeError):
            return default
    
    def _normalize_ts_code(self, code: str) -> str:
        """
        标准化股票代码为 ts_code 格式 (如 000001.SZ)
        """
        code = str(code).strip().upper()
        
        # 已经是标准格式
        if '.' in code:
            return code
        
        # 去掉可能的前缀
        for prefix in ['SH', 'SZ', 'BJ']:
            if code.startswith(prefix):
                code = code[2:]
                break
        
        code = code.zfill(6)
        
        # 根据代码判断交易所
        if code.startswith(('60', '68', '90')):
            return f"{code}.SH"
        elif code.startswith(('00', '30', '20')):
            return f"{code}.SZ"
        elif code.startswith(('8', '4')):
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"
    
    def _extract_code(self, ts_code: str) -> str:
        """从 ts_code 提取纯数字代码"""
        return ts_code.split('.')[0] if '.' in ts_code else ts_code
