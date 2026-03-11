"""
BaoStock 数据源适配器

提供 BaoStock 的异步封装，免费数据源，主要用于：
- 股票列表
- 日线行情
- 每日基础指标 (PE/PB)

返回数据格式遵循 base.py 中定义的标准 TypedDict 结构。
"""
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import re

import pandas as pd

from .base import (
    AsyncDataSourceAdapter,
    DataSourceType,
    DataSourceCapability,
    StockBasicRecord,
    DailyRecord,
    DailyBasicRecord,
    RealtimeQuoteRecord,
    IndexDailyRecord,
    KlineRecord,
)


class BaoStockAdapter(AsyncDataSourceAdapter):
    """
    BaoStock 数据源适配器
    
    特点:
    - 完全免费
    - 历史数据较全
    - 实时行情不支持
    - 需要登录/登出
    """
    
    def __init__(self):
        super().__init__()
        self._bs = None
        self._logged_in = False
    
    # ==================== 元信息 ====================
    
    @property
    def name(self) -> str:
        return "baostock"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.BAOSTOCK
    
    @property
    def capability(self) -> DataSourceCapability:
        return DataSourceCapability(
            stock_basic=True,
            daily_quotes=True,
            daily_basic=True,
            realtime_quotes=False,  # 不支持实时
            financial_data=False,
            money_flow=False,
            limit_data=False,
            index_data=True,
            trade_calendar=True,
            news=False,
            kline=True,
        )
    
    def _get_default_priority(self) -> int:
        return 10  # 最低优先级
    
    # ==================== 生命周期 ====================
    
    async def initialize(self) -> None:
        """初始化"""
        if self._initialized:
            return
        
        try:
            import baostock as bs
            self._bs = bs
            self._initialized = True
            self.logger.info("BaoStock adapter initialized ✓")
        except ImportError:
            self.logger.error("BaoStock not installed. Run: pip install baostock")
    
    async def shutdown(self) -> None:
        """关闭"""
        await self._logout()
        self._bs = None
        self._initialized = False
        self.logger.info("BaoStock adapter shutdown")
    
    async def is_available(self) -> bool:
        """检查是否可用"""
        return self._initialized and self._bs is not None
    
    async def _login(self) -> bool:
        """登录 BaoStock"""
        if self._logged_in:
            return True
        
        if not await self.is_available():
            return False
        
        try:
            loop = asyncio.get_event_loop()
            lg = await loop.run_in_executor(None, self._bs.login)
            
            if lg.error_code != '0':
                self.logger.error(f"BaoStock login failed: {lg.error_msg}")
                return False
            
            self._logged_in = True
            return True
        except Exception as e:
            self.logger.error(f"BaoStock login error: {e}")
            return False
    
    async def _logout(self) -> None:
        """登出 BaoStock"""
        if not self._logged_in:
            return
        
        try:
            if self._bs:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._bs.logout)
            self._logged_in = False
        except Exception as e:
            self.logger.warning(f"BaoStock logout error: {e}")
    
    def _convert_code(self, code: str) -> str:
        """将代码转换为 BaoStock 格式 (sh.600000 / sz.000001)"""
        code = str(code).strip()
        
        # 已经是 bs 格式
        if code.startswith(('sh.', 'sz.')):
            return code
        
        # 处理 ts_code 格式
        if '.' in code:
            parts = code.split('.')
            pure_code = parts[0]
            exchange = parts[1].lower()
            return f"{exchange}.{pure_code}"
        
        # 纯数字代码
        pure_code = code.zfill(6)
        if pure_code.startswith(('60', '68', '90')):
            return f"sh.{pure_code}"
        else:
            return f"sz.{pure_code}"
    
    # ==================== 股票基础信息 ====================
    
    async def get_stock_basic(
        self,
        ts_code: Optional[str] = None,
        list_status: str = "L",
    ) -> List[StockBasicRecord]:
        """
        获取股票列表
        
        Returns:
            List[StockBasicRecord]: 标准化的股票基础信息列表
        """
        if not await self._login():
            return []
        
        try:
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(None, self._bs.query_stock_basic)
            
            if rs.error_code != '0':
                self.logger.error(f"BaoStock query failed: {rs.error_msg}")
                return []
            
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return []
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 只保留股票 (type=1)
            df = df[df['type'] == '1']
            
            # 获取行业信息
            industry_map = await self._get_industry_map()
            
            records: List[StockBasicRecord] = []
            for _, row in df.iterrows():
                bs_code = str(row.get('code', ''))
                symbol = re.sub(r'^(sh|sz)\.', '', bs_code)
                ts_code_val = self._bs_code_to_ts_code(bs_code)
                industry = industry_map.get(bs_code, "") if industry_map else ""
                
                records.append({
                    "ts_code": ts_code_val,
                    "symbol": symbol,
                    "name": str(row.get('code_name', '')),
                    "area": "",
                    "industry": industry,
                    "market": "主板",
                    "list_date": "",
                    "list_status": "L",
                })
            
            self.logger.info(f"BaoStock got {len(records)} stocks")
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to get stock list: {e}")
            return []
    
    async def _get_industry_map(self) -> Dict[str, str]:
        """获取行业映射"""
        if not self._logged_in:
            return {}
        
        try:
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(None, self._bs.query_stock_industry)
            
            if rs.error_code != '0':
                return {}
            
            industry_list = []
            while rs.error_code == '0' and rs.next():
                industry_list.append(rs.get_row_data())
            
            if not industry_list:
                return {}
            
            df = pd.DataFrame(industry_list, columns=rs.fields)
            
            # 清理行业名称 (去掉编码前缀)
            def clean_industry(name):
                if not name or pd.isna(name):
                    return ''
                return re.sub(r'^[A-Z]\d+', '', str(name)).strip()
            
            df['industry_clean'] = df['industry'].apply(clean_industry)
            
            return dict(zip(df['code'], df['industry_clean']))
        except Exception:
            return {}
    
    def _bs_code_to_ts_code(self, bs_code: str) -> str:
        """将 BaoStock 代码转换为 ts_code"""
        if not bs_code:
            return ""
        
        parts = bs_code.split('.')
        if len(parts) != 2:
            return bs_code
        
        exchange, code = parts
        return f"{code}.{exchange.upper()}"
    
    # ==================== 日线数据 ====================
    
    async def get_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: str = "qfq",
    ) -> List[DailyRecord]:
        """
        获取日线行情
        
        Returns:
            List[DailyRecord]: 标准化的日线行情列表
        """
        if not ts_code or not await self._login():
            return []
        
        try:
            bs_code = self._convert_code(ts_code)
            
            # 日期格式转换
            if start_date:
                start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            else:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            if end_date:
                end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            else:
                end_date = datetime.now().strftime("%Y-%m-%d")
            
            # 复权标志
            adj_flag = "2" if adj == "qfq" else "1" if adj == "hfq" else "3"
            
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(
                None,
                lambda: self._bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag=adj_flag,
                )
            )
            
            if rs.error_code != '0':
                self.logger.error(f"BaoStock query failed: {rs.error_msg}")
                return []
            
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return []
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            records: List[DailyRecord] = []
            for _, row in df.iterrows():
                close_val = self._safe_float(row.get('close'))
                pre_close = self._safe_float(row.get('preclose'))
                change = None
                if close_val and pre_close:
                    change = close_val - pre_close
                
                records.append({
                    "ts_code": ts_code,
                    "trade_date": str(row.get('date', '')).replace('-', ''),
                    "open": self._safe_float(row.get('open')),
                    "high": self._safe_float(row.get('high')),
                    "low": self._safe_float(row.get('low')),
                    "close": close_val,
                    "pre_close": pre_close,
                    "change": change,
                    "pct_chg": self._safe_float(row.get('pctChg')),
                    "vol": self._safe_float(row.get('volume')),
                    "amount": self._safe_float(row.get('amount')),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get daily: {e}")
            return []
    
    async def get_daily_basic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[DailyBasicRecord]:
        """
        获取每日指标 (PE/PB)
        
        Returns:
            List[DailyBasicRecord]: 标准化的每日指标列表
        """
        if not trade_date or not await self._login():
            return []
        
        # 如果指定了单只股票
        if ts_code:
            return await self._get_single_stock_valuation(ts_code, trade_date)
        
        # 批量获取太慢，返回空
        self.logger.warning("BaoStock daily_basic batch query is slow, skipping")
        return []
    
    async def _get_single_stock_valuation(
        self,
        ts_code: str,
        trade_date: str,
    ) -> List[DailyBasicRecord]:
        """获取单只股票估值数据"""
        try:
            bs_code = self._convert_code(ts_code)
            formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(
                None,
                lambda: self._bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                    start_date=formatted_date,
                    end_date=formatted_date,
                    frequency="d",
                    adjustflag="3",
                )
            )
            
            if rs.error_code != '0':
                return []
            
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return []
            
            row = data_list[0]
            
            return [{
                "ts_code": ts_code,
                "trade_date": trade_date,
                "close": self._safe_float(row[2]) if len(row) > 2 else None,
                "turnover_rate": None,
                "turnover_rate_f": None,
                "volume_ratio": None,
                "pe": None,
                "pe_ttm": self._safe_float(row[3]) if len(row) > 3 else None,
                "pb": self._safe_float(row[4]) if len(row) > 4 else None,
                "ps": None,
                "ps_ttm": self._safe_float(row[5]) if len(row) > 5 else None,
                "dv_ratio": None,
                "dv_ttm": None,
                "total_share": None,
                "float_share": None,
                "total_mv": None,  # BaoStock 不提供
                "circ_mv": None,
            }]
        except Exception as e:
            self.logger.error(f"Failed to get valuation for {ts_code}: {e}")
            return []
    
    # ==================== 实时行情 ====================
    
    async def get_realtime_quotes(
        self,
        ts_codes: Optional[List[str]] = None,
    ) -> Dict[str, RealtimeQuoteRecord]:
        """BaoStock 不支持实时行情"""
        return {}
    
    # ==================== K线 ====================
    
    async def get_kline(
        self,
        code: str,
        period: str = "day",
        limit: int = 120,
        adj: Optional[str] = None,
    ) -> List[KlineRecord]:
        """
        获取K线数据
        
        Returns:
            List[KlineRecord]: 标准化的K线数据列表
        """
        if not await self._login():
            return []
        
        try:
            bs_code = self._convert_code(code)
            
            # 周期映射
            freq_map = {
                "day": "d",
                "week": "w",
                "month": "m",
                "5m": "5",
                "15m": "15",
                "30m": "30",
                "60m": "60",
            }
            frequency = freq_map.get(period, "d")
            
            # 复权标志
            adj_flag = "2" if adj == "qfq" else "1" if adj == "hfq" else "3"
            
            # 计算日期范围
            end_date = datetime.now().strftime("%Y-%m-%d")
            if period in ("day", "week", "month"):
                start_date = (datetime.now() - timedelta(days=limit * 7)).strftime("%Y-%m-%d")
            else:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(
                None,
                lambda: self._bs.query_history_k_data_plus(
                    bs_code,
                    "date,time,open,high,low,close,volume,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    adjustflag=adj_flag,
                )
            )
            
            if rs.error_code != '0':
                return []
            
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return []
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df = df.tail(limit)
            
            items: List[KlineRecord] = []
            for _, row in df.iterrows():
                time_str = row.get('date', '')
                if 'time' in df.columns and row.get('time'):
                    time_str = f"{time_str} {row.get('time')}"
                
                items.append({
                    "time": time_str,
                    "open": self._safe_float(row.get('open')),
                    "high": self._safe_float(row.get('high')),
                    "low": self._safe_float(row.get('low')),
                    "close": self._safe_float(row.get('close')),
                    "volume": self._safe_float(row.get('volume')),
                    "amount": self._safe_float(row.get('amount')),
                })
            
            return items
        except Exception as e:
            self.logger.error(f"Failed to get kline: {e}")
            return []
    
    # ==================== 交易日历 ====================
    
    async def get_trade_calendar(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """获取交易日列表"""
        if not await self._login():
            return []
        
        try:
            # BaoStock 使用 query_trade_dates
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(
                None,
                lambda: self._bs.query_trade_dates(
                    start_date=f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}",
                    end_date=f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}",
                )
            )
            
            if rs.error_code != '0':
                return []
            
            dates = []
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                if len(row) >= 2 and row[1] == '1':  # is_trading_day
                    dates.append(row[0].replace('-', ''))
            
            return sorted(dates)
        except Exception as e:
            self.logger.error(f"Failed to get trade calendar: {e}")
            return []
    
    async def get_latest_trade_date(self) -> Optional[str]:
        """获取最近交易日"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        return yesterday
    
    # ==================== 指数 ====================
    
    async def get_index_daily(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[IndexDailyRecord]:
        """
        获取指数日线
        
        Returns:
            List[IndexDailyRecord]: 标准化的指数日线列表
        """
        if not await self._login():
            return []
        
        try:
            bs_code = self._convert_code(ts_code)
            
            if start_date:
                start_date_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            else:
                start_date_fmt = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            if end_date:
                end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            else:
                end_date_fmt = datetime.now().strftime("%Y-%m-%d")
            
            loop = asyncio.get_event_loop()
            rs = await loop.run_in_executor(
                None,
                lambda: self._bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                    frequency="d",
                )
            )
            
            if rs.error_code != '0':
                return []
            
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return []
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            records: List[IndexDailyRecord] = []
            for _, row in df.iterrows():
                close_val = self._safe_float(row.get('close'))
                pre_close = self._safe_float(row.get('preclose'))
                change = None
                if close_val and pre_close:
                    change = close_val - pre_close
                
                records.append({
                    "ts_code": ts_code,
                    "trade_date": str(row.get('date', '')).replace('-', ''),
                    "open": self._safe_float(row.get('open')),
                    "high": self._safe_float(row.get('high')),
                    "low": self._safe_float(row.get('low')),
                    "close": close_val,
                    "pre_close": pre_close,
                    "change": change,
                    "pct_chg": self._safe_float(row.get('pctChg')),
                    "vol": self._safe_float(row.get('volume')),
                    "amount": self._safe_float(row.get('amount')),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get index daily: {e}")
            return []
