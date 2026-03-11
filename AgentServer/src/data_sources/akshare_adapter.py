"""
AKShare 数据源适配器

提供 AKShare 的异步封装，免费数据源，主要用于：
- 实时行情
- K线数据
- 新闻公告
- 股票列表

返回数据格式遵循 base.py 中定义的标准 TypedDict 结构。
"""
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import pandas as pd

from .base import (
    AsyncDataSourceAdapter,
    DataSourceType,
    DataSourceCapability,
    StockBasicRecord,
    DailyRecord,
    DailyBasicRecord,
    RealtimeQuoteRecord,
    MoneyflowRecord,
    LimitListRecord,
    KlineRecord,
    NewsRecord,
)


class AKShareAdapter(AsyncDataSourceAdapter):
    """
    AKShare 数据源适配器
    
    特点:
    - 完全免费
    - 实时行情较好 (东方财富/新浪)
    - 新闻采集能力强
    - 财务数据支持有限
    """
    
    def __init__(self):
        super().__init__()
        self._ak = None
    
    # ==================== 元信息 ====================
    
    @property
    def name(self) -> str:
        return "akshare"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.AKSHARE
    
    @property
    def capability(self) -> DataSourceCapability:
        return DataSourceCapability(
            stock_basic=True,
            daily_quotes=True,
            daily_basic=True,      # 支持但较慢
            realtime_quotes=True,
            financial_data=False,  # 不支持完整财务
            money_flow=True,
            limit_data=True,
            index_data=True,
            trade_calendar=True,
            news=True,
            kline=True,
        )
    
    def _get_default_priority(self) -> int:
        return 50  # 中等优先级
    
    # ==================== 生命周期 ====================
    
    async def initialize(self) -> None:
        """初始化"""
        if self._initialized:
            return
        
        try:
            import akshare as ak
            self._ak = ak
            self._initialized = True
            self.logger.info("AKShare adapter initialized ✓")
        except ImportError:
            self.logger.error("AKShare not installed. Run: pip install akshare")
    
    async def shutdown(self) -> None:
        """关闭"""
        self._ak = None
        self._initialized = False
        self.logger.info("AKShare adapter shutdown")
    
    async def is_available(self) -> bool:
        """检查是否可用"""
        return self._initialized and self._ak is not None
    
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
        if not await self.is_available():
            return []
        
        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                self._ak.stock_info_a_code_name
            )
            
            if df is None or df.empty:
                return []
            
            # 标准化列名
            df = df.rename(columns={
                'code': 'symbol',
                '代码': 'symbol',
                'name': 'name',
                '名称': 'name'
            })
            
            if 'symbol' not in df.columns or 'name' not in df.columns:
                self.logger.error(f"Unexpected columns: {df.columns.tolist()}")
                return []
            
            records: List[StockBasicRecord] = []
            for _, row in df.iterrows():
                symbol = str(row.get("symbol", "")).zfill(6)
                records.append({
                    "ts_code": self._normalize_ts_code(symbol),
                    "symbol": symbol,
                    "name": str(row.get("name", "")),
                    "area": "",
                    "industry": "",
                    "market": self._get_market(symbol),
                    "list_date": "",
                    "list_status": "L",
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get stock list: {e}")
            return []
    
    def _get_market(self, code: str) -> str:
        """根据代码判断市场"""
        code = str(code).zfill(6)
        if code.startswith('000'):
            return '主板'
        elif code.startswith('002'):
            return '中小板'
        elif code.startswith('300'):
            return '创业板'
        elif code.startswith('60'):
            return '主板'
        elif code.startswith('688'):
            return '科创板'
        elif code.startswith('8'):
            return '北交所'
        return '未知'
    
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
        if not await self.is_available() or not ts_code:
            return []
        
        try:
            code = self._extract_code(ts_code)
            
            period_map = {"qfq": "qfq", "hfq": "hfq", None: ""}
            adjust = period_map.get(adj, "qfq")
            
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
            )
            
            if df is None or df.empty:
                return []
            
            records: List[DailyRecord] = []
            for _, row in df.iterrows():
                close_val = self._safe_float(row.get('收盘') or row.get('close'))
                pre_close = self._safe_float(row.get('昨收') or row.get('pre_close'))
                change = None
                pct_chg = self._safe_float(row.get('涨跌幅') or row.get('pct_chg'))
                
                if close_val and pre_close:
                    change = close_val - pre_close
                
                records.append({
                    "ts_code": ts_code,
                    "trade_date": str(row.get('日期') or row.get('date', '')).replace('-', ''),
                    "open": self._safe_float(row.get('开盘') or row.get('open')),
                    "high": self._safe_float(row.get('最高') or row.get('high')),
                    "low": self._safe_float(row.get('最低') or row.get('low')),
                    "close": close_val,
                    "pre_close": pre_close,
                    "change": change,
                    "pct_chg": pct_chg,
                    "vol": self._safe_float(row.get('成交量') or row.get('volume')),
                    "amount": self._safe_float(row.get('成交额') or row.get('amount')),
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
        获取每日指标
        
        注意: AKShare 获取每日指标较慢，需要逐个股票查询
        
        Returns:
            List[DailyBasicRecord]: 标准化的每日指标列表
        """
        if not await self.is_available():
            return []
        
        # 如果指定了单只股票
        if ts_code:
            return await self._get_single_stock_basic(ts_code, trade_date)
        
        # 批量获取太慢，返回空
        self.logger.warning("AKShare daily_basic batch query not supported")
        return []
    
    async def _get_single_stock_basic(
        self,
        ts_code: str,
        trade_date: Optional[str] = None,
    ) -> List[DailyBasicRecord]:
        """获取单只股票的基本面数据"""
        try:
            code = self._extract_code(ts_code)
            
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None,
                lambda: self._ak.stock_individual_info_em(symbol=code)
            )
            
            if info is None or info.empty:
                return []
            
            info_dict = {}
            for _, row in info.iterrows():
                item = row.get('item', '')
                value = row.get('value', '')
                info_dict[item] = value
            
            # 总市值单位可能是万元，转换为亿元
            total_mv_raw = self._safe_float(info_dict.get('总市值', 0))
            # AKShare 的市值单位需要确认，这里假设是万元
            total_mv_yi = total_mv_raw / 10000 if total_mv_raw and total_mv_raw > 1000 else total_mv_raw
            
            circ_mv_raw = self._safe_float(info_dict.get('流通市值', 0))
            circ_mv_yi = circ_mv_raw / 10000 if circ_mv_raw and circ_mv_raw > 1000 else circ_mv_raw
            
            return [{
                "ts_code": ts_code,
                "trade_date": trade_date or datetime.now().strftime("%Y%m%d"),
                "close": self._safe_float(info_dict.get('最新', 0)),
                "turnover_rate": self._safe_float(info_dict.get('换手率')),
                "turnover_rate_f": None,
                "volume_ratio": self._safe_float(info_dict.get('量比')),
                "pe": self._safe_float(info_dict.get('市盈率-动态')),
                "pe_ttm": self._safe_float(info_dict.get('市盈率-TTM')),
                "pb": self._safe_float(info_dict.get('市净率')),
                "ps": None,
                "ps_ttm": None,
                "dv_ratio": None,
                "dv_ttm": None,
                "total_share": None,
                "float_share": None,
                "total_mv": total_mv_yi,
                "circ_mv": circ_mv_yi,
            }]
        except Exception as e:
            self.logger.error(f"Failed to get stock basic for {ts_code}: {e}")
            return []
    
    # ==================== 实时行情 ====================
    
    async def get_realtime_quotes(
        self,
        ts_codes: Optional[List[str]] = None,
        source: str = "eastmoney",
    ) -> Dict[str, RealtimeQuoteRecord]:
        """
        获取全市场实时行情
        
        Args:
            ts_codes: 不使用，获取全市场
            source: "eastmoney" 或 "sina"
        
        Returns:
            Dict[code, RealtimeQuoteRecord]: 以6位代码为key的实时行情字典
        """
        if not await self.is_available():
            return {}
        
        try:
            loop = asyncio.get_event_loop()
            
            if source == "sina":
                df = await loop.run_in_executor(None, self._ak.stock_zh_a_spot)
            else:
                df = await loop.run_in_executor(None, self._ak.stock_zh_a_spot_em)
            
            if df is None or df.empty:
                return {}
            
            # 列名兼容
            code_col = next((c for c in ["代码", "code", "symbol", "股票代码"] if c in df.columns), None)
            price_col = next((c for c in ["最新价", "现价", "price", "最新", "trade"] if c in df.columns), None)
            pct_col = next((c for c in ["涨跌幅", "涨跌幅(%)", "pct_chg"] if c in df.columns), None)
            amount_col = next((c for c in ["成交额", "amount"] if c in df.columns), None)
            open_col = next((c for c in ["今开", "开盘", "open"] if c in df.columns), None)
            high_col = next((c for c in ["最高", "high"] if c in df.columns), None)
            low_col = next((c for c in ["最低", "low"] if c in df.columns), None)
            pre_close_col = next((c for c in ["昨收", "pre_close"] if c in df.columns), None)
            volume_col = next((c for c in ["成交量", "volume", "vol"] if c in df.columns), None)
            
            if not code_col or not price_col:
                self.logger.error(f"Missing required columns: {df.columns.tolist()}")
                return {}
            
            result: Dict[str, RealtimeQuoteRecord] = {}
            
            for _, row in df.iterrows():
                code_raw = row.get(code_col)
                if not code_raw:
                    continue
                
                code_str = str(code_raw).strip()
                
                # 提取纯数字代码
                if len(code_str) > 6:
                    code_str = ''.join(filter(str.isdigit, code_str))
                
                if code_str.isdigit():
                    code = code_str.zfill(6)
                else:
                    code_digits = ''.join(filter(str.isdigit, code_str))
                    if not code_digits:
                        continue
                    code = code_digits.zfill(6)
                
                result[code] = {
                    "ts_code": self._normalize_ts_code(code),
                    "close": self._safe_float(row.get(price_col)),
                    "open": self._safe_float(row.get(open_col)) if open_col else None,
                    "high": self._safe_float(row.get(high_col)) if high_col else None,
                    "low": self._safe_float(row.get(low_col)) if low_col else None,
                    "pre_close": self._safe_float(row.get(pre_close_col)) if pre_close_col else None,
                    "pct_chg": self._safe_float(row.get(pct_col)) if pct_col else None,
                    "vol": self._safe_float(row.get(volume_col)) if volume_col else None,
                    "amount": self._safe_float(row.get(amount_col)) if amount_col else None,
                }
            
            self.logger.info(f"Got {len(result)} realtime quotes from {source}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get realtime quotes: {e}")
            return {}
    
    # ==================== 新闻 ====================
    
    async def get_stock_news(
        self,
        symbol: str,
        limit: int = 20,
    ) -> List[NewsRecord]:
        """
        获取个股新闻
        
        Returns:
            List[NewsRecord]: 标准化的新闻列表
        """
        if not await self.is_available():
            return []
        
        try:
            code = self._extract_code(symbol)
            
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ak.stock_news_em(symbol=code)
            )
            
            if df is None or df.empty:
                return []
            
            df = df.head(limit)
            
            news_list: List[NewsRecord] = []
            for _, row in df.iterrows():
                news_list.append({
                    "ts_code": symbol,
                    "title": str(row.get("新闻标题", "")),
                    "content": str(row.get("新闻内容", ""))[:500],
                    "datetime": self._parse_datetime(row.get("发布时间", "")),
                    "url": str(row.get("新闻链接", "")),
                    "source": "东方财富",
                    "type": "news",
                })
            
            return news_list
        except Exception as e:
            self.logger.error(f"Failed to get news for {symbol}: {e}")
            return []
    
    async def get_news(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
    ) -> List[NewsRecord]:
        """
        获取新闻
        
        Returns:
            List[NewsRecord]: 标准化的新闻列表
        """
        if ts_code:
            return await self.get_stock_news(ts_code, limit=limit)
        return []
    
    def _parse_datetime(self, dt_str: str) -> str:
        """解析日期时间"""
        if not dt_str:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
            try:
                return datetime.strptime(str(dt_str), fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        
        return str(dt_str)
    
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
        if not await self.is_available():
            return []
        
        try:
            code6 = self._extract_code(code).zfill(6)
            items: List[KlineRecord] = []
            
            if period in ("day", "week", "month"):
                period_map = {"day": "daily", "week": "weekly", "month": "monthly"}
                adjust_map = {None: "", "qfq": "qfq", "hfq": "hfq"}
                
                loop = asyncio.get_event_loop()
                df = await loop.run_in_executor(
                    None,
                    lambda: self._ak.stock_zh_a_hist(
                        symbol=code6,
                        period=period_map[period],
                        adjust=adjust_map.get(adj, ""),
                    )
                )
                
                if df is None or df.empty:
                    return []
                
                df = df.tail(limit)
                
                for _, row in df.iterrows():
                    items.append({
                        "time": str(row.get('日期') or row.get('date', '')),
                        "open": self._safe_float(row.get('开盘') or row.get('open')),
                        "high": self._safe_float(row.get('最高') or row.get('high')),
                        "low": self._safe_float(row.get('最低') or row.get('low')),
                        "close": self._safe_float(row.get('收盘') or row.get('close')),
                        "volume": self._safe_float(row.get('成交量') or row.get('volume')),
                        "amount": self._safe_float(row.get('成交额') or row.get('amount')),
                    })
            else:
                # 分钟线
                per_map = {"5m": "5", "15m": "15", "30m": "30", "60m": "60"}
                if period not in per_map:
                    return []
                
                loop = asyncio.get_event_loop()
                df = await loop.run_in_executor(
                    None,
                    lambda: self._ak.stock_zh_a_minute(
                        symbol=code6,
                        period=per_map[period],
                        adjust=adj if adj in ("qfq", "hfq") else "",
                    )
                )
                
                if df is None or df.empty:
                    return []
                
                df = df.tail(limit)
                
                for _, row in df.iterrows():
                    items.append({
                        "time": str(row.get('时间') or row.get('day', '')),
                        "open": self._safe_float(row.get('开盘') or row.get('open')),
                        "high": self._safe_float(row.get('最高') or row.get('high')),
                        "low": self._safe_float(row.get('最低') or row.get('low')),
                        "close": self._safe_float(row.get('收盘') or row.get('close')),
                        "volume": self._safe_float(row.get('成交量') or row.get('volume')),
                        "amount": self._safe_float(row.get('成交额') or row.get('amount')),
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
        if not await self.is_available():
            return []
        
        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ak.tool_trade_date_hist_sina()
            )
            
            if df is None or df.empty:
                return []
            
            # 转换为 YYYYMMDD 格式并过滤
            dates = []
            for _, row in df.iterrows():
                d = row.get('trade_date')
                if d:
                    d_str = str(d).replace('-', '')[:8]
                    if start_date <= d_str <= end_date:
                        dates.append(d_str)
            
            return sorted(dates)
        except Exception as e:
            self.logger.error(f"Failed to get trade calendar: {e}")
            return []
    
    async def get_latest_trade_date(self) -> Optional[str]:
        """获取最近交易日"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        return yesterday
    
    # ==================== 指数 ====================
    
    async def get_realtime_index_quotes(self) -> Dict[str, RealtimeQuoteRecord]:
        """
        获取主要指数实时行情
        
        Returns:
            Dict[ts_code, RealtimeQuoteRecord]: 以指数代码为key的实时行情字典
        """
        if not await self.is_available():
            return {}
        
        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                self._ak.stock_zh_index_spot_em
            )
            
            if df is None or df.empty:
                return {}
            
            result: Dict[str, RealtimeQuoteRecord] = {}
            
            # 只取三大指数
            target_codes = {
                "000001": "000001.SH",  # 上证指数
                "399001": "399001.SZ",  # 深证成指
                "399006": "399006.SZ",  # 创业板指
            }
            
            code_col = next((c for c in ["代码", "code"] if c in df.columns), None)
            price_col = next((c for c in ["最新价", "current"] if c in df.columns), None)
            pct_col = next((c for c in ["涨跌幅", "pct_chg"] if c in df.columns), None)
            open_col = next((c for c in ["今开", "open"] if c in df.columns), None)
            high_col = next((c for c in ["最高", "high"] if c in df.columns), None)
            low_col = next((c for c in ["最低", "low"] if c in df.columns), None)
            pre_close_col = next((c for c in ["昨收", "pre_close"] if c in df.columns), None)
            vol_col = next((c for c in ["成交量", "volume"] if c in df.columns), None)
            amount_col = next((c for c in ["成交额", "amount"] if c in df.columns), None)
            
            if not code_col or not price_col:
                return {}
            
            for _, row in df.iterrows():
                code = str(row.get(code_col, ""))
                if code in target_codes:
                    ts_code = target_codes[code]
                    result[ts_code] = {
                        "ts_code": ts_code,
                        "close": self._safe_float(row.get(price_col)),
                        "open": self._safe_float(row.get(open_col)) if open_col else None,
                        "high": self._safe_float(row.get(high_col)) if high_col else None,
                        "low": self._safe_float(row.get(low_col)) if low_col else None,
                        "pre_close": self._safe_float(row.get(pre_close_col)) if pre_close_col else None,
                        "pct_chg": self._safe_float(row.get(pct_col)) if pct_col else None,
                        "vol": self._safe_float(row.get(vol_col)) if vol_col else None,
                        "amount": self._safe_float(row.get(amount_col)) if amount_col else None,
                    }
            
            return result
        except Exception as e:
            self.logger.error(f"Failed to get index quotes: {e}")
            return {}
    
    # ==================== 资金流向 ====================
    
    async def get_moneyflow_industry(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[MoneyflowRecord]:
        """
        获取行业资金流向
        
        Returns:
            List[MoneyflowRecord]: 标准化的行业资金流向列表
        """
        if not await self.is_available():
            return []
        
        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                self._ak.stock_sector_fund_flow_rank
            )
            
            if df is None or df.empty:
                return []
            
            the_trade_date = trade_date or datetime.now().strftime("%Y%m%d")
            
            records: List[MoneyflowRecord] = []
            for _, row in df.iterrows():
                name = str(row.get("名称", "") or row.get("name", ""))
                # 用行业名称作为 ts_code（稳定且唯一）
                ts_code = name if name else "UNKNOWN"
                
                records.append({
                    "ts_code": ts_code,
                    "trade_date": the_trade_date,
                    "name": name,
                    "pct_change": self._safe_float(row.get("涨跌幅") or row.get("pct_change")),
                    "close": self._safe_float(row.get("收盘价") or row.get("最新价")),
                    "net_amount": self._safe_float(row.get("今日主力净流入-净额") or row.get("net_amount")),
                    "net_amount_rate": self._safe_float(row.get("今日主力净流入-净占比")),
                    "buy_elg_amount": self._safe_float(row.get("今日特大单净流入-净额")),
                    "buy_lg_amount": self._safe_float(row.get("今日大单净流入-净额")),
                    "buy_md_amount": self._safe_float(row.get("今日中单净流入-净额")),
                    "buy_sm_amount": self._safe_float(row.get("今日小单净流入-净额")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get moneyflow industry: {e}")
            return []
    
    # ==================== 涨跌停 ====================
    
    async def get_limit_list(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit_type: Optional[str] = None,
    ) -> List[LimitListRecord]:
        """
        获取涨跌停列表
        
        Returns:
            List[LimitListRecord]: 标准化的涨跌停列表
        """
        if not await self.is_available():
            return []
        
        try:
            loop = asyncio.get_event_loop()
            the_trade_date = trade_date or datetime.now().strftime("%Y%m%d")
            
            # 获取涨停
            if limit_type is None or limit_type == "U":
                df_up = await loop.run_in_executor(
                    None,
                    lambda: self._ak.stock_zt_pool_em(date=trade_date) if trade_date else self._ak.stock_zt_pool_em()
                )
            else:
                df_up = pd.DataFrame()
            
            # 获取跌停
            if limit_type is None or limit_type == "D":
                df_down = await loop.run_in_executor(
                    None,
                    lambda: self._ak.stock_zt_pool_dtgc_em(date=trade_date) if trade_date else self._ak.stock_zt_pool_dtgc_em()
                )
            else:
                df_down = pd.DataFrame()
            
            records: List[LimitListRecord] = []
            
            if df_up is not None and not df_up.empty:
                for _, row in df_up.iterrows():
                    code = str(row.get("代码", "")).zfill(6)
                    records.append({
                        "ts_code": self._normalize_ts_code(code),
                        "trade_date": the_trade_date,
                        "name": str(row.get("名称", "")),
                        "industry": str(row.get("所属行业", "") or ""),
                        "close": self._safe_float(row.get("最新价")),
                        "pct_chg": self._safe_float(row.get("涨跌幅")),
                        "amount": self._safe_float(row.get("成交额")),
                        "limit_amount": self._safe_float(row.get("封板金额")),
                        "float_mv": self._safe_float(row.get("流通市值")),
                        "total_mv": self._safe_float(row.get("总市值")),
                        "turnover_ratio": self._safe_float(row.get("换手率")),
                        "fd_amount": self._safe_float(row.get("封单金额")),
                        "first_time": str(row.get("首次封板时间", "") or ""),
                        "last_time": str(row.get("最后封板时间", "") or ""),
                        "open_times": int(row.get("炸板次数", 0) or 0),
                        "up_stat": str(row.get("涨停统计", "") or ""),
                        "limit_times": int(row.get("连板数", 0) or 0),
                        "limit": "U",
                    })
            
            if df_down is not None and not df_down.empty:
                for _, row in df_down.iterrows():
                    code = str(row.get("代码", "")).zfill(6)
                    records.append({
                        "ts_code": self._normalize_ts_code(code),
                        "trade_date": the_trade_date,
                        "name": str(row.get("名称", "")),
                        "industry": str(row.get("所属行业", "") or ""),
                        "close": self._safe_float(row.get("最新价")),
                        "pct_chg": self._safe_float(row.get("涨跌幅")),
                        "amount": self._safe_float(row.get("成交额")),
                        "limit_amount": None,
                        "float_mv": self._safe_float(row.get("流通市值")),
                        "total_mv": self._safe_float(row.get("总市值")),
                        "turnover_ratio": self._safe_float(row.get("换手率")),
                        "fd_amount": None,
                        "first_time": "",
                        "last_time": "",
                        "open_times": 0,
                        "up_stat": "",
                        "limit_times": 0,
                        "limit": "D",
                    })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get limit list: {e}")
            return []
