"""
Tushare 数据源适配器

提供 Tushare Pro API 的异步封装，包含完整的财务数据、行情、资金流向等功能。
需要有效的 Tushare Token 才能使用。

返回数据格式遵循 base.py 中定义的标准 TypedDict 结构。
"""
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta

import pandas as pd

from .base import (
    AsyncDataSourceAdapter,
    DataSourceType,
    DataSourceCapability,
    TokenBucket,
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


class TushareAdapter(AsyncDataSourceAdapter):
    """
    Tushare Pro 数据源适配器
    
    特点:
    - 数据最全面 (财务、资金流向、涨跌停等)
    - 需要付费 Token
    - 有 API 调用频率限制
    """
    
    # 财务指标核心字段
    FINA_INDICATOR_FIELDS = ",".join([
        "ts_code", "ann_date", "end_date",
        "eps", "dt_eps", "bps", "cfps", "ocfps",
        "roe", "roe_waa", "roe_dt", "roa", "roic",
        "netprofit_margin", "grossprofit_margin", "profit_to_gr",
        "tr_yoy", "or_yoy", "netprofit_yoy", "dt_netprofit_yoy",
        "basic_eps_yoy", "roe_yoy", "bps_yoy", "assets_yoy",
        "assets_turn", "ca_turn", "fa_turn", "inv_turn", "ar_turn",
        "current_ratio", "quick_ratio", "cash_ratio",
        "debt_to_assets", "debt_to_eqt", "eqt_to_debt",
        "ocf_to_or", "ocf_to_profit", "fcff", "fcfe",
        "ebit", "ebitda", "op_income",
        "q_netprofit_yoy", "q_netprofit_qoq", "q_sales_yoy", "q_sales_qoq",
    ])
    
    # 三大核心指数
    CORE_INDEX_CODES = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
    ]
    
    def __init__(self, token: Optional[str] = None):
        """
        Args:
            token: Tushare Token，不传则从环境变量/配置读取
        """
        super().__init__()
        self._token = token
        self._ts = None      # tushare 模块
        self._pro = None     # tushare pro api
        self._bucket: Optional[TokenBucket] = None
    
    # ==================== 元信息 ====================
    
    @property
    def name(self) -> str:
        return "tushare"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.TUSHARE
    
    @property
    def capability(self) -> DataSourceCapability:
        return DataSourceCapability(
            stock_basic=True,
            daily_quotes=True,
            daily_basic=True,
            realtime_quotes=True,
            financial_data=True,
            money_flow=True,
            limit_data=True,
            index_data=True,
            trade_calendar=True,
            news=True,
            kline=True,
        )
    
    def _get_default_priority(self) -> int:
        return 100  # 最高优先级
    
    # ==================== 生命周期 ====================
    
    async def initialize(self) -> None:
        """初始化 Tushare 连接"""
        if self._initialized:
            return
        
        # 获取 token
        token = self._token
        if not token:
            try:
                from core.settings import settings
                if settings.tushare.is_configured:
                    token = settings.tushare.token.get_secret_value()
            except Exception:
                pass
        
        if not token:
            self.logger.warning("Tushare token not configured")
            return
        
        self.logger.info("Initializing Tushare adapter...")
        
        try:
            import tushare as ts
            ts.set_token(token)
            self._ts = ts
            self._pro = ts.pro_api()
            self._initialized = True
            self.logger.info("Tushare adapter initialized ✓")
        except Exception as e:
            self.logger.error(f"Failed to initialize Tushare: {e}")
    
    async def shutdown(self) -> None:
        """关闭适配器"""
        self._ts = None
        self._pro = None
        self._bucket = None
        self._initialized = False
        self.logger.info("Tushare adapter shutdown")
    
    async def is_available(self) -> bool:
        """检查是否可用"""
        return self._initialized and self._pro is not None
    
    async def _call_api(self, api_name: str, **kwargs) -> pd.DataFrame:
        """调用 Tushare API"""
        if not await self.is_available():
            raise RuntimeError("Tushare adapter not initialized")
        
        loop = asyncio.get_event_loop()
        api_func = getattr(self._pro, api_name)
        result = await loop.run_in_executor(None, lambda: api_func(**kwargs))
        return result
    
    # ==================== 股票基础信息 ====================
    
    async def get_stock_basic(
        self,
        ts_code: Optional[str] = None,
        list_status: str = "L",
    ) -> List[StockBasicRecord]:
        """
        获取股票基础信息
        
        Returns:
            List[StockBasicRecord]: 标准化的股票基础信息列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {"list_status": list_status}
            if ts_code:
                params["ts_code"] = ts_code
            
            df = await self._call_api("stock_basic", **params)
            if df.empty:
                return []
            
            records: List[StockBasicRecord] = []
            for _, row in df.iterrows():
                code = str(row.get("ts_code", ""))
                records.append({
                    "ts_code": code,
                    "symbol": self._extract_code(code),
                    "name": str(row.get("name", "")),
                    "area": str(row.get("area", "") or ""),
                    "industry": str(row.get("industry", "") or ""),
                    "market": str(row.get("market", "") or ""),
                    "list_date": str(row.get("list_date", "") or ""),
                    "list_status": str(row.get("list_status", "L")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get stock_basic: {e}")
            return []
    
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
        if not await self.is_available():
            return []
        
        try:
            params = {"adj": adj}
            if ts_code:
                params["ts_code"] = ts_code
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("daily", **params)
            if df.empty:
                return []
            
            records: List[DailyRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "open": self._safe_float(row.get("open")),
                    "high": self._safe_float(row.get("high")),
                    "low": self._safe_float(row.get("low")),
                    "close": self._safe_float(row.get("close")),
                    "pre_close": self._safe_float(row.get("pre_close")),
                    "change": self._safe_float(row.get("change")),
                    "pct_chg": self._safe_float(row.get("pct_chg")),
                    "vol": self._safe_float(row.get("vol")),
                    "amount": self._safe_float(row.get("amount")),
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
        获取每日指标 (PE/PB/换手率/市值等)
        
        注意: Tushare 原始市值单位是万元，这里转换为亿元
        
        Returns:
            List[DailyBasicRecord]: 标准化的每日指标列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {}
            if ts_code:
                params["ts_code"] = ts_code
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("daily_basic", **params)
            if df.empty:
                return []
            
            records: List[DailyBasicRecord] = []
            for _, row in df.iterrows():
                # Tushare 市值单位是万元，转换为亿元
                total_mv_wan = self._safe_float(row.get("total_mv"))
                circ_mv_wan = self._safe_float(row.get("circ_mv"))
                
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "close": self._safe_float(row.get("close")),
                    "turnover_rate": self._safe_float(row.get("turnover_rate")),
                    "turnover_rate_f": self._safe_float(row.get("turnover_rate_f")),
                    "volume_ratio": self._safe_float(row.get("volume_ratio")),
                    "pe": self._safe_float(row.get("pe")),
                    "pe_ttm": self._safe_float(row.get("pe_ttm")),
                    "pb": self._safe_float(row.get("pb")),
                    "ps": self._safe_float(row.get("ps")),
                    "ps_ttm": self._safe_float(row.get("ps_ttm")),
                    "dv_ratio": self._safe_float(row.get("dv_ratio")),
                    "dv_ttm": self._safe_float(row.get("dv_ttm")),
                    "total_share": self._safe_float(row.get("total_share")),
                    "float_share": self._safe_float(row.get("float_share")),
                    "total_mv": total_mv_wan / 10000 if total_mv_wan else None,
                    "circ_mv": circ_mv_wan / 10000 if circ_mv_wan else None,
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get daily_basic: {e}")
            return []
    
    # ==================== 实时行情 ====================
    
    async def get_realtime_quotes(
        self,
        ts_codes: Optional[List[str]] = None,
        batch_size: int = 50,
        timeout: float = 2.0,
    ) -> Dict[str, RealtimeQuoteRecord]:
        """
        获取实时行情
        
        Returns:
            Dict[code, RealtimeQuoteRecord]: 以6位代码为key的实时行情字典
        """
        if not await self.is_available() or self._ts is None:
            return {}
        
        if not ts_codes:
            return {}
        
        result: Dict[str, RealtimeQuoteRecord] = {}
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i + batch_size]
            
            try:
                ts_code_str = ",".join(batch_codes)
                loop = asyncio.get_event_loop()
                
                df = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        lambda code_str=ts_code_str: self._ts.realtime_quote(ts_code=code_str)
                    ),
                    timeout=timeout
                )
                
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        ts_code = row.get("TS_CODE") or row.get("ts_code", "")
                        if ts_code:
                            code6 = self._extract_code(ts_code)
                            result[code6] = {
                                "ts_code": ts_code,
                                "close": self._safe_float(row.get("PRICE") or row.get("price")),
                                "open": self._safe_float(row.get("OPEN") or row.get("open")),
                                "high": self._safe_float(row.get("HIGH") or row.get("high")),
                                "low": self._safe_float(row.get("LOW") or row.get("low")),
                                "pre_close": self._safe_float(row.get("PRE_CLOSE") or row.get("pre_close")),
                                "pct_chg": self._safe_float(row.get("PCT_CHANGE") or row.get("pct_change")),
                                "vol": self._safe_float(row.get("VOL") or row.get("vol")),
                                "amount": self._safe_float(row.get("AMOUNT") or row.get("amount")),
                            }
                
            except asyncio.TimeoutError:
                self.logger.warning(f"Realtime quote batch timeout")
            except Exception as e:
                self.logger.error(f"Failed to get realtime quote: {e}")
            
            if i + batch_size < len(ts_codes):
                await asyncio.sleep(0.1)
        
        return result
    
    async def get_realtime_index_quotes(self) -> Dict[str, RealtimeQuoteRecord]:
        """
        获取三大指数实时行情
        
        Returns:
            Dict[ts_code, RealtimeQuoteRecord]: 以指数代码为key的实时行情字典
        """
        if not await self.is_available() or self._ts is None:
            return {}
        
        result: Dict[str, RealtimeQuoteRecord] = {}
        
        try:
            ts_code_str = ",".join(self.CORE_INDEX_CODES)
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ts.realtime_quote(ts_code=ts_code_str)
            )
            
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    ts_code = row.get("TS_CODE") or row.get("ts_code", "")
                    if ts_code:
                        result[ts_code] = {
                            "ts_code": ts_code,
                            "close": self._safe_float(row.get("PRICE") or row.get("price")),
                            "open": self._safe_float(row.get("OPEN") or row.get("open")),
                            "high": self._safe_float(row.get("HIGH") or row.get("high")),
                            "low": self._safe_float(row.get("LOW") or row.get("low")),
                            "pre_close": self._safe_float(row.get("PRE_CLOSE") or row.get("pre_close")),
                            "pct_chg": self._safe_float(row.get("PCT_CHANGE") or row.get("pct_change")),
                            "vol": self._safe_float(row.get("VOL") or row.get("vol")),
                            "amount": self._safe_float(row.get("AMOUNT") or row.get("amount")),
                        }
        except Exception as e:
            self.logger.error(f"Failed to get realtime index quote: {e}")
        
        return result
    
    # ==================== 财务数据 ====================
    
    async def get_financial_indicator(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取财务指标"""
        if not await self.is_available() or not ts_code:
            return []
        
        try:
            params = {
                "ts_code": ts_code,
                "fields": self.FINA_INDICATOR_FIELDS,
            }
            if period:
                params["period"] = period
            
            df = await self._call_api("fina_indicator", **params)
            records = df.to_dict("records") if not df.empty else []
            
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get fina_indicator for {ts_code}: {e}")
            return []
    
    async def get_income_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取利润表"""
        if not await self.is_available() or not ts_code:
            return []
        
        try:
            params = {"ts_code": ts_code}
            if period:
                params["period"] = period
            
            df = await self._call_api("income", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get income for {ts_code}: {e}")
            return []
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取资产负债表"""
        if not await self.is_available() or not ts_code:
            return []
        
        try:
            params = {"ts_code": ts_code}
            if period:
                params["period"] = period
            
            df = await self._call_api("balancesheet", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get balancesheet for {ts_code}: {e}")
            return []
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """获取现金流量表"""
        if not await self.is_available() or not ts_code:
            return []
        
        try:
            params = {"ts_code": ts_code}
            if period:
                params["period"] = period
            
            df = await self._call_api("cashflow", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get cashflow for {ts_code}: {e}")
            return []
    
    async def get_financial_data(
        self,
        ts_code: str,
        limit: int = 4,
    ) -> Dict[str, Any]:
        """获取完整财务数据"""
        if not await self.is_available():
            return {}
        
        financial_data = {"ts_code": ts_code}
        
        results = await asyncio.gather(
            self.get_income_statement(ts_code, limit=limit),
            self.get_balance_sheet(ts_code, limit=limit),
            self.get_cashflow_statement(ts_code, limit=limit),
            self.get_financial_indicator(ts_code, limit=limit),
            return_exceptions=True
        )
        
        if isinstance(results[0], list):
            financial_data["income_statement"] = results[0]
        if isinstance(results[1], list):
            financial_data["balance_sheet"] = results[1]
        if isinstance(results[2], list):
            financial_data["cashflow_statement"] = results[2]
        if isinstance(results[3], list):
            financial_data["financial_indicators"] = results[3]
        
        return financial_data
    
    # ==================== 资金流向 ====================
    
    async def get_moneyflow_industry(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[MoneyflowRecord]:
        """
        获取行业资金流向 (东方财富)
        
        注: 使用 moneyflow_ind_dc 接口代替 moneyflow_ind_ths，因为 DC 接口提供完整的分档资金流向数据
        
        Returns:
            List[MoneyflowRecord]: 标准化的行业资金流向列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {"content_type": "行业"}
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("moneyflow_ind_dc", **params)
            if df.empty:
                return []
            
            records: List[MoneyflowRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "name": str(row.get("name", "")),
                    "pct_change": self._safe_float(row.get("pct_change")),
                    "close": self._safe_float(row.get("close")),
                    "net_amount": self._safe_float(row.get("net_amount")),
                    "net_amount_rate": self._safe_float(row.get("net_amount_rate")),
                    "buy_elg_amount": self._safe_float(row.get("buy_elg_amount")),
                    "buy_lg_amount": self._safe_float(row.get("buy_lg_amount")),
                    "buy_md_amount": self._safe_float(row.get("buy_md_amount")),
                    "buy_sm_amount": self._safe_float(row.get("buy_sm_amount")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get moneyflow_ind_dc (industry): {e}")
            return []
    
    async def get_moneyflow_concept(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[MoneyflowRecord]:
        """
        获取概念板块资金流向 (东方财富)
        
        注: 使用 moneyflow_ind_dc 接口代替 moneyflow_cnt_ths，因为 DC 接口提供完整的分档资金流向数据
        
        Returns:
            List[MoneyflowRecord]: 标准化的概念资金流向列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {"content_type": "概念"}
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("moneyflow_ind_dc", **params)
            if df.empty:
                return []
            
            records: List[MoneyflowRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "name": str(row.get("name", "")),
                    "pct_change": self._safe_float(row.get("pct_change")),
                    "close": self._safe_float(row.get("close")),
                    "net_amount": self._safe_float(row.get("net_amount")),
                    "net_amount_rate": self._safe_float(row.get("net_amount_rate")),
                    "buy_elg_amount": self._safe_float(row.get("buy_elg_amount")),
                    "buy_lg_amount": self._safe_float(row.get("buy_lg_amount")),
                    "buy_md_amount": self._safe_float(row.get("buy_md_amount")),
                    "buy_sm_amount": self._safe_float(row.get("buy_sm_amount")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get moneyflow_ind_dc (concept): {e}")
            return []
    
    async def get_moneyflow_hsgt(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取沪深港通资金流向"""
        if not await self.is_available():
            return []
        
        try:
            params = {}
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("moneyflow_hsgt", **params)
            return df.to_dict("records") if not df.empty else []
        except Exception as e:
            self.logger.error(f"Failed to get moneyflow_hsgt: {e}")
            return []
    
    # ==================== 涨跌停 ====================
    
    # limit_list_d 需要显式指定 fields 参数才能获取完整数据
    LIMIT_LIST_FIELDS = ",".join([
        "trade_date", "ts_code", "industry", "name",
        "close", "pct_chg", "amount", "limit_amount",
        "float_mv", "total_mv", "turnover_ratio",
        "fd_amount", "first_time", "last_time",
        "open_times", "up_stat", "limit_times", "limit"
    ])
    
    async def get_limit_list(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit_type: Optional[str] = None,
    ) -> List[LimitListRecord]:
        """
        获取涨跌停统计
        
        Returns:
            List[LimitListRecord]: 标准化的涨跌停列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {"fields": self.LIMIT_LIST_FIELDS}
            if trade_date:
                params["trade_date"] = trade_date
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if ts_code:
                params["ts_code"] = ts_code
            if limit_type:
                params["limit_type"] = limit_type
            
            df = await self._call_api("limit_list_d", **params)
            if df.empty:
                return []
            
            records: List[LimitListRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "name": str(row.get("name", "")),
                    "industry": str(row.get("industry", "") or ""),
                    "close": self._safe_float(row.get("close")),
                    "pct_chg": self._safe_float(row.get("pct_chg")),
                    "amount": self._safe_float(row.get("amount")),
                    "limit_amount": self._safe_float(row.get("limit_amount")),
                    "float_mv": self._safe_float(row.get("float_mv")),
                    "total_mv": self._safe_float(row.get("total_mv")),
                    "turnover_ratio": self._safe_float(row.get("turnover_ratio")),
                    "fd_amount": self._safe_float(row.get("fd_amount")),
                    "first_time": str(row.get("first_time", "") or ""),
                    "last_time": str(row.get("last_time", "") or ""),
                    "open_times": self._safe_int(row.get("open_times")),
                    "up_stat": str(row.get("up_stat", "") or ""),
                    "limit_times": self._safe_int(row.get("limit_times")),
                    "limit": str(row.get("limit", "U")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get limit_list_d: {e}")
            return []
    
    async def get_stk_limit(
        self,
        trade_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取涨跌停价格"""
        if not await self.is_available():
            return []
        
        if not trade_date:
            trade_date = date.today().strftime("%Y%m%d")
        
        try:
            df = await self._call_api("stk_limit", trade_date=trade_date)
            return df.to_dict("records") if not df.empty else []
        except Exception as e:
            self.logger.error(f"Failed to get stk_limit: {e}")
            return []
    
    # ==================== 指数 ====================
    
    async def get_index_basic(
        self,
        market: str = "SSE",
        ts_code: Optional[str] = None,
    ) -> List[IndexBasicRecord]:
        """
        获取指数基础信息
        
        Returns:
            List[IndexBasicRecord]: 标准化的指数基础信息列表
        """
        if not await self.is_available():
            return []
        
        try:
            params = {
                "market": market,
                "fields": "ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date",
            }
            if ts_code:
                params["ts_code"] = ts_code
            
            df = await self._call_api("index_basic", **params)
            if df.empty:
                return []
            
            records: List[IndexBasicRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "name": str(row.get("name", "")),
                    "fullname": str(row.get("fullname", "") or ""),
                    "market": str(row.get("market", "") or ""),
                    "publisher": str(row.get("publisher", "") or ""),
                    "index_type": str(row.get("index_type", "") or ""),
                    "category": str(row.get("category", "") or ""),
                    "base_date": str(row.get("base_date", "") or ""),
                    "base_point": self._safe_float(row.get("base_point")),
                    "list_date": str(row.get("list_date", "") or ""),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get index_basic: {e}")
            return []
    
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
        if not await self.is_available():
            return []
        
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("index_daily", **params)
            if df.empty:
                return []
            
            records: List[IndexDailyRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": str(row.get("ts_code", "")),
                    "trade_date": str(row.get("trade_date", "")),
                    "open": self._safe_float(row.get("open")),
                    "high": self._safe_float(row.get("high")),
                    "low": self._safe_float(row.get("low")),
                    "close": self._safe_float(row.get("close")),
                    "pre_close": self._safe_float(row.get("pre_close")),
                    "change": self._safe_float(row.get("change")),
                    "pct_chg": self._safe_float(row.get("pct_chg")),
                    "vol": self._safe_float(row.get("vol")),
                    "amount": self._safe_float(row.get("amount")),
                })
            
            return records
        except Exception as e:
            self.logger.error(f"Failed to get index_daily: {e}")
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
            df = await self._call_api(
                "trade_cal",
                start_date=start_date,
                end_date=end_date,
                is_open="1",
            )
            
            if df.empty:
                return []
            
            return df["cal_date"].tolist()
        except Exception as e:
            self.logger.error(f"Failed to get trade_cal: {e}")
            return []
    
    async def get_latest_trade_date(self) -> Optional[str]:
        """获取最近交易日"""
        if not await self.is_available():
            return None
        
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        
        # 18点之前用昨天
        if now.hour < 18:
            cutoff_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        else:
            cutoff_date = today
        
        start_date = (date.today() - timedelta(days=60)).strftime("%Y%m%d")
        
        try:
            df = await self._call_api(
                "trade_cal",
                exchange="SSE",
                start_date=start_date,
                end_date=cutoff_date,
                is_open="1",
            )
            
            if not df.empty:
                dates = sorted(df["cal_date"].tolist())
                valid_dates = [d for d in dates if d <= cutoff_date]
                if valid_dates:
                    return valid_dates[-1]
        except Exception as e:
            self.logger.error(f"Failed to get latest trade date: {e}")
        
        return cutoff_date
    
    async def is_trading_time(self) -> bool:
        """检查当前是否为交易时间"""
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        
        # 检查是否为交易日
        try:
            df = await self._call_api(
                "trade_cal",
                exchange="SSE",
                start_date=today,
                end_date=today,
            )
            
            if df.empty or df.iloc[0]["is_open"] != 1:
                return False
        except Exception:
            pass
        
        return await super().is_trading_time()
    
    # ==================== 新闻 ====================
    
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
        if not await self.is_available():
            return []
        
        try:
            params = {}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            
            df = await self._call_api("news", **params)
            if df.empty:
                return []
            
            records: List[NewsRecord] = []
            for _, row in df.iterrows():
                records.append({
                    "ts_code": ts_code or "",
                    "title": str(row.get("title", "")),
                    "content": str(row.get("content", "") or "")[:500],
                    "datetime": str(row.get("datetime", "")),
                    "url": str(row.get("url", "") or ""),
                    "source": str(row.get("source", "") or "tushare"),
                    "type": "news",
                })
            
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get news: {e}")
            return []
    
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
            from tushare.pro.data_pro import pro_bar
        except Exception:
            self.logger.error("Tushare pro_bar not available")
            return []
        
        try:
            ts_code = self._normalize_ts_code(code)
            
            freq_map = {
                "day": "D",
                "week": "W",
                "month": "M",
                "5m": "5min",
                "15m": "15min",
                "30m": "30min",
                "60m": "60min",
            }
            freq = freq_map.get(period, "D")
            adj_arg = adj if adj in (None, "qfq", "hfq") else None
            
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: pro_bar(
                    ts_code=ts_code,
                    api=self._pro,
                    freq=freq,
                    adj=adj_arg,
                    limit=limit,
                )
            )
            
            if df is None or df.empty:
                return []
            
            items: List[KlineRecord] = []
            tcol = 'trade_time' if 'trade_time' in df.columns else 'trade_date'
            df = df.sort_values(tcol)
            
            for _, row in df.iterrows():
                items.append({
                    "time": str(row.get(tcol)),
                    "open": self._safe_float(row.get('open')),
                    "high": self._safe_float(row.get('high')),
                    "low": self._safe_float(row.get('low')),
                    "close": self._safe_float(row.get('close')),
                    "volume": self._safe_float(row.get('vol')),
                    "amount": self._safe_float(row.get('amount')),
                })
            
            return items
        except Exception as e:
            self.logger.error(f"Failed to get kline: {e}")
            return []
