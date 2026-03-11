"""
数据源管理器

统一管理多个数据源适配器，提供自动降级和优先级调度。

使用方式:
    from core.managers import data_source_manager
    
    # 初始化 (在应用启动时调用)
    await data_source_manager.initialize()
    
    # 获取数据 (自动选择可用的数据源)
    stocks, source = await data_source_manager.get_stock_basic()
    daily, source = await data_source_manager.get_daily(ts_code="000001.SZ")
    
    # 关闭
    await data_source_manager.shutdown()
"""

from typing import Optional, List, Dict, Any, Tuple, Type
import logging

from core.base import BaseManager
from core.settings import settings
from src.data_sources import (
    AsyncDataSourceAdapter,
    TushareAdapter,
    AKShareAdapter,
    BaoStockAdapter,
)


logger = logging.getLogger(__name__)


class DataSourceManager(BaseManager):
    """
    数据源管理器
    
    职责:
    - 管理多个数据源适配器
    - 根据优先级自动选择数据源
    - 当主数据源不可用时自动降级
    - 对 Node 层提供统一的数据获取接口
    """
    
    def __init__(self):
        super().__init__()
        self._adapters: List[AsyncDataSourceAdapter] = []
        self._adapter_map: Dict[str, AsyncDataSourceAdapter] = {}
    
    async def initialize(self) -> None:
        """初始化所有数据源适配器"""
        if self._initialized:
            return
        
        self.logger.info("Initializing DataSourceManager...")
        
        # 检查 Tushare 是否配置
        tushare_configured = False
        try:
            tushare_configured = settings.tushare.is_configured
        except Exception:
            pass
        
        # 按优先级添加适配器
        adapter_classes: List[Tuple[Type[AsyncDataSourceAdapter], dict]] = []
        
        if tushare_configured:
            token = settings.tushare.token.get_secret_value()
            adapter_classes.append((TushareAdapter, {"token": token}))
            self.logger.info("Tushare token configured, adding TushareAdapter")
        else:
            self.logger.warning("Tushare token not configured, using free data sources only")
        
        # 免费数据源始终添加
        adapter_classes.append((AKShareAdapter, {}))
        adapter_classes.append((BaoStockAdapter, {}))
        
        # 初始化所有适配器
        for adapter_cls, kwargs in adapter_classes:
            try:
                adapter = adapter_cls(**kwargs)
                await adapter.initialize()
                
                if await adapter.is_available():
                    self._adapters.append(adapter)
                    self._adapter_map[adapter.name] = adapter
                    self.logger.info(f"Adapter {adapter.name} initialized (priority: {adapter.priority})")
                else:
                    self.logger.warning(f"Adapter {adapter.name} not available")
                    await adapter.shutdown()
            except Exception as e:
                self.logger.error(f"Failed to initialize {adapter_cls.__name__}: {e}")
        
        # 按优先级排序 (高优先级在前)
        self._adapters.sort(key=lambda x: x.priority, reverse=True)
        
        self._initialized = True
        self.logger.info(f"DataSourceManager initialized with {len(self._adapters)} adapters: {[a.name for a in self._adapters]}")
    
    async def shutdown(self) -> None:
        """关闭所有适配器"""
        for adapter in self._adapters:
            try:
                await adapter.shutdown()
            except Exception as e:
                self.logger.error(f"Error shutting down {adapter.name}: {e}")
        
        self._adapters.clear()
        self._adapter_map.clear()
        self._initialized = False
        self.logger.info("DataSourceManager shutdown")
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized or not self._adapters:
            return False
        
        # 至少有一个适配器可用
        for adapter in self._adapters:
            if await adapter.is_available():
                return True
        return False
    
    def get_available_adapters(self) -> List[str]:
        """获取可用的适配器名称列表"""
        return [a.name for a in self._adapters]
    
    def get_adapter(self, name: str) -> Optional[AsyncDataSourceAdapter]:
        """根据名称获取特定适配器"""
        return self._adapter_map.get(name)
    
    # ==================== 通用数据获取方法 ====================
    
    async def _get_with_fallback(
        self,
        method_name: str,
        preferred_source: Optional[str] = None,
        **kwargs,
    ) -> Tuple[Any, Optional[str]]:
        """
        通用的降级获取方法
        
        Args:
            method_name: 要调用的方法名
            preferred_source: 优先使用的数据源名称
            **kwargs: 传递给方法的参数
            
        Returns:
            (data, source_name) - 数据和数据源名称
        """
        adapters = list(self._adapters)
        
        # 如果指定了优先数据源，调整顺序
        if preferred_source and preferred_source in self._adapter_map:
            preferred = self._adapter_map[preferred_source]
            adapters = [preferred] + [a for a in adapters if a.name != preferred_source]
        
        for adapter in adapters:
            if not await adapter.is_available():
                continue
            
            method = getattr(adapter, method_name, None)
            if method is None:
                continue
            
            try:
                result = await method(**kwargs)
                
                # 检查结果是否有效
                if result is not None:
                    if isinstance(result, (list, dict)):
                        if len(result) > 0:
                            return result, adapter.name
                    else:
                        return result, adapter.name
            except Exception as e:
                self.logger.warning(f"{adapter.name}.{method_name} failed: {e}")
                continue
        
        return None, None
    
    # ==================== 股票基础信息 ====================
    
    async def get_stock_basic(
        self,
        ts_code: Optional[str] = None,
        list_status: str = "L",
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取股票基础信息"""
        return await self._get_with_fallback(
            "get_stock_basic",
            preferred_source=preferred_source,
            ts_code=ts_code,
            list_status=list_status,
        )
    
    # ==================== 日线数据 ====================
    
    async def get_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: str = "qfq",
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取日线行情"""
        return await self._get_with_fallback(
            "get_daily",
            preferred_source=preferred_source,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            adj=adj,
        )
    
    async def get_daily_basic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取每日指标"""
        return await self._get_with_fallback(
            "get_daily_basic",
            preferred_source=preferred_source,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
    
    # ==================== 实时行情 ====================
    
    async def get_realtime_quotes(
        self,
        ts_codes: Optional[List[str]] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
        """获取实时行情"""
        return await self._get_with_fallback(
            "get_realtime_quotes",
            preferred_source=preferred_source,
            ts_codes=ts_codes,
        )
    
    async def get_realtime_index_quotes(
        self,
        preferred_source: Optional[str] = None,
    ) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
        """获取指数实时行情"""
        return await self._get_with_fallback(
            "get_realtime_index_quotes",
            preferred_source=preferred_source,
        )
    
    # ==================== 财务数据 ====================
    
    async def get_financial_indicator(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取财务指标"""
        return await self._get_with_fallback(
            "get_financial_indicator",
            preferred_source=preferred_source,
            ts_code=ts_code,
            period=period,
            limit=limit,
        )
    
    async def get_financial_data(
        self,
        ts_code: str,
        limit: int = 4,
        preferred_source: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """获取完整财务数据"""
        return await self._get_with_fallback(
            "get_financial_data",
            preferred_source=preferred_source,
            ts_code=ts_code,
            limit=limit,
        )
    
    async def get_income_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取利润表"""
        return await self._get_with_fallback(
            "get_income_statement",
            preferred_source=preferred_source,
            ts_code=ts_code,
            period=period,
            limit=limit,
        )
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取资产负债表"""
        return await self._get_with_fallback(
            "get_balance_sheet",
            preferred_source=preferred_source,
            ts_code=ts_code,
            period=period,
            limit=limit,
        )
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取现金流量表"""
        return await self._get_with_fallback(
            "get_cashflow_statement",
            preferred_source=preferred_source,
            ts_code=ts_code,
            period=period,
            limit=limit,
        )
    
    # ==================== 资金流向 ====================
    
    async def get_moneyflow_industry(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取行业资金流向"""
        return await self._get_with_fallback(
            "get_moneyflow_industry",
            preferred_source=preferred_source,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
    
    async def get_moneyflow_concept(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取概念板块资金流向"""
        return await self._get_with_fallback(
            "get_moneyflow_concept",
            preferred_source=preferred_source,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
    
    async def get_moneyflow_hsgt(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取沪深港通资金流向"""
        return await self._get_with_fallback(
            "get_moneyflow_hsgt",
            preferred_source=preferred_source,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
    
    # ==================== 涨跌停 ====================
    
    async def get_limit_list(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit_type: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取涨跌停统计"""
        return await self._get_with_fallback(
            "get_limit_list",
            preferred_source=preferred_source,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            ts_code=ts_code,
            limit_type=limit_type,
        )
    
    async def get_stk_limit(
        self,
        trade_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取涨跌停价格"""
        return await self._get_with_fallback(
            "get_stk_limit",
            preferred_source=preferred_source,
            trade_date=trade_date,
        )
    
    # ==================== 指数 ====================
    
    async def get_index_basic(
        self,
        market: str = "SSE",
        ts_code: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取指数基础信息"""
        return await self._get_with_fallback(
            "get_index_basic",
            preferred_source=preferred_source,
            market=market,
            ts_code=ts_code,
        )
    
    async def get_index_daily(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取指数日线"""
        return await self._get_with_fallback(
            "get_index_daily",
            preferred_source=preferred_source,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
    
    # ==================== 交易日历 ====================
    
    async def get_trade_calendar(
        self,
        start_date: str,
        end_date: str,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[str], Optional[str]]:
        """获取交易日列表"""
        return await self._get_with_fallback(
            "get_trade_calendar",
            preferred_source=preferred_source,
            start_date=start_date,
            end_date=end_date,
        )
    
    async def get_latest_trade_date(
        self,
        preferred_source: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """获取最近交易日"""
        return await self._get_with_fallback(
            "get_latest_trade_date",
            preferred_source=preferred_source,
        )
    
    async def is_trading_time(self) -> bool:
        """检查当前是否为交易时间"""
        for adapter in self._adapters:
            if await adapter.is_available():
                return await adapter.is_trading_time()
        return False
    
    # ==================== 新闻 ====================
    
    async def get_news(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取新闻"""
        return await self._get_with_fallback(
            "get_news",
            preferred_source=preferred_source,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    
    async def get_stock_news(
        self,
        symbol: str,
        limit: int = 20,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取个股新闻"""
        return await self._get_with_fallback(
            "get_stock_news",
            preferred_source=preferred_source,
            symbol=symbol,
            limit=limit,
        )
    
    # ==================== K线 ====================
    
    async def get_kline(
        self,
        code: str,
        period: str = "day",
        limit: int = 120,
        adj: Optional[str] = None,
        preferred_source: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """获取K线数据"""
        return await self._get_with_fallback(
            "get_kline",
            preferred_source=preferred_source,
            code=code,
            period=period,
            limit=limit,
            adj=adj,
        )


# 全局单例
data_source_manager = DataSourceManager()
