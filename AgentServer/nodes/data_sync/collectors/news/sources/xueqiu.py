"""
雪球采集器

采集内容:
- 热股 (热门股票涨跌)
- 7x24 快讯 (实时新闻)
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class XueqiuSource(BaseSource):
    """
    雪球采集器
    
    API:
    - 热股: https://stock.xueqiu.com/v5/stock/hot_stock/list.json
    - 7x24快讯: https://xueqiu.com/statuses/livenews/list.json
    
    注意: 需要先访问首页和行情页获取 cookie/token
    """
    
    name = NewsSource.XUEQIU
    display_name = "雪球"
    default_category = NewsCategory.FINANCE_FLASH
    
    HOT_STOCK_URL = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
    LIVE_NEWS_URL = "https://xueqiu.com/statuses/livenews/list.json"
    
    def __init__(self):
        super().__init__()
        self._session_initialized = False
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取所有类型"""
        items = []
        
        # 初始化 session (获取必要的 cookie/token)
        await self._ensure_session()
        
        # 7x24 快讯
        news_items = await self.fetch_live_news(limit=30, trace_id=trace_id)
        items.extend(news_items)
        
        # 热股
        hot_items = await self.fetch_hot_stocks(limit=20, trace_id=trace_id)
        items.extend(hot_items)
        
        if since:
            items = [item for item in items if item.publish_time and item.publish_time > since]
        
        self.logger.info(f"[{trace_id}] Xueqiu fetched {len(items)} items")
        return items[:limit]
    
    async def _ensure_session(self):
        """确保 session 已初始化 (获取必要的 token)"""
        if self._session_initialized:
            return
        
        try:
            # 使用 HTML 请求头访问页面以获取完整 cookie
            html_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
            
            # 1. 访问首页获取基础 cookie
            await self.client.get("https://xueqiu.com/", headers=html_headers)
            
            # 2. 访问行情页获取完整 token (xq_a_token 等)
            await self.client.get("https://xueqiu.com/hq", headers=html_headers)
            
            self._session_initialized = True
            self.logger.debug("Xueqiu session initialized successfully")
        except Exception as e:
            self.logger.warning(f"Session init error: {e}")
    
    async def fetch_live_news(
        self,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取 7x24 快讯"""
        items = []
        
        try:
            params = {"count": limit}
            resp = await self.client.get(self.LIVE_NEWS_URL, params=params)
            resp.raise_for_status()
            
            data = resp.json()
            
            for item in data.get("items", [])[:limit]:
                text = item.get("text", "")
                
                if not text:
                    continue
                
                # 解析时间
                created_at = item.get("created_at", 0)
                publish_time = None
                if created_at:
                    publish_time = datetime.fromtimestamp(created_at / 1000)
                
                # 标题取前50个字符
                title = text[:50].strip()
                if len(text) > 50:
                    title += "..."
                
                news_item = self.create_news_item(
                    title=self._clean_text(title),
                    content=self._clean_text(text),
                    url=item.get("target", f"https://xueqiu.com/"),
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=str(item.get("id", "")),
                    tags=["7x24", "快讯"],
                )
                items.append(news_item)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch live news error: {e}")
        
        return items
    
    async def fetch_hot_stocks(
        self,
        limit: int = 20,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取热股"""
        items = []
        
        try:
            params = {"size": limit, "_type": "10", "type": "10"}
            headers = {
                "Referer": "https://xueqiu.com/hq",
                "Origin": "https://xueqiu.com",
            }
            resp = await self.client.get(self.HOT_STOCK_URL, params=params, headers=headers)
            resp.raise_for_status()
            
            data = resp.json()
            
            for item in data.get("data", {}).get("items", [])[:limit]:
                if item.get("ad"):
                    continue
                
                symbol = item.get("symbol", "")  # 雪球使用 symbol 而非 code
                name = item.get("name", "")
                percent = item.get("percent", 0)
                
                if not name:
                    continue
                
                # 构建标题
                title = f"{name} ({symbol}) 涨跌幅 {percent:+.2f}%"
                
                # 转换股票代码
                ts_code = self._convert_code(symbol)
                
                news_item = self.create_news_item(
                    title=title,
                    content=f"股票 {name} 当前涨跌幅 {percent:+.2f}%",
                    url=f"https://xueqiu.com/s/{symbol}",
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=symbol,
                    ts_codes=[ts_code] if ts_code else [],
                    tags=["热股"],
                    extra={
                        "percent": percent,
                        "symbol": symbol,
                    },
                )
                items.append(news_item)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch hot stocks error: {e}")
        
        return items
    
    def _convert_code(self, code: str) -> str:
        """转换股票代码"""
        if not code:
            return ""
        if code.startswith("SH"):
            return f"{code[2:]}.SH"
        elif code.startswith("SZ"):
            return f"{code[2:]}.SZ"
        return ""
    
    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
