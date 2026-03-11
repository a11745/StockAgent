"""
华尔街见闻采集器

采集内容:
- 快讯 (实时滚动)
- 文章 (深度内容)
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class WallstreetcnSource(BaseSource):
    """
    华尔街见闻采集器
    
    API:
    - 快讯: https://api-one.wallstcn.com/apiv1/content/lives
    - 文章: https://api-one.wallstcn.com/apiv1/content/articles
    """
    
    name = NewsSource.WALLSTREETCN
    display_name = "华尔街见闻"
    default_category = NewsCategory.FINANCE_FLASH
    
    LIVES_URL = "https://api-one.wallstcn.com/apiv1/content/lives"
    ARTICLES_URL = "https://api-one.wallstcn.com/apiv1/content/articles"
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取所有类型"""
        items = []
        
        # 快讯
        flash_items = await self.fetch_lives(limit=50, trace_id=trace_id)
        items.extend(flash_items)
        
        # 文章
        article_items = await self.fetch_articles(limit=30, trace_id=trace_id)
        items.extend(article_items)
        
        if since:
            items = [item for item in items if item.publish_time and item.publish_time > since]
        
        # 去重
        seen = set()
        unique = []
        for item in items:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique.append(item)
        
        self.logger.info(f"[{trace_id}] Wallstreetcn fetched {len(unique)} items")
        return unique[:limit]
    
    async def fetch_lives(
        self,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取快讯"""
        items = []
        
        try:
            params = {"channel": "global-channel", "limit": limit}
            data = await self.fetch_json(self.LIVES_URL, params=params)
            
            if not data:
                return items
            
            for item in data.get("data", {}).get("items", [])[:limit]:
                title = item.get("title") or item.get("content_text", "")
                content = item.get("content_text", "")
                
                if not title:
                    continue
                
                display_time = item.get("display_time", 0)
                publish_time = datetime.fromtimestamp(display_time) if display_time else None
                
                news_item = self.create_news_item(
                    title=self._clean_html(title)[:200],
                    content=self._clean_html(content),
                    url=item.get("uri", ""),
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=str(item.get("id", "")),
                    tags=["快讯"],
                )
                items.append(news_item)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch lives error: {e}")
        
        return items
    
    async def fetch_articles(
        self,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取文章"""
        items = []
        
        try:
            params = {"limit": limit}
            data = await self.fetch_json(self.ARTICLES_URL, params=params)
            
            if not data:
                return items
            
            for item in data.get("data", {}).get("items", [])[:limit]:
                title = item.get("title", "")
                content = item.get("content_short", "") or item.get("content", "")
                
                if not title:
                    continue
                
                display_time = item.get("display_time", 0)
                publish_time = datetime.fromtimestamp(display_time) if display_time else None
                
                news_item = self.create_news_item(
                    title=title,
                    content=self._clean_html(content),
                    summary=item.get("content_short", ""),
                    url=f"https://wallstreetcn.com/articles/{item.get('id', '')}",
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_ARTICLE,
                    source_id=str(item.get("id", "")),
                    author=item.get("author", {}).get("display_name", ""),
                    tags=["文章"],
                )
                items.append(news_item)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch articles error: {e}")
        
        return items
    
    def _clean_html(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
