"""
东方财富采集器

采集内容:
- 财富号文章 (用户/机构发布)
- 快讯 (滚动新闻)
"""

import json
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class EastMoneySource(BaseSource):
    """
    东方财富采集器
    
    API:
    - 快讯: https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html
    - 财富号: https://caifuhao.eastmoney.com/hot
    """
    
    name = NewsSource.EASTMONEY
    display_name = "东方财富"
    default_category = NewsCategory.FINANCE_ARTICLE
    
    # 使用可用的替代 API
    FAST_NEWS_URL = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"
    CAIFUHAO_URL = "https://caifuhao.eastmoney.com/hot"
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取所有类型"""
        items = []
        
        # 快讯
        flash_items = await self.fetch_fast_news(limit=50, trace_id=trace_id)
        items.extend(flash_items)
        
        # 财富号热门
        hot_items = await self.fetch_caifuhao(limit=30, trace_id=trace_id)
        items.extend(hot_items)
        
        if since:
            items = [item for item in items if item.publish_time and item.publish_time > since]
        
        # 去重
        seen = set()
        unique = []
        for item in items:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique.append(item)
        
        self.logger.info(f"[{trace_id}] EastMoney fetched {len(unique)} items")
        return unique[:limit]
    
    async def fetch_fast_news(
        self,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取快讯"""
        items = []
        
        try:
            resp = await self.fetch_page(self.FAST_NEWS_URL)
            
            if not resp:
                return items
            
            text = resp.text
            
            # 解析 JSONP 格式: var ajaxResult={...}
            if text.startswith("var ajaxResult="):
                json_str = text[len("var ajaxResult="):]
                data = json.loads(json_str)
            else:
                data = json.loads(text)
            
            lives_list = data.get("LivesList", [])
            
            for item in lives_list[:limit]:
                # 从 newstext 或 digest 获取内容
                content = item.get("newstext", "") or item.get("digest", "")
                title = item.get("title", "") or item.get("simtitle", "")
                
                # 如果没有标题，从内容中提取
                if not title and content:
                    # 取内容前50个字符作为标题
                    title = content[:50].strip()
                    if len(content) > 50:
                        title += "..."
                
                if not title:
                    continue
                
                # 解析时间
                show_time = item.get("showtime", "")
                publish_time = None
                if show_time:
                    try:
                        publish_time = datetime.strptime(show_time, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
                
                # 获取 URL
                url = item.get("url_w", "") or item.get("url_m", "") or item.get("url_unique", "")
                
                news_item = self.create_news_item(
                    title=self._clean_text(title),
                    content=self._clean_text(content),
                    url=url,
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=str(item.get("id", "")),
                    tags=["快讯"],
                )
                items.append(news_item)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch fast news error: {e}")
        
        return items
    
    async def fetch_caifuhao(
        self,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取财富号热门文章"""
        items = []
        
        try:
            # 通过网页抓取财富号
            url = "https://caifuhao.eastmoney.com/hot"
            resp = await self.fetch_page(url)
            
            if not resp:
                return items
            
            html = resp.text
            
            # 匹配文章链接
            # 格式: /news/xxx
            patterns = [
                r'<a[^>]*href="(/news/\d+)"[^>]*>.*?<h3[^>]*>([^<]+)</h3>',
                r'href="(https://caifuhao\.eastmoney\.com/news/\d+)"[^>]*>([^<]+)</a>',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                
                for url_path, title in matches[:limit]:
                    title = title.strip()
                    
                    if not title or len(title) < 5:
                        continue
                    
                    if url_path.startswith("/"):
                        full_url = f"https://caifuhao.eastmoney.com{url_path}"
                    else:
                        full_url = url_path
                    
                    news_item = self.create_news_item(
                        title=self._clean_text(title),
                        url=full_url,
                        category=NewsCategory.FINANCE_ARTICLE,
                        tags=["财富号"],
                    )
                    items.append(news_item)
                    
                    if len(items) >= limit:
                        break
                
                if items:
                    break
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch caifuhao error: {e}")
        
        return items
    
    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
