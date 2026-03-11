"""
稀土掘金采集器

采集内容:
- 技术热门文章 (推荐流)
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class JuejinSource(BaseSource):
    """
    稀土掘金采集器
    
    API: https://api.juejin.cn/recommend_api/v1/article/recommend_cate_feed
    
    使用推荐流 API 获取带完整内容的文章
    """
    
    name = NewsSource.JUEJIN
    display_name = "稀土掘金"
    default_category = NewsCategory.INDUSTRY_NEWS
    
    FEED_URL = "https://api.juejin.cn/recommend_api/v1/article/recommend_cate_feed"
    
    # 分类 ID
    CATEGORIES = {
        "backend": "6809637767543259144",   # 后端
        "frontend": "6809637767543259143",  # 前端
        "ai": "6809637773935378440",        # 人工智能
    }
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取推荐文章"""
        items = []
        
        try:
            # 抓取多个分类
            for cat_name, cat_id in self.CATEGORIES.items():
                cat_items = await self._fetch_category(cat_id, cat_name, limit=20, trace_id=trace_id)
                items.extend(cat_items)
            
            # 去重
            seen = set()
            unique = []
            for item in items:
                if item.source_id not in seen:
                    seen.add(item.source_id)
                    unique.append(item)
            items = unique
            
            if since:
                items = [item for item in items if item.publish_time and item.publish_time > since]
            
            self.logger.info(f"[{trace_id}] Juejin fetched {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch error: {e}")
        
        return items[:limit]
    
    async def _fetch_category(
        self,
        cate_id: str,
        cat_name: str,
        limit: int = 20,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取指定分类"""
        items = []
        
        try:
            # 使用 POST 请求
            payload = {
                "id_type": 2,
                "sort_type": 200,  # 热门
                "cate_id": cate_id,
                "cursor": "0",
                "limit": limit,
            }
            
            resp = await self.client.post(self.FEED_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("err_no") != 0:
                self.logger.warning(f"[{trace_id}] Juejin API error: {data.get('err_msg')}")
                return items
            
            data_list = data.get("data")
            if not data_list or not isinstance(data_list, list):
                self.logger.debug(f"[{trace_id}] Juejin {cat_name} returned empty data")
                return items
            
            for item in data_list[:limit]:
                if not item or not isinstance(item, dict):
                    continue
                
                article = item.get("article_info")
                if not article or not isinstance(article, dict):
                    continue
                
                title = article.get("title", "")
                article_id = article.get("article_id", "")
                
                if not title or not article_id:
                    continue
                
                # 解析时间
                ctime = article.get("ctime", 0)
                publish_time = None
                if ctime:
                    try:
                        ts = int(ctime)
                        if ts > 10000000000:
                            ts = ts / 1000
                        publish_time = datetime.fromtimestamp(ts)
                    except (ValueError, TypeError):
                        pass
                
                brief = article.get("brief_content", "")
                
                # 获取作者
                author_info = item.get("author_user_info", {})
                author = author_info.get("user_name", "")
                
                news_item = self.create_news_item(
                    title=title,
                    content=brief,
                    url=f"https://juejin.cn/post/{article_id}",
                    publish_time=publish_time,
                    category=NewsCategory.INDUSTRY_NEWS,
                    source_id=str(article_id),
                    author=author,
                    tags=["科技", cat_name],
                )
                items.append(news_item)
        
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch category {cat_name} error: {e}")
        
        return items
