"""
财联社采集器

采集内容:
1. 电报快讯 (实时滚动)
2. 头条文章 (深度内容)
3. 要闻 (重要新闻)
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class CLSSource(BaseSource):
    """
    财联社采集器
    
    API:
    - 电报: https://www.cls.cn/nodeapi/updateTelegraphList
    - 滚动: https://www.cls.cn/nodeapi/roll_news
    - 文章: https://www.cls.cn/nodeapi/content/home/heads
    
    Example:
        source = CLSSource()
        
        # 抓取所有类型
        items = await source.fetch()
        
        # 只抓取电报
        items = await source.fetch_telegraph()
        
        # 只抓取文章
        items = await source.fetch_articles()
    """
    
    name = NewsSource.CLS
    display_name = "财联社"
    default_category = NewsCategory.FINANCE_FLASH
    
    # API 地址
    TELEGRAPH_URL = "https://www.cls.cn/nodeapi/updateTelegraphList"
    ROLL_NEWS_URL = "https://www.cls.cn/nodeapi/roll_news"
    HEADS_URL = "https://www.cls.cn/nodeapi/content/home/heads"
    ARTICLE_DETAIL_URL = "https://www.cls.cn/nodeapi/content/detail"
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取所有类型的新闻
        
        注意: roll_news 和 heads API 已失效 (404)，仅使用 telegraph API
        """
        items = []
        
        # 抓取电报 (主要数据来源)
        telegraph_items = await self.fetch_telegraph(limit=limit, trace_id=trace_id)
        items.extend(telegraph_items)
        
        # 按时间过滤
        if since:
            items = [item for item in items if item.publish_time and item.publish_time > since]
        
        # 去重 (按 content_hash)
        seen = set()
        unique_items = []
        for item in items:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique_items.append(item)
        
        self.logger.info(f"[{trace_id}] CLS fetched {len(unique_items)} items")
        
        return unique_items[:limit]
    
    async def fetch_telegraph(
        self,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取电报快讯
        
        电报是财联社的实时滚动新闻，更新频率高。
        """
        items = []
        
        try:
            params = {
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
            }
            
            data = await self.fetch_json(self.TELEGRAPH_URL, params=params)
            
            if not data:
                return items
            
            roll_data = data.get("data", {}).get("roll_data", [])
            
            for item in roll_data[:limit]:
                if item.get("is_ad"):
                    continue
                
                title = item.get("title") or item.get("brief", "")
                content = item.get("content", "") or item.get("brief", "")
                
                if not title:
                    continue
                
                # 解析时间
                ctime = item.get("ctime", 0)
                publish_time = datetime.fromtimestamp(ctime) if ctime else None
                
                # 提取关联股票
                ts_codes = self._extract_stocks(item.get("subjects", []))
                
                # 判断重要性
                is_important = bool(item.get("important"))
                category = NewsCategory.FINANCE_FLASH
                
                news_item = self.create_news_item(
                    title=self._clean_html(title),
                    content=self._clean_html(content),
                    url=f"https://www.cls.cn/detail/{item.get('id', '')}",
                    publish_time=publish_time,
                    category=category,
                    source_id=str(item.get("id", "")),
                    ts_codes=ts_codes,
                    tags=["电报"] + (["重要"] if is_important else []),
                    extra={
                        "important": is_important,
                        "level": item.get("level", 0),
                    },
                )
                items.append(news_item)
            
            self.logger.debug(f"[{trace_id}] CLS telegraph: {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch telegraph error: {e}")
        
        return items
    
    async def fetch_roll_news(
        self,
        limit: int = 50,
        category: int = 0,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取滚动新闻
        
        滚动新闻比电报更详细，包含更多内容。
        
        Args:
            category: 分类 (0=全部, 1=A股, 2=港股, 3=美股, 4=外盘, 5=宏观)
        """
        items = []
        
        try:
            params = {
                "category": category,
                "limit": limit,
            }
            
            data = await self.fetch_json(self.ROLL_NEWS_URL, params=params)
            
            if not data:
                return items
            
            news_list = data.get("data", {}).get("roll_news_data", [])
            
            for item in news_list[:limit]:
                title = item.get("title", "")
                content = item.get("content", "") or item.get("brief", "")
                
                if not title:
                    continue
                
                ctime = item.get("ctime", 0)
                publish_time = datetime.fromtimestamp(ctime) if ctime else None
                
                ts_codes = self._extract_stocks(item.get("subjects", []))
                
                news_item = self.create_news_item(
                    title=self._clean_html(title),
                    content=self._clean_html(content),
                    url=f"https://www.cls.cn/detail/{item.get('id', '')}",
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=str(item.get("id", "")),
                    ts_codes=ts_codes,
                    tags=["滚动"],
                )
                items.append(news_item)
            
            self.logger.debug(f"[{trace_id}] CLS roll news: {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch roll news error: {e}")
        
        return items
    
    async def fetch_articles(
        self,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取头条文章
        
        头条是深度内容，包含完整的新闻分析。
        """
        items = []
        
        try:
            params = {
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
            }
            
            data = await self.fetch_json(self.HEADS_URL, params=params)
            
            if not data:
                return items
            
            articles = data.get("data", {}).get("top_article", [])
            articles += data.get("data", {}).get("depth_article", [])
            
            for item in articles[:limit]:
                article_id = item.get("id")
                title = item.get("title", "")
                
                if not title or not article_id:
                    continue
                
                # 获取文章详情
                detail = await self._fetch_article_detail(article_id, trace_id)
                
                content = ""
                if detail:
                    content = detail.get("content", "")
                
                ctime = item.get("ctime", 0)
                publish_time = datetime.fromtimestamp(ctime) if ctime else None
                
                ts_codes = self._extract_stocks(item.get("subjects", []))
                
                news_item = self.create_news_item(
                    title=self._clean_html(title),
                    content=self._clean_html(content),
                    summary=item.get("brief", ""),
                    url=f"https://www.cls.cn/detail/{article_id}",
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_ARTICLE,
                    source_id=str(article_id),
                    ts_codes=ts_codes,
                    tags=["头条", "深度"],
                    author=item.get("author", {}).get("name", ""),
                )
                items.append(news_item)
            
            self.logger.debug(f"[{trace_id}] CLS articles: {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch articles error: {e}")
        
        return items
    
    async def _fetch_article_detail(
        self,
        article_id: str,
        trace_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取文章详情"""
        try:
            params = {
                "id": article_id,
                "app": "CailianpressWeb",
                "os": "web",
            }
            
            data = await self.fetch_json(self.ARTICLE_DETAIL_URL, params=params)
            
            if data:
                return data.get("data", {})
            
        except Exception as e:
            self.logger.debug(f"[{trace_id}] Fetch article detail error: {e}")
        
        return None
    
    def _extract_stocks(self, subjects: List[Dict[str, Any]]) -> List[str]:
        """从 subjects 中提取股票代码"""
        ts_codes = []
        
        if not subjects:
            return ts_codes
        
        for subject in subjects:
            code = subject.get("code", "")
            if code:
                # 转换为统一格式
                if code.startswith("SH") or code.startswith("SZ"):
                    # 已经是 SH/SZ 格式，转为 ts_code 格式
                    ts_code = f"{code[2:]}.{code[:2]}"
                elif "." in code:
                    ts_code = code
                else:
                    # 猜测交易所
                    if code.startswith("6"):
                        ts_code = f"{code}.SH"
                    else:
                        ts_code = f"{code}.SZ"
                
                ts_codes.append(ts_code)
        
        return ts_codes
    
    def _clean_html(self, text: str) -> str:
        """清理 HTML 标签"""
        if not text:
            return ""
        # 去除 HTML 标签
        text = re.sub(r'<[^>]+>', '', text)
        # 去除多余空白
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
