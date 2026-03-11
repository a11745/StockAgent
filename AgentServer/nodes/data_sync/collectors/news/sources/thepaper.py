"""
澎湃新闻采集器

采集内容:
- 热榜 (综合热门新闻)
- 财经新闻
- 编辑推荐
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class ThePaperSource(BaseSource):
    """
    澎湃新闻采集器
    
    API: https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar
    
    该 API 返回多个列表:
    - hotNews: 热门新闻
    - financialInformationNews: 财经资讯
    - editorHandpicked: 编辑推荐
    """
    
    name = NewsSource.THEPAPER
    display_name = "澎湃新闻"
    default_category = NewsCategory.GENERAL
    
    SIDEBAR_URL = "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar"
    DETAIL_URL = "https://cache.thepaper.cn/contentapi/wwwDetail/contDetail"
    
    async def _fetch_detail(self, cont_id: str, trace_id: Optional[str] = None) -> str:
        """获取文章详情内容 (直接抓取网页)"""
        try:
            page_url = f"https://www.thepaper.cn/newsDetail_forward_{cont_id}"
            resp = await self.client.get(page_url, timeout=10)
            
            if resp.status_code != 200:
                return ""
            
            html = resp.text
            
            # 直接提取所有 <p> 标签内容 (最可靠)
            p_tags = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
            
            # 过滤并合并有效段落
            paragraphs = []
            for p in p_tags:
                # 清理 HTML 标签
                text = re.sub(r'<[^>]+>', '', p)
                text = re.sub(r'&[a-zA-Z]+;', ' ', text)
                text = re.sub(r'\s+', ' ', text).strip()
                # 只保留有实际内容的段落 (长度>10)
                if text and len(text) > 10:
                    paragraphs.append(text)
            
            content = ' '.join(paragraphs)
            return content[:3000] if content else ""
            
        except Exception:
            return ""
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取所有类型"""
        items = []
        
        try:
            data = await self.fetch_json(self.SIDEBAR_URL)
            
            if not data:
                self.logger.warning(f"[{trace_id}] ThePaper API returned None")
                return items
            
            # resultCode 可能是 int 或 str
            result_code = data.get("resultCode")
            if result_code != 1 and str(result_code) != "1":
                self.logger.warning(f"[{trace_id}] ThePaper API failed: resultCode={result_code}")
                return items
            
            inner_data = data.get("data", {})
            if not isinstance(inner_data, dict):
                self.logger.warning(f"[{trace_id}] ThePaper data is not dict: {type(inner_data)}")
                return items
            
            # 1. 热榜
            hot_news = inner_data.get("hotNews", [])
            self.logger.debug(f"[{trace_id}] ThePaper hotNews count: {len(hot_news)}")
            hot_items = await self._parse_news_list(hot_news, NewsCategory.GENERAL, "热榜", 20, trace_id)
            items.extend(hot_items)
            
            # 2. 财经资讯
            finance_news = inner_data.get("financialInformationNews", [])
            self.logger.debug(f"[{trace_id}] ThePaper finance count: {len(finance_news)}")
            finance_items = await self._parse_news_list(finance_news, NewsCategory.FINANCE_ARTICLE, "财经", 10, trace_id)
            items.extend(finance_items)
            
            # 3. 编辑推荐
            editor_news = inner_data.get("editorHandpicked", [])
            self.logger.debug(f"[{trace_id}] ThePaper editor count: {len(editor_news)}")
            editor_items = await self._parse_news_list(editor_news, NewsCategory.GENERAL, "推荐", 10, trace_id)
            items.extend(editor_items)
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch error: {e}", exc_info=True)
        
        if since:
            items = [item for item in items if item.publish_time and item.publish_time > since]
        
        # 去重
        seen = set()
        unique = []
        for item in items:
            if item.content_hash not in seen:
                seen.add(item.content_hash)
                unique.append(item)
        
        self.logger.info(f"[{trace_id}] ThePaper fetched {len(unique)} items")
        return unique[:limit]
    
    async def _parse_news_list(
        self,
        news_list: List[Dict[str, Any]],
        category: NewsCategory,
        tag: str,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """解析新闻列表"""
        items = []
        
        if not news_list or not isinstance(news_list, list):
            return items
        
        for item in news_list[:limit]:
            try:
                if not isinstance(item, dict):
                    continue
                
                title = item.get("name", "")
                cont_id = item.get("contId")
                
                if not title or not cont_id:
                    continue
                
                # 解析时间 - 支持多种格式
                publish_time = None
                
                # 优先使用 pubTimeLong (毫秒时间戳)
                pub_time_long = item.get("pubTimeLong")
                if pub_time_long:
                    try:
                        ts = int(pub_time_long)
                        if ts > 10000000000:
                            ts = ts / 1000
                        publish_time = datetime.fromtimestamp(ts)
                    except (ValueError, TypeError, OSError):
                        pass
                
                # 如果没有时间戳，尝试解析字符串
                if not publish_time:
                    pub_time = item.get("pubTime", "") or item.get("pubTimeNew", "")
                    if pub_time and not any(x in pub_time for x in ["小时前", "分钟前", "刚刚", "昨天"]):
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
                            try:
                                publish_time = datetime.strptime(pub_time, fmt)
                                break
                            except ValueError:
                                continue
                
                # 获取摘要/简介
                summary = item.get("summary", "") or item.get("intro", "") or item.get("smallcontent", "")
                url = f"https://www.thepaper.cn/newsDetail_forward_{cont_id}"
                
                # 如果摘要不足，获取详情页内容
                if not summary or len(summary) < 50:
                    try:
                        detail_content = await self._fetch_detail(cont_id, trace_id)
                        if detail_content:
                            summary = detail_content
                    except Exception as e:
                        self.logger.debug(f"[{trace_id}] Fetch detail error for {cont_id}: {e}")
                
                news_item = self.create_news_item(
                    title=title,
                    content=summary,
                    url=url,
                    publish_time=publish_time,
                    category=category,
                    source_id=str(cont_id),
                    tags=[tag],
                )
                items.append(news_item)
            
            except Exception as e:
                self.logger.debug(f"Parse item error: {e}")
                continue
        
        return items
