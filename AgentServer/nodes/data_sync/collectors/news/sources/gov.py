"""
国务院采集器

采集内容:
- 政策文库 (重要政策文件)
- 政策解读
"""

import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class GovSource(BaseSource):
    """
    国务院采集器
    
    API: https://sousuo.www.gov.cn/search-gov/data
    
    使用搜索 API 获取政策数据
    """
    
    name = NewsSource.GOV
    display_name = "国务院"
    default_category = NewsCategory.POLICY_RELEASE
    
    SEARCH_URL = "https://sousuo.www.gov.cn/search-gov/data"
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取政策"""
        items = []
        
        # 政策文库
        policy_items = await self._fetch_policy_library(limit=30, trace_id=trace_id)
        items.extend(policy_items)
        
        # 注意: 政策类新闻不使用 since 过滤
        # 原因: 政策更新频率低，去重引擎会处理重复内容
        
        self.logger.info(f"[{trace_id}] Gov fetched {len(items)} items")
        return items[:limit]
    
    async def _fetch_policy_library(
        self,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取政策文库"""
        items = []
        
        try:
            params = {
                "t": "zhengcelibrary_gw",
                "sort": "publishDate",
                "sortType": "1",
                "pageSize": str(limit),
                "pageNum": "0",
            }
            
            # 直接使用 client 请求，而非 fetch_json
            resp = await self.client.get(self.SEARCH_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            if not data or not isinstance(data, dict):
                self.logger.warning(f"[{trace_id}] Gov API returned invalid data")
                return items
            
            search_vo = data.get("searchVO")
            if not search_vo or not isinstance(search_vo, dict):
                self.logger.warning(f"[{trace_id}] Gov searchVO is empty or invalid")
                return items
            
            list_vo = search_vo.get("listVO")
            if not list_vo or not isinstance(list_vo, list):
                self.logger.warning(f"[{trace_id}] Gov listVO is empty or invalid")
                return items
            
            self.logger.debug(f"[{trace_id}] Gov listVO has {len(list_vo)} items")
            
            for item in list_vo[:limit]:
                try:
                    if not isinstance(item, dict):
                        continue
                    
                    title = item.get("title", "")
                    url = item.get("url", "")
                    
                    if not title or not url:
                        continue
                    
                    # 清理标题中的 HTML
                    title = self._clean_text(title)
                    
                    # 解析日期 - 支持多种格式
                    publish_time = None
                    
                    # 优先使用时间戳
                    ptime = item.get("ptime") or item.get("pubtime")
                    if ptime:
                        try:
                            ts = int(ptime)
                            if ts > 10000000000:
                                ts = ts / 1000
                            publish_time = datetime.fromtimestamp(ts)
                        except (ValueError, TypeError, OSError):
                            pass
                    
                    # 如果没有时间戳，尝试字符串
                    if not publish_time:
                        pub_time_str = item.get("pubtimeStr", "")
                        if pub_time_str:
                            for fmt in ["%Y.%m.%d", "%Y-%m-%d", "%Y年%m月%d日"]:
                                try:
                                    publish_time = datetime.strptime(pub_time_str, fmt)
                                    break
                                except ValueError:
                                    continue
                    
                    # 判断类型
                    category = NewsCategory.POLICY_RELEASE
                    tags = ["政策", "国务院"]
                    
                    child_type = item.get("childtype", "")
                    if "解读" in title or "解读" in str(child_type):
                        category = NewsCategory.POLICY_INTERPRET
                        tags.append("解读")
                    elif "通知" in title or "公告" in title:
                        category = NewsCategory.POLICY_NOTICE
                        tags.append("通知")
                    
                    # 获取摘要
                    summary = item.get("summary", "")
                    
                    # 如果摘要为空或太短，尝试获取详情页
                    content = self._clean_text(summary)
                    if len(content) < 50 and url:
                        detail_content = await self._fetch_detail(url, trace_id)
                        if detail_content:
                            content = detail_content
                    
                    news_item = self.create_news_item(
                        title=title,
                        content=content,
                        url=url,
                        publish_time=publish_time,
                        category=category,
                        source_id=str(item.get("id", "")),
                        tags=tags,
                    )
                    items.append(news_item)
                
                except Exception as e:
                    self.logger.debug(f"[{trace_id}] Parse item error: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch policy library error: {e}", exc_info=True)
        
        return items
    
    async def _fetch_detail(self, url: str, trace_id: Optional[str] = None) -> Optional[str]:
        """获取详情页内容 (静默处理错误)"""
        try:
            resp = await self.client.get(url, timeout=10)
            
            # 非 200 静默返回
            if resp.status_code != 200:
                return None
            
            html = resp.text
            
            # 尝试提取正文
            patterns = [
                r'<div[^>]*class="[^"]*pages_content[^"]*"[^>]*>(.*?)</div>',
                r'<div[^>]*class="[^"]*article[^"]*"[^>]*>(.*?)</div>',
                r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                r'<div[^>]*id="[^"]*UCAP-CONTENT[^"]*"[^>]*>(.*?)</div>',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
                if match:
                    content = match.group(1)
                    content = self._clean_text(content)
                    if len(content) > 50:
                        return content[:2000]
            
            # 后备：提取所有 <p> 标签
            p_tags = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
            if p_tags:
                content = ' '.join(p_tags)
                content = self._clean_text(content)
                if len(content) > 50:
                    return content[:2000]
            
            return None
        except Exception as e:
            self.logger.debug(f"[{trace_id}] Fetch detail error: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
