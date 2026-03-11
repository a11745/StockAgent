"""
工信部采集器

采集内容:
1. 政策发布 - 重要政策文件
2. 政策解读 - 政策解释说明

使用 jpaas-publish-server API 获取数据
"""

import json
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class MIITSource(BaseSource):
    """
    工信部采集器
    
    从工信部官网抓取政策信息。
    
    API: https://www.miit.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit
    
    Example:
        source = MIITSource()
        items = await source.fetch()
    """
    
    name = NewsSource.MIIT
    display_name = "工信部"
    default_category = NewsCategory.POLICY_RELEASE
    
    BASE_URL = "https://www.miit.gov.cn"
    API_URL = "https://www.miit.gov.cn/api-gateway/jpaas-publish-server/front/page/build/unit"
    
    # API 参数配置 (从页面提取)
    CATEGORIES = {
        "zcjd": {
            "name": "政策解读",
            "category": NewsCategory.POLICY_INTERPRET,
            "params": {
                "parseType": "buildstatic",
                "webId": "8d828e408d90447786ddbe128d495e9e",
                "tplSetId": "209741b2109044b5b7695700b2bec37e",
                "pageType": "column",
                "tagId": "右侧内容",
                "editType": "null",
                "pageId": "1b56e5adc362428299dfc3eb444fe23a",
            },
        },
        "zcwj": {
            "name": "政策文件",
            "category": NewsCategory.POLICY_RELEASE,
            "params": {
                "parseType": "buildstatic",
                "webId": "8d828e408d90447786ddbe128d495e9e",
                "tplSetId": "209741b2109044b5b7695700b2bec37e",
                "pageType": "column",
                "tagId": "右侧内容",
                "editType": "null",
                "pageId": "wjfb",  # 需要找到实际的 pageId
            },
        },
    }
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取政策信息 (主要抓取政策解读)
        """
        items = []
        
        # 只抓取政策解读 (已验证可用)
        try:
            cat_items = await self.fetch_category(
                cat_id="zcjd",
                limit=30,
                trace_id=trace_id,
            )
            items.extend(cat_items)
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch zcjd error: {e}")
        
        # 注意: 政策类新闻不使用 since 过滤
        # 原因: 1. 政策发布日期只有日期没有时间，时区转换会导致问题
        #       2. 政策更新频率低，去重引擎会处理重复内容
        #       3. 政策发布时间可能延迟录入，实际采集时间与发布日期差异大
        
        self.logger.info(f"[{trace_id}] MIIT fetched {len(items)} items")
        
        return items[:limit]
    
    async def fetch_category(
        self,
        cat_id: str,
        limit: int = 30,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """
        抓取指定分类
        
        Args:
            cat_id: 分类ID (zcjd/zcwj)
            limit: 最多抓取数量
            trace_id: 追踪ID
        """
        items = []
        cat_info = self.CATEGORIES.get(cat_id)
        
        if not cat_info:
            return items
        
        try:
            params = cat_info.get("params", {})
            
            # 直接使用 client 请求
            resp = await self.client.get(self.API_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            if not isinstance(data, dict):
                self.logger.warning(f"[{trace_id}] MIIT API response is not dict")
                return items
            
            if not data.get("success"):
                self.logger.warning(f"[{trace_id}] MIIT API failed: {data.get('message')}")
                return items
            
            inner_data = data.get("data")
            if not inner_data or not isinstance(inner_data, dict):
                self.logger.warning(f"[{trace_id}] MIIT data is empty or invalid")
                return items
            
            html = inner_data.get("html", "")
            if not html:
                self.logger.warning(f"[{trace_id}] MIIT html is empty")
                return items
            
            self.logger.debug(f"[{trace_id}] MIIT html length: {len(html)}")
            
            # 解析 HTML 内容
            items = await self._parse_api_html(html, cat_info, limit, trace_id)
            
            self.logger.debug(f"[{trace_id}] MIIT {cat_id}: {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch category {cat_id} error: {e}", exc_info=True)
        
        return items
    
    async def _parse_api_html(
        self,
        html: str,
        cat_info: Dict[str, Any],
        limit: int,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """解析 API 返回的 HTML 内容"""
        items = []
        
        if not html:
            return items
        
        # 匹配文章链接和标题
        # 格式: <a ... href="/zwgk/zcjd/art/2026/art_xxx.html" ... title="标题">...<span>2026-03-03</span>
        pattern = r'<a[^>]*href="([^"]+)"[^>]*title="([^"]+)"[^>]*>.*?(\d{4}-\d{2}-\d{2})'
        matches = re.findall(pattern, html, re.DOTALL)
        
        self.logger.debug(f"MIIT regex found {len(matches)} matches")
        
        seen_urls = set()
        for url, title, date_str in matches:
            try:
                title = title.strip()
                
                if not title or len(title) < 5:
                    continue
                
                # 去重
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                
                # 过滤无关内容
                if any(kw in title for kw in ["更多", "首页", "上一页", "下一页"]):
                    continue
                
                # 解析日期
                try:
                    publish_time = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    publish_time = None
                
                # 构建完整 URL
                if url.startswith("/"):
                    full_url = f"{self.BASE_URL}{url}"
                elif url.startswith("http"):
                    full_url = url
                else:
                    full_url = f"{self.BASE_URL}/{url}"
                
                # 获取详情页内容
                content = await self.fetch_detail(full_url, trace_id=trace_id)
                
                news_item = self.create_news_item(
                    title=self._clean_text(title),
                    content=content or "",
                    url=full_url,
                    publish_time=publish_time,
                    category=cat_info["category"],
                    tags=[cat_info["name"], "工信部"],
                )
                items.append(news_item)
                
                if len(items) >= limit:
                    break
            
            except Exception as e:
                self.logger.debug(f"MIIT parse item error: {e}")
                continue
        
        return items
    
    async def fetch_detail(
        self,
        url: str,
        trace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        抓取文章详情 (静默处理错误)
        
        Args:
            url: 文章 URL
            trace_id: 追踪ID
            
        Returns:
            文章正文
        """
        try:
            # 直接使用 client，静默处理错误
            resp = await self.client.get(url, timeout=10)
            
            # 非 200 静默返回
            if resp.status_code != 200:
                return None
            
            html = resp.text
            
            # 提取正文
            content = self._extract_content(html)
            
            return content
            
        except Exception as e:
            self.logger.debug(f"[{trace_id}] Fetch detail error: {e}")
            return None
    
    def _extract_content(self, html: str) -> str:
        """从 HTML 中提取正文"""
        # 尝试匹配正文区域
        patterns = [
            r'<div[^>]*class="[^"]*article-content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*TRS_Editor[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*id="[^"]*content[^"]*"[^>]*>(.*?)</div>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)
                return self._clean_text(content)
        
        return ""
    
    def _clean_text(self, text: str) -> str:
        """清理文本"""
        if not text:
            return ""
        # 去除 HTML 标签
        text = re.sub(r'<[^>]+>', '', text)
        # 去除多余空白
        text = re.sub(r'\s+', ' ', text)
        # 去除特殊字符
        text = re.sub(r'[\r\n\t]', '', text)
        return text.strip()
