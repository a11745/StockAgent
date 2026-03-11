"""
金十数据采集器

采集内容:
- 快讯 (实时滚动)
"""

import re
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.collector.sources.base import BaseSource
from src.collector.types import NewsItem, NewsSource, NewsCategory


class Jin10Source(BaseSource):
    """
    金十数据采集器
    
    API: https://www.jin10.com/flash_newest.js
    """
    
    name = NewsSource.JIN10
    display_name = "金十数据"
    default_category = NewsCategory.FINANCE_FLASH
    
    FLASH_URL = "https://www.jin10.com/flash_newest.js"
    
    async def fetch(
        self,
        since: Optional[datetime] = None,
        limit: int = 100,
        trace_id: Optional[str] = None,
    ) -> List[NewsItem]:
        """抓取快讯"""
        items = []
        
        try:
            timestamp = int(time.time() * 1000)
            url = f"{self.FLASH_URL}?t={timestamp}"
            
            resp = await self.fetch_page(url)
            if not resp:
                return items
            
            # 解析 JS 格式
            raw_data = resp.text
            json_str = raw_data.replace("var newest = ", "").rstrip(";").strip()
            data_list = json.loads(json_str)
            
            for item in data_list[:limit]:
                data = item.get("data", {})
                title = data.get("title") or data.get("content", "")
                
                if not title:
                    continue
                
                # 去除 HTML
                title = re.sub(r"</?b>", "", title)
                
                # 提取【】中的标题
                match = re.match(r"^【([^】]*)】(.*)$", title)
                if match:
                    main_title = match.group(1)
                    content = match.group(2)
                else:
                    main_title = title[:50]
                    content = title
                
                # 解析时间
                time_str = item.get("time", "")
                publish_time = None
                if time_str:
                    try:
                        publish_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
                
                is_important = bool(item.get("important"))
                
                news_item = self.create_news_item(
                    title=main_title,
                    content=content,
                    url=f"https://flash.jin10.com/detail/{item.get('id', '')}",
                    publish_time=publish_time,
                    category=NewsCategory.FINANCE_FLASH,
                    source_id=str(item.get("id", "")),
                    tags=["快讯"] + (["重要"] if is_important else []),
                    extra={"important": is_important},
                )
                items.append(news_item)
            
            if since:
                items = [item for item in items if item.publish_time and item.publish_time > since]
            
            self.logger.info(f"[{trace_id}] Jin10 fetched {len(items)} items")
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Fetch error: {e}")
        
        return items[:limit]
