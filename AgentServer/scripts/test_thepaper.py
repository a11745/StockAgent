"""
澎湃新闻采集测试脚本 (独立版本，无需项目依赖)
"""

import asyncio
import httpx
import re
from datetime import datetime


SIDEBAR_URL = "https://cache.thepaper.cn/contentapi/wwwIndex/rightSidebar"


async def fetch_detail(client: httpx.AsyncClient, cont_id: str) -> str:
    """获取文章详情内容"""
    try:
        page_url = f"https://www.thepaper.cn/newsDetail_forward_{cont_id}"
        resp = await client.get(page_url, timeout=10)
        
        if resp.status_code != 200:
            return ""
        
        html = resp.text
        
        # 提取所有 <p> 标签内容
        p_tags = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
        
        paragraphs = []
        for p in p_tags:
            text = re.sub(r'<[^>]+>', '', p)
            text = re.sub(r'&[a-zA-Z]+;', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            if text and len(text) > 10:
                paragraphs.append(text)
        
        content = ' '.join(paragraphs)
        return content[:3000] if content else ""
        
    except Exception as e:
        print(f"    获取详情失败: {e}")
        return ""


async def main():
    print("=" * 60)
    print("澎湃新闻采集测试")
    print("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # 获取列表
        resp = await client.get(SIDEBAR_URL)
        data = resp.json()
        
        if data.get("resultCode") not in [1, "1"]:
            print("API 请求失败")
            return
        
        inner_data = data.get("data", {})
        hot_news = inner_data.get("hotNews", [])[:10]
        
        print(f"\n共获取到 {len(hot_news)} 条热榜新闻\n")
        
        for i, item in enumerate(hot_news, 1):
            title = item.get("name", "")
            cont_id = item.get("contId", "")
            
            # 解析时间
            pub_time_long = item.get("pubTimeLong")
            publish_time = None
            if pub_time_long:
                try:
                    ts = int(pub_time_long)
                    if ts > 10000000000:
                        ts = ts / 1000
                    publish_time = datetime.fromtimestamp(ts)
                except:
                    pass
            
            print("-" * 60)
            print(f"[{i}] 标题: {title}")
            print(f"    ID: {cont_id}")
            print(f"    时间: {publish_time}")
            
            # 获取列表中的摘要
            summary = item.get("summary", "") or item.get("intro", "")
            print(f"    列表摘要: {summary[:100] if summary else '[空]'}")
            
            # 获取详情页内容
            if cont_id:
                content = await fetch_detail(client, cont_id)
                if content:
                    print(f"    详情内容: {content[:200]}...")
                    print(f"    内容长度: {len(content)} 字符")
                else:
                    print(f"    详情内容: [获取失败]")
            print()


if __name__ == "__main__":
    asyncio.run(main())
