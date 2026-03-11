"""
调试澎湃新闻网页结构
"""

import asyncio
import httpx
import re


async def main():
    url = "https://www.thepaper.cn/newsDetail_forward_32740090"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=10)
        html = resp.text
        
        print(f"状态码: {resp.status_code}")
        print(f"HTML 长度: {len(html)}")
        print()
        
        # 查找可能的正文区域
        patterns = [
            (r'class="[^"]*content[^"]*"', "content 类"),
            (r'class="[^"]*article[^"]*"', "article 类"),
            (r'class="[^"]*news[^"]*"', "news 类"),
            (r'class="[^"]*text[^"]*"', "text 类"),
            (r'class="[^"]*body[^"]*"', "body 类"),
            (r'class="[^"]*detail[^"]*"', "detail 类"),
            (r'class="[^"]*main[^"]*"', "main 类"),
        ]
        
        print("=" * 60)
        print("查找 class 名称:")
        print("=" * 60)
        for pattern, name in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                print(f"{name}: {set(matches)}")
        
        print()
        print("=" * 60)
        print("尝试提取正文:")
        print("=" * 60)
        
        # 更多的提取模式
        extract_patterns = [
            r'<div[^>]*class="[^"]*index_cententWrap[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*news_txt[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*content_[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*newsContent[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*article[^"]*"[^>]*>(.*?)</div>',
            r'<article[^>]*>(.*?)</article>',
        ]
        
        for pattern in extract_patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)
                clean = re.sub(r'<[^>]+>', '', content)
                clean = re.sub(r'\s+', ' ', clean).strip()
                print(f"\n模式: {pattern[:50]}...")
                print(f"长度: {len(clean)}")
                print(f"内容预览: {clean[:200]}...")
        
        # 提取所有 <p> 标签
        print()
        print("=" * 60)
        print("<p> 标签内容:")
        print("=" * 60)
        p_tags = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL)
        print(f"共找到 {len(p_tags)} 个 <p> 标签")
        
        all_p_content = ""
        for i, p in enumerate(p_tags[:10]):
            clean = re.sub(r'<[^>]+>', '', p)
            clean = re.sub(r'\s+', ' ', clean).strip()
            if clean and len(clean) > 10:
                print(f"[{i}] {clean[:100]}...")
                all_p_content += clean + " "
        
        print(f"\n合并后长度: {len(all_p_content)}")
        
        # 保存 HTML 供分析
        with open("debug_thepaper.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("\nHTML 已保存到 debug_thepaper.html")


if __name__ == "__main__":
    asyncio.run(main())
