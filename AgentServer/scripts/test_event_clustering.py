"""
测试事件聚类

用法:
    cd AgentServer
    
    # 运行事件聚类 (处理未聚类的新闻)
    python scripts/test_event_clustering.py
    
    # 指定批次大小
    python scripts/test_event_clustering.py --batch 50
    
    # 查看最近的事件
    python scripts/test_event_clustering.py --list
    
    # 查看最近 N 小时的事件
    python scripts/test_event_clustering.py --list --hours 24
"""

import asyncio
import argparse
import sys
import os
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from src.collector.event_cluster import EventClusterEngine


async def run_clustering(batch_size: int):
    """运行事件聚类"""
    print(f"\n{'='*60}")
    print("运行事件聚类")
    print(f"{'='*60}")
    print(f"批次大小: {batch_size}")
    print(f"{'='*60}\n")
    
    # 先检查待处理的新闻数量
    print("检查待聚类新闻...")
    pending_count = await check_pending_news()
    
    if pending_count == 0:
        print("没有待聚类的新闻，退出")
        return None
    
    print(f"\n开始聚类处理...")
    engine = EventClusterEngine()
    trace_id = f"test_{datetime.now().strftime('%H%M%S')}"
    
    print(f"[{trace_id}] 初始化 LLM 服务...")
    
    result = await engine.process_pending_news(
        batch_size=batch_size,
        trace_id=trace_id,
    )
    
    print(f"\n{'='*60}")
    print("聚类结果")
    print(f"{'='*60}")
    print(f"处理新闻数: {result.total_processed}")
    print(f"新建事件数: {result.new_events}")
    print(f"合并新闻数: {result.merged_news}")
    
    if result.events:
        print(f"\n新建/更新的事件:")
        for event in result.events[:10]:  # 只显示前 10 个
            print(f"  - [{event.importance.value}] {event.title}")
            print(f"    新闻数: {event.news_count}, 分类: {event.category}")
    
    print(f"\n{'='*60}")
    print("完成")
    print(f"{'='*60}\n")
    
    return result


async def list_events(hours: int, limit: int):
    """查看最近的事件"""
    print(f"\n{'='*60}")
    print(f"最近 {hours} 小时的事件")
    print(f"{'='*60}\n")
    
    engine = EventClusterEngine()
    trace_id = f"list_{datetime.now().strftime('%H%M%S')}"
    
    events = await engine.get_recent_events(
        hours=hours,
        limit=limit,
        trace_id=trace_id,
    )
    
    if not events:
        print("暂无事件")
    else:
        print(f"共 {len(events)} 个事件:\n")
        for i, event in enumerate(events, 1):
            importance = event.get("fingerprint", {}).get("importance", "unknown")
            news_count = event.get("news_count", 0)
            category = event.get("category", "general")
            title = event.get("title", "无标题")
            summary = event.get("summary", "")[:50]
            
            print(f"{i}. [{importance}] {title}")
            print(f"   分类: {category}, 新闻数: {news_count}")
            if summary:
                print(f"   摘要: {summary}...")
            print()
    
    print(f"{'='*60}")
    print("完成")
    print(f"{'='*60}\n")


async def check_pending_news():
    """检查待聚类的新闻数量"""
    from core.managers import mongo_manager
    from datetime import timedelta
    
    if not mongo_manager.is_initialized:
        await mongo_manager.initialize()
    
    # 查询条件与 process_pending_news 保持一致
    cutoff = datetime.utcnow() - timedelta(hours=48)  # 48小时内的新闻
    count = await mongo_manager._db["news"].count_documents({
        "clustered_at": {"$exists": False},
        "collect_time": {"$gte": cutoff},
    })
    
    print(f"待聚类新闻数: {count}")
    return count


def main():
    parser = argparse.ArgumentParser(description="测试事件聚类")
    parser.add_argument(
        "--batch",
        type=int,
        default=100,
        help="批次大小 (默认 100)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="查看最近的事件"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="查看最近 N 小时的事件 (默认 24)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="最多显示 N 个事件 (默认 50)"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="只检查待聚类的新闻数量"
    )
    
    args = parser.parse_args()
    
    if args.check:
        asyncio.run(check_pending_news())
    elif args.list:
        asyncio.run(list_events(args.hours, args.limit))
    else:
        asyncio.run(run_clustering(args.batch))


if __name__ == "__main__":
    main()
