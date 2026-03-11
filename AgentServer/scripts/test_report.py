"""
测试早报/午报生成 (详细调试版)

用法:
    cd AgentServer
    
    # 测试早报 (不推送)
    python scripts/test_report.py morning
    
    # 测试午报 (不推送)
    python scripts/test_report.py noon
    
    # 指定时间范围 (小时)
    python scripts/test_report.py morning --hours 24
    
    # 详细模式 (打印所有事件)
    python scripts/test_report.py morning --verbose
"""

import asyncio
import argparse
import sys
import os
import logging
from datetime import datetime, timedelta
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from src.report import ReportGenerator, ReportType
from src.report.analyzer import EventAnalyzer
from core.managers import mongo_manager


async def fetch_events(hours_back: int):
    """获取聚合后的事件"""
    if not mongo_manager.is_initialized:
        await mongo_manager.initialize()
    
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    
    events = await mongo_manager.find_many(
        "news_events",
        {
            "last_update_time": {"$gte": cutoff},
        },
        limit=200,
        sort=[("news_count", -1), ("last_update_time", -1)],
    )
    
    return events


async def test_report_detailed(
    report_type: str,
    date: str,
    hours_back: int,
    push_wechat: bool,
    push_websocket: bool,
    verbose: bool,
):
    """详细测试报告生成"""
    print(f"\n{'='*70}")
    print(f"测试 {report_type} 报告 (详细模式)")
    print(f"{'='*70}")
    print(f"日期: {date}")
    print(f"时间范围: 过去 {hours_back} 小时")
    print(f"{'='*70}\n")
    
    # ==================== 步骤 1: 获取事件 ====================
    print(f"{'='*70}")
    print("步骤 1: 获取聚合后的事件 (从 news_events 集合)")
    print(f"{'='*70}")
    
    t1 = time.time()
    events = await fetch_events(hours_back)
    t2 = time.time()
    
    print(f"查询耗时: {(t2-t1)*1000:.0f}ms")
    print(f"获取到 {len(events)} 个事件\n")
    
    if not events:
        print("❌ 没有找到事件！")
        print("\n可能原因:")
        print("  1. news_events 集合为空 (需要先运行事件聚类)")
        print("  2. 时间范围内没有事件 (尝试增加 --hours 参数)")
        print("\n建议:")
        print("  python scripts/test_event_clustering.py  # 先运行事件聚类")
        return
    
    # 显示事件详情
    print("事件列表:")
    print("-" * 70)
    for i, event in enumerate(events[:20] if not verbose else events, 1):
        title = event.get("title", "无标题")[:50]
        category = event.get("category", "general")
        news_count = event.get("news_count", 0)
        importance = event.get("fingerprint", {}).get("importance", "unknown")
        sources = event.get("sources", [])[:3]
        
        print(f"{i:2}. [{importance:6}] [{category:10}] (新闻数:{news_count:2}) {title}")
        if verbose:
            print(f"    来源: {', '.join(sources)}")
            summary = event.get("summary", "")[:80]
            if summary:
                print(f"    摘要: {summary}")
    
    if len(events) > 20 and not verbose:
        print(f"... 还有 {len(events) - 20} 个事件 (使用 --verbose 查看全部)")
    print()
    
    # ==================== 步骤 2: 分析重要性 ====================
    print(f"{'='*70}")
    print("步骤 2: LLM 分析事件重要性")
    print(f"{'='*70}")
    
    analyzer = EventAnalyzer()
    trace_id = f"test_{datetime.now().strftime('%H%M%S')}"
    
    print(f"使用 LLM 分析 {len(events)} 个事件...")
    print("(这可能需要一些时间...)\n")
    
    t1 = time.time()
    items = await analyzer.analyze_importance(events, trace_id)
    t2 = time.time()
    
    print(f"分析耗时: {(t2-t1)*1000:.0f}ms")
    print(f"分析完成，生成 {len(items)} 个报告项\n")
    
    if items:
        # 按重要性分组统计
        high_count = sum(1 for item in items if item.importance == "high")
        medium_count = sum(1 for item in items if item.importance == "medium")
        low_count = sum(1 for item in items if item.importance == "low")
        
        print(f"重要性分布:")
        print(f"  🔴 高: {high_count}")
        print(f"  🟡 中: {medium_count}")
        print(f"  🟢 低: {low_count}")
        print()
        
        # 显示分析结果
        print("分析结果 (前 10 项):")
        print("-" * 70)
        for i, item in enumerate(items[:10], 1):
            imp = item.importance.value if hasattr(item.importance, 'value') else str(item.importance)
            icon = "🔴" if imp == "high" else ("🟡" if imp == "medium" else "🟢")
            category = item.raw_event.get("category", "general") if item.raw_event else "general"
            print(f"{i:2}. {icon} [{category:10}] {item.title[:45]}")
            if item.summary:
                print(f"    摘要: {item.summary[:60]}...")
        print()
    
    # ==================== 步骤 3: 生成报告 ====================
    print(f"{'='*70}")
    print("步骤 3: 生成完整报告")
    print(f"{'='*70}")
    
    generator = ReportGenerator()
    
    if report_type == "morning":
        rtype = ReportType.MORNING
    else:
        rtype = ReportType.NOON
    
    t1 = time.time()
    result = await generator.generate_and_push(
        report_type=rtype,
        date=date,
        hours_back=hours_back,
        push_wechat=push_wechat,
        push_websocket=push_websocket,
        trace_id=trace_id,
    )
    t2 = time.time()
    
    print(f"生成耗时: {(t2-t1)*1000:.0f}ms")
    print(f"成功: {result.success}")
    
    if result.errors:
        print(f"错误: {result.errors}")
    
    if result.report:
        report = result.report
        print(f"\n报告ID: {report.id}")
        print(f"标题: {report.title}")
        
        print(f"\n统计:")
        print(f"  事件数: {report.stats.event_count}")
        print(f"  新闻数: {report.stats.news_count}")
        print(f"  重要事件: {report.stats.high_importance_count}")
        print(f"  宏观: {report.stats.macro_count}")
        print(f"  行业: {report.stats.industry_count}")
        print(f"  个股: {report.stats.stock_count}")
        print(f"  热点: {report.stats.hot_count}")
        
        if report.sections:
            print(f"\n板块详情:")
            for section in report.sections:
                print(f"  📁 {section.title}: {len(section.items)} 条")
                for item in section.items[:3]:
                    print(f"     - {item.title[:40]}")
                if len(section.items) > 3:
                    print(f"     ... 还有 {len(section.items) - 3} 条")
        
        # 打印 Markdown 内容
        if report.content_markdown:
            print(f"\n{'='*70}")
            print("Markdown 内容")
            print(f"{'='*70}")
            print(report.content_markdown[:1500])
            if len(report.content_markdown) > 1500:
                print(f"\n... (共 {len(report.content_markdown)} 字符)")
    
    print(f"\n{'='*70}")
    print("测试完成")
    print(f"{'='*70}\n")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="测试早报/午报生成 (详细模式)")
    parser.add_argument(
        "type",
        choices=["morning", "noon"],
        help="报告类型: morning=早报, noon=午报"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="日期 (格式: YYYY-MM-DD, 默认今天)"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=None,
        help="时间范围 (小时, 默认: 早报16小时, 午报6小时)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="详细模式 (显示所有事件)"
    )
    parser.add_argument(
        "--push-wechat",
        action="store_true",
        help="推送到企业微信"
    )
    parser.add_argument(
        "--push-websocket",
        action="store_true",
        help="推送到 WebSocket"
    )
    
    args = parser.parse_args()
    
    # 默认值
    date = args.date or datetime.now().strftime("%Y-%m-%d")
    
    if args.hours:
        hours_back = args.hours
    else:
        hours_back = 16 if args.type == "morning" else 6
    
    # 运行测试
    asyncio.run(test_report_detailed(
        report_type=args.type,
        date=date,
        hours_back=hours_back,
        push_wechat=args.push_wechat,
        push_websocket=args.push_websocket,
        verbose=args.verbose,
    ))


if __name__ == "__main__":
    main()
