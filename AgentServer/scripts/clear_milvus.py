#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
清空 Milvus 向量数据库

用法:
    python scripts/clear_milvus.py              # 清空所有集合
    python scripts/clear_milvus.py --collection research_reports  # 只清空指定集合
    python scripts/clear_milvus.py --list       # 列出所有集合
"""

import sys
import asyncio
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.settings import settings


def get_milvus_uri():
    """构建 Milvus URI"""
    host = settings.milvus.host
    port = settings.milvus.port
    return f"http://{host}:{port}"


async def list_collections():
    """列出所有集合"""
    from pymilvus import MilvusClient
    
    uri = get_milvus_uri()
    print(f"连接 Milvus: {uri}")
    
    try:
        client = MilvusClient(uri=uri)
        collections = client.list_collections()
    except Exception as e:
        print(f"连接失败: {e}")
        return []
    
    print(f"\n找到 {len(collections)} 个集合:")
    for col in collections:
        try:
            stats = client.get_collection_stats(col)
            row_count = stats.get("row_count", 0)
            print(f"  - {col}: {row_count} 条记录")
        except Exception:
            print(f"  - {col}: (无法获取统计)")
    
    return collections


async def clear_collection(collection_name: str):
    """清空指定集合（删除后重建）"""
    from pymilvus import MilvusClient
    
    uri = get_milvus_uri()
    client = MilvusClient(uri=uri)
    
    if not client.has_collection(collection_name):
        print(f"集合 '{collection_name}' 不存在")
        return
    
    # 获取原集合信息
    stats = client.get_collection_stats(collection_name)
    row_count = stats.get("row_count", 0)
    print(f"集合 '{collection_name}' 当前有 {row_count} 条记录")
    
    # 删除集合
    print(f"正在删除集合 '{collection_name}'...")
    client.drop_collection(collection_name)
    
    # 重建集合
    print(f"正在重建集合 '{collection_name}'...")
    client.create_collection(
        collection_name=collection_name,
        dimension=settings.milvus.embedding_dim,
        metric_type="COSINE",
    )
    
    print(f"集合 '{collection_name}' 已清空并重建")


async def clear_all():
    """清空所有集合"""
    collections = [
        settings.milvus.research_reports_collection,
        settings.milvus.market_snippets_collection,
    ]
    
    for col in collections:
        await clear_collection(col)
    
    print("\n所有集合已清空")


def main():
    parser = argparse.ArgumentParser(description="清空 Milvus 向量数据库")
    parser.add_argument("--collection", "-c", help="指定要清空的集合名称")
    parser.add_argument("--list", "-l", action="store_true", help="列出所有集合")
    parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    args = parser.parse_args()
    
    if args.list:
        asyncio.run(list_collections())
        return
    
    if not args.yes:
        if args.collection:
            confirm = input(f"确定要清空集合 '{args.collection}'? [y/N]: ")
        else:
            confirm = input("确定要清空所有向量集合? [y/N]: ")
        
        if confirm.lower() != 'y':
            print("已取消")
            return
    
    if args.collection:
        asyncio.run(clear_collection(args.collection))
    else:
        asyncio.run(clear_all())


if __name__ == "__main__":
    main()
