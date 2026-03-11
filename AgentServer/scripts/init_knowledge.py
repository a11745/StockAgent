"""
知识库初始化脚本

从 data/knowledge/ 目录加载知识文件到向量数据库。

Usage:
    python scripts/init_knowledge.py
    python scripts/init_knowledge.py --reload  # 清空后重新加载
"""

import asyncio
import argparse
import logging
from pathlib import Path

# 添加项目路径
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.rag import KnowledgeLoader, FixedKnowledgeStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def init_knowledge(reload: bool = False):
    """初始化知识库"""
    
    knowledge_path = Path(__file__).parent.parent / "data" / "knowledge"
    
    if not knowledge_path.exists():
        logger.error(f"Knowledge path not found: {knowledge_path}")
        return
    
    logger.info(f"Loading knowledge from: {knowledge_path}")
    
    # 初始化
    loader = KnowledgeLoader(str(knowledge_path))
    store = FixedKnowledgeStore()
    
    # 检查现有数据
    existing_count = await store.count()
    logger.info(f"Existing knowledge count: {existing_count}")
    
    if existing_count > 0 and not reload:
        logger.info("Knowledge already loaded. Use --reload to reload.")
        return
    
    # 清空现有数据 (如果 reload)
    if reload and existing_count > 0:
        logger.info("Clearing existing knowledge...")
        await store.clear(trace_id="init")
    
    # 加载知识
    logger.info("Loading knowledge files...")
    items, result = await loader.load_all(
        generate_vectors=True,
        trace_id="init",
    )
    
    if result.errors:
        for error in result.errors:
            logger.warning(f"Load error: {error}")
    
    if not items:
        logger.warning("No knowledge items loaded")
        return
    
    # 存入数据库
    logger.info(f"Inserting {len(items)} items into database...")
    insert_result = await store.insert_batch(items, trace_id="init")
    
    if insert_result.success:
        logger.info(f"Successfully loaded {len(items)} knowledge items")
        
        # 显示分类统计
        categories = await store.list_categories(trace_id="init")
        logger.info("Knowledge by category:")
        for cat, count in categories.items():
            logger.info(f"  - {cat}: {count}")
    else:
        logger.error(f"Insert failed: {insert_result.message}")


def main():
    parser = argparse.ArgumentParser(description="Initialize knowledge base")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Clear existing knowledge and reload",
    )
    args = parser.parse_args()
    
    asyncio.run(init_knowledge(reload=args.reload))


if __name__ == "__main__":
    main()
