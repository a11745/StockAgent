"""
知识加载器

从 Markdown 文件加载知识到向量库。
"""

import os
import re
import logging
import yaml
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime

from ..types import (
    FixedKnowledgeItem,
    FixedKnowledgeCategory,
    KnowledgeLoadResult,
)


class KnowledgeLoader:
    """
    知识加载器
    
    从 Markdown 文件加载知识，支持 Front Matter 元数据。
    
    文件格式:
    ```markdown
    ---
    id: unique_id
    category: tech_candlestick_single
    title: 锤子线
    tags: [反转, 底部]
    importance: high
    ---
    
    # 锤子线
    
    ## 形态特征
    ...
    ```
    
    Args:
        base_path: 知识文件根目录
    
    Example:
        loader = KnowledgeLoader("data/knowledge")
        items = await loader.load_all()
    """
    
    def __init__(self, base_path: str = "data/knowledge"):
        self.base_path = Path(base_path)
        self.logger = logging.getLogger("src.rag.knowledge.KnowledgeLoader")
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def load_all(
        self,
        generate_vectors: bool = True,
        trace_id: Optional[str] = None,
    ) -> Tuple[List[FixedKnowledgeItem], KnowledgeLoadResult]:
        """
        加载所有知识文件
        
        Args:
            generate_vectors: 是否生成向量
            trace_id: 追踪ID
            
        Returns:
            (知识项列表, 加载结果)
        """
        items = []
        result = KnowledgeLoadResult(success=True)
        
        if not self.base_path.exists():
            self.logger.warning(f"Knowledge path not found: {self.base_path}")
            result.success = False
            result.errors.append(f"Path not found: {self.base_path}")
            return items, result
        
        # 遍历所有 .md 文件
        md_files = list(self.base_path.rglob("*.md"))
        self.logger.info(f"[{trace_id}] Found {len(md_files)} markdown files")
        
        for file_path in md_files:
            try:
                item = await self._load_file(file_path, generate_vectors, trace_id)
                if item:
                    items.append(item)
                    result.loaded_count += 1
            except Exception as e:
                self.logger.error(f"[{trace_id}] Failed to load {file_path}: {e}")
                result.failed_count += 1
                result.errors.append(f"{file_path}: {str(e)}")
        
        self.logger.info(
            f"[{trace_id}] Loaded {result.loaded_count} items, "
            f"failed: {result.failed_count}"
        )
        
        return items, result
    
    async def load_category(
        self,
        category: FixedKnowledgeCategory,
        generate_vectors: bool = True,
        trace_id: Optional[str] = None,
    ) -> List[FixedKnowledgeItem]:
        """加载指定分类的知识"""
        items, _ = await self.load_all(generate_vectors, trace_id)
        return [item for item in items if item.category == category]
    
    async def load_file(
        self,
        file_path: str,
        generate_vectors: bool = True,
        trace_id: Optional[str] = None,
    ) -> Optional[FixedKnowledgeItem]:
        """加载单个文件"""
        return await self._load_file(Path(file_path), generate_vectors, trace_id)
    
    async def _load_file(
        self,
        file_path: Path,
        generate_vectors: bool,
        trace_id: Optional[str],
    ) -> Optional[FixedKnowledgeItem]:
        """加载单个 Markdown 文件"""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 解析 Front Matter
        front_matter, body = self._parse_front_matter(content)
        
        if not front_matter:
            # 没有 Front Matter，尝试从文件路径推断
            front_matter = self._infer_metadata(file_path)
        
        # 提取标题
        title = front_matter.get("title")
        if not title:
            title = self._extract_title(body) or file_path.stem
        
        # 解析分类
        category_str = front_matter.get("category", "")
        try:
            category = FixedKnowledgeCategory(category_str)
        except ValueError:
            category = self._infer_category(file_path)
        
        # 提取要点
        key_points = front_matter.get("key_points", [])
        if not key_points:
            key_points = self._extract_key_points(body)
        
        # 生成向量
        vector = []
        if generate_vectors:
            llm = await self._get_llm()
            text_for_embedding = f"{title}\n{front_matter.get('summary', '')}\n{body[:1000]}"
            vectors = await llm.embedding([text_for_embedding])
            if vectors:
                vector = vectors[0]
        
        item = FixedKnowledgeItem(
            id=front_matter.get("id", file_path.stem),
            title=title,
            content=body,
            category=category,
            tags=front_matter.get("tags", []),
            importance=front_matter.get("importance", "medium"),
            source_file=str(file_path.relative_to(self.base_path) if self.base_path in file_path.parents else file_path),
            source_url=front_matter.get("source_url"),
            summary=front_matter.get("summary"),
            key_points=key_points,
            examples=front_matter.get("examples", []),
            related_ids=front_matter.get("related_ids", []),
            prerequisites=front_matter.get("prerequisites", []),
            vector=vector,
        )
        
        return item
    
    def _parse_front_matter(self, content: str) -> Tuple[Dict[str, Any], str]:
        """解析 YAML Front Matter"""
        pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)$'
        match = re.match(pattern, content, re.DOTALL)
        
        if match:
            try:
                front_matter = yaml.safe_load(match.group(1))
                body = match.group(2).strip()
                return front_matter or {}, body
            except yaml.YAMLError:
                pass
        
        return {}, content
    
    def _infer_metadata(self, file_path: Path) -> Dict[str, Any]:
        """从文件路径推断元数据"""
        parts = file_path.relative_to(self.base_path).parts if self.base_path in file_path.parents else file_path.parts
        
        metadata = {
            "id": file_path.stem,
            "title": file_path.stem.replace("_", " ").replace("-", " ").title(),
        }
        
        # 从目录结构推断分类
        if parts:
            folder = parts[0] if len(parts) > 1 else ""
            metadata["inferred_category"] = folder
        
        return metadata
    
    def _infer_category(self, file_path: Path) -> FixedKnowledgeCategory:
        """从文件路径推断分类"""
        path_str = str(file_path).lower()
        
        # 大盘复盘
        if "market_review" in path_str or "复盘" in path_str:
            if "open" in path_str or "开盘" in path_str:
                return FixedKnowledgeCategory.MARKET_REVIEW_OPEN
            elif "close" in path_str or "收盘" in path_str:
                return FixedKnowledgeCategory.MARKET_REVIEW_CLOSE
            elif "week" in path_str or "周" in path_str:
                return FixedKnowledgeCategory.MARKET_REVIEW_WEEKLY
            return FixedKnowledgeCategory.MARKET_REVIEW_INTRADAY
        
        # 技术分析 - 筹码
        if "chip" in path_str or "筹码" in path_str:
            if "peak" in path_str or "峰" in path_str:
                return FixedKnowledgeCategory.TECH_CHIP_PEAK
            if "cost" in path_str or "成本" in path_str:
                return FixedKnowledgeCategory.TECH_CHIP_COST
            return FixedKnowledgeCategory.TECH_CHIP_DISTRIBUTION
        
        # 技术分析 - K线
        if "candle" in path_str or "k线" in path_str or "蜡烛" in path_str:
            if "single" in path_str or "单根" in path_str:
                return FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE
            if "double" in path_str or "组合" in path_str:
                return FixedKnowledgeCategory.TECH_CANDLESTICK_DOUBLE
            return FixedKnowledgeCategory.TECH_CANDLESTICK_PATTERN
        
        # 策略因子
        if "factor" in path_str or "因子" in path_str:
            if "volume" in path_str or "量价" in path_str:
                return FixedKnowledgeCategory.FACTOR_VOLUME_PRICE
            if "momentum" in path_str or "动量" in path_str:
                return FixedKnowledgeCategory.FACTOR_MOMENTUM
            if "sentiment" in path_str or "情绪" in path_str:
                return FixedKnowledgeCategory.FACTOR_SENTIMENT
            return FixedKnowledgeCategory.FACTOR_TECHNICAL
        
        # 均线
        if "ma" in path_str or "均线" in path_str or "moving_average" in path_str:
            return FixedKnowledgeCategory.TECH_MOVING_AVERAGE
        
        # 趋势
        if "trend" in path_str or "趋势" in path_str:
            return FixedKnowledgeCategory.TECH_TREND
        
        # 成交量
        if "volume" in path_str and "factor" not in path_str:
            return FixedKnowledgeCategory.TECH_VOLUME
        
        # 默认
        return FixedKnowledgeCategory.TECH_CANDLESTICK_PATTERN
    
    def _extract_title(self, body: str) -> Optional[str]:
        """从内容中提取标题"""
        match = re.search(r'^#\s+(.+)$', body, re.MULTILINE)
        if match:
            return match.group(1).strip()
        return None
    
    def _extract_key_points(self, body: str) -> List[str]:
        """从内容中提取要点"""
        points = []
        
        # 查找 ## 要点 或 ## Key Points 段落
        pattern = r'##\s*(?:要点|关键点|Key\s*Points?|重点)\s*\n(.*?)(?=\n##|\Z)'
        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        
        if match:
            content = match.group(1)
            # 提取列表项
            items = re.findall(r'^[-*]\s+(.+)$', content, re.MULTILINE)
            points.extend(items[:5])  # 最多 5 个
        
        return points
    
    async def reload_all(
        self,
        store: "FixedKnowledgeStore",
        trace_id: Optional[str] = None,
    ) -> KnowledgeLoadResult:
        """
        重新加载所有知识到存储
        
        Args:
            store: 知识存储
            trace_id: 追踪ID
        """
        items, result = await self.load_all(generate_vectors=True, trace_id=trace_id)
        
        if items:
            # 清空并重新插入
            await store.clear(trace_id)
            insert_result = await store.insert_batch(items, trace_id)
            result.loaded_count = len(insert_result.inserted_ids) if hasattr(insert_result, 'inserted_ids') else len(items)
        
        return result
