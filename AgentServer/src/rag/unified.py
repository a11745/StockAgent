"""
统一 RAG Pipeline

整合:
1. 固定知识库 (公共知识)
2. 用户知识库 (个人交易体系)
3. 记忆系统 (语义/情景/程序性记忆)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from .knowledge import (
    FixedKnowledgeStore,
    UserKnowledgeStore,
    FixedKnowledgeCategory,
    UserKnowledgeType,
    KnowledgeSearchResult,
)
from ..memory import (
    memory_manager,
    SemanticStore,
    EpisodicStore,
    ProceduralStore,
)


class RetrievalSource(str, Enum):
    """检索来源"""
    FIXED_KNOWLEDGE = "fixed_knowledge"      # 固定知识库
    USER_KNOWLEDGE = "user_knowledge"        # 用户知识库
    SEMANTIC_MEMORY = "semantic_memory"      # 语义记忆 (新闻/研报)
    EPISODIC_MEMORY = "episodic_memory"      # 情景记忆 (历史分析)
    PROCEDURAL_MEMORY = "procedural_memory"  # 程序性记忆 (交易模式)


@dataclass
class RetrievalConfig:
    """检索配置"""
    # 各来源的开关
    use_fixed_knowledge: bool = True
    use_user_knowledge: bool = True
    use_semantic_memory: bool = True
    use_episodic_memory: bool = True
    use_procedural_memory: bool = False  # 默认关闭，需要明确场景才用
    
    # 各来源的 top_k
    fixed_top_k: int = 5
    user_top_k: int = 5
    semantic_top_k: int = 5
    episodic_top_k: int = 5
    procedural_top_k: int = 3
    
    # 过滤条件
    fixed_categories: Optional[List[FixedKnowledgeCategory]] = None
    user_knowledge_types: Optional[List[UserKnowledgeType]] = None
    ts_code: Optional[str] = None
    ts_codes: Optional[List[str]] = None
    
    # 时间范围 (用于新闻检索)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    
    # 重排序
    use_reranker: bool = False
    rerank_top_k: int = 10


@dataclass
class RetrievalItem:
    """检索结果项"""
    source: RetrievalSource
    content: str
    title: Optional[str] = None
    score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_context_str(self) -> str:
        """转换为上下文字符串"""
        prefix = ""
        if self.source == RetrievalSource.FIXED_KNOWLEDGE:
            prefix = f"[知识库] "
        elif self.source == RetrievalSource.USER_KNOWLEDGE:
            prefix = f"[我的规则] "
        elif self.source == RetrievalSource.SEMANTIC_MEMORY:
            source_name = self.metadata.get("source", "资讯")
            prefix = f"[{source_name}] "
        elif self.source == RetrievalSource.EPISODIC_MEMORY:
            prefix = f"[历史分析] "
        elif self.source == RetrievalSource.PROCEDURAL_MEMORY:
            prefix = f"[交易模式] "
        
        title_str = f"**{self.title}**\n" if self.title else ""
        return f"{prefix}{title_str}{self.content}"


@dataclass
class RAGResult:
    """RAG 结果"""
    query: str
    items: List[RetrievalItem] = field(default_factory=list)
    context: str = ""
    response: str = ""
    
    # 统计
    total_count: int = 0
    by_source: Dict[str, int] = field(default_factory=dict)
    query_time_ms: float = 0
    
    # 引用
    citations: List[Dict[str, Any]] = field(default_factory=list)


class UnifiedRAGPipeline:
    """
    统一 RAG Pipeline
    
    整合多个知识源:
    1. 固定知识库 (公共复盘规则、技术分析知识)
    2. 用户知识库 (个人交易规则、策略)
    3. 记忆系统 (新闻、历史分析、交易模式)
    
    Example:
        rag = UnifiedRAGPipeline()
        
        # 简单问答
        result = await rag.query(
            user_id="user1",
            query="如何分析筹码峰",
        )
        
        # 复盘辅助
        result = await rag.query(
            user_id="user1",
            query="帮我复盘这笔茅台的交易",
            config=RetrievalConfig(
                use_fixed_knowledge=True,   # 复盘规则
                use_user_knowledge=True,    # 我的规则
                use_episodic_memory=True,   # 历史分析
                use_procedural_memory=True, # 交易模式
                ts_code="600519.SH",
            ),
        )
        
        # 技术分析
        result = await rag.query(
            user_id="user1",
            query="分析一下这根K线",
            config=RetrievalConfig(
                use_fixed_knowledge=True,
                fixed_categories=[FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE],
            ),
        )
    """
    
    def __init__(self):
        self.logger = logging.getLogger("src.rag.UnifiedRAGPipeline")
        
        # 知识存储
        self.fixed_store = FixedKnowledgeStore()
        self.user_store = UserKnowledgeStore()
        
        # 记忆存储 (复用 memory_manager)
        self.semantic_store = SemanticStore()
        self.episodic_store = EpisodicStore()
        self.procedural_store = ProceduralStore()
        
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def query(
        self,
        user_id: str,
        query: str,
        config: Optional[RetrievalConfig] = None,
        generate_response: bool = True,
        system_prompt: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> RAGResult:
        """
        执行 RAG 查询
        
        Args:
            user_id: 用户ID
            query: 查询文本
            config: 检索配置
            generate_response: 是否生成回答
            system_prompt: 自定义系统提示词
            trace_id: 追踪ID
            
        Returns:
            RAG 结果
        """
        config = config or RetrievalConfig()
        start_time = datetime.utcnow()
        
        result = RAGResult(query=query)
        
        try:
            # 1. 多路检索
            items = await self._retrieve(user_id, query, config, trace_id)
            result.items = items
            result.total_count = len(items)
            
            # 统计来源分布
            for item in items:
                source = item.source.value
                result.by_source[source] = result.by_source.get(source, 0) + 1
            
            # 2. 重排序 (可选)
            if config.use_reranker and len(items) > config.rerank_top_k:
                items = await self._rerank(query, items, config.rerank_top_k, trace_id)
                result.items = items
            
            # 3. 组装上下文
            result.context = self._build_context(items)
            
            # 4. 生成回答
            if generate_response:
                result.response = await self._generate(
                    query=query,
                    context=result.context,
                    system_prompt=system_prompt,
                    trace_id=trace_id,
                )
                
                # 提取引用
                result.citations = self._extract_citations(result.response, items)
            
            result.query_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            self.logger.info(
                f"[{trace_id}] RAG query completed: {result.total_count} items, "
                f"{result.query_time_ms:.1f}ms"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] RAG query failed: {e}")
            result.response = f"抱歉，查询过程中发生错误: {str(e)}"
            return result
    
    async def _retrieve(
        self,
        user_id: str,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """多路检索"""
        import asyncio
        
        items = []
        tasks = []
        
        # 1. 固定知识检索
        if config.use_fixed_knowledge:
            tasks.append(self._retrieve_fixed(query, config, trace_id))
        
        # 2. 用户知识检索
        if config.use_user_knowledge:
            tasks.append(self._retrieve_user(user_id, query, config, trace_id))
        
        # 3. 语义记忆检索 (新闻/研报)
        if config.use_semantic_memory:
            tasks.append(self._retrieve_semantic(query, config, trace_id))
        
        # 4. 情景记忆检索 (历史分析)
        if config.use_episodic_memory:
            tasks.append(self._retrieve_episodic(user_id, query, config, trace_id))
        
        # 5. 程序性记忆检索 (交易模式)
        if config.use_procedural_memory:
            tasks.append(self._retrieve_procedural(user_id, query, config, trace_id))
        
        # 并行执行
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.warning(f"[{trace_id}] Retrieval task failed: {result}")
                continue
            items.extend(result)
        
        # 按分数排序
        items.sort(key=lambda x: x.score, reverse=True)
        
        return items
    
    async def _retrieve_fixed(
        self,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """检索固定知识"""
        result = await self.fixed_store.search(
            query=query,
            top_k=config.fixed_top_k,
            categories=config.fixed_categories,
            trace_id=trace_id,
        )
        
        items = []
        for item in result.fixed_items:
            items.append(RetrievalItem(
                source=RetrievalSource.FIXED_KNOWLEDGE,
                content=item.content[:1000],
                title=item.title,
                score=item.score or 0.5,
                metadata={
                    "id": item.id,
                    "category": item.category.value if hasattr(item.category, 'value') else str(item.category),
                    "key_points": item.key_points,
                },
            ))
        
        return items
    
    async def _retrieve_user(
        self,
        user_id: str,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """检索用户知识"""
        result = await self.user_store.search(
            user_id=user_id,
            query=query,
            top_k=config.user_top_k,
            trace_id=trace_id,
        )
        
        items = []
        for item in result.user_items:
            content = item.content
            if item.conditions:
                content += f"\n条件: {', '.join(item.conditions)}"
            if item.actions:
                content += f"\n动作: {', '.join(item.actions)}"
            
            items.append(RetrievalItem(
                source=RetrievalSource.USER_KNOWLEDGE,
                content=content[:1000],
                title=item.title,
                score=item.score or 0.5,
                metadata={
                    "id": item.id,
                    "type": item.knowledge_type.value,
                    "tags": item.tags,
                },
            ))
        
        return items
    
    async def _retrieve_semantic(
        self,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """检索语义记忆 (新闻/研报)"""
        result = await self.semantic_store.search(
            query=query,
            top_k=config.semantic_top_k,
            ts_code=config.ts_code,
            ts_codes=config.ts_codes,
            date_from=config.date_from,
            date_to=config.date_to,
            trace_id=trace_id,
        )
        
        items = []
        for item in result.items:
            items.append(RetrievalItem(
                source=RetrievalSource.SEMANTIC_MEMORY,
                content=item.content[:800],
                title=None,
                score=item.score or 0.5,
                metadata={
                    "id": item.id,
                    "ts_code": item.metadata.ts_code,
                    "source": item.metadata.source,
                    "category": item.metadata.category,
                },
            ))
        
        return items
    
    async def _retrieve_episodic(
        self,
        user_id: str,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """检索情景记忆 (历史分析)"""
        result = await self.episodic_store.search(
            user_id=user_id,
            query=query,
            top_k=config.episodic_top_k,
            ts_code=config.ts_code,
            trace_id=trace_id,
        )
        
        items = []
        for item in result.items:
            items.append(RetrievalItem(
                source=RetrievalSource.EPISODIC_MEMORY,
                content=item.content[:800],
                title=None,
                score=item.score or 0.5,
                metadata={
                    "id": item.id,
                    "ts_code": item.metadata.ts_code,
                    "category": item.metadata.category,
                    "created_at": item.metadata.created_at.isoformat() if item.metadata.created_at else None,
                },
            ))
        
        return items
    
    async def _retrieve_procedural(
        self,
        user_id: str,
        query: str,
        config: RetrievalConfig,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """检索程序性记忆 (交易模式)"""
        patterns = await self.procedural_store.get_patterns(
            user_id=user_id,
            min_confidence=0.3,
            trace_id=trace_id,
        )
        
        items = []
        for pattern in patterns[:config.procedural_top_k]:
            content = f"{pattern.description}\n"
            if pattern.conditions:
                content += f"条件: {', '.join(pattern.conditions)}\n"
            if pattern.actions:
                content += f"动作: {', '.join(pattern.actions)}"
            
            items.append(RetrievalItem(
                source=RetrievalSource.PROCEDURAL_MEMORY,
                content=content,
                title=pattern.name,
                score=pattern.confidence,
                metadata={
                    "id": pattern.id,
                    "type": pattern.pattern_type.value,
                    "success_rate": pattern.success_rate,
                    "sample_count": pattern.sample_count,
                },
            ))
        
        return items
    
    async def _rerank(
        self,
        query: str,
        items: List[RetrievalItem],
        top_k: int,
        trace_id: Optional[str],
    ) -> List[RetrievalItem]:
        """重排序 (使用 LLM 打分)"""
        if len(items) <= top_k:
            return items
        
        llm = await self._get_llm()
        
        # 简单的规则重排序 (避免 LLM 调用开销)
        # 可以后续升级为交叉编码器
        
        # 优先级权重
        source_weights = {
            RetrievalSource.USER_KNOWLEDGE: 1.2,      # 用户知识优先
            RetrievalSource.FIXED_KNOWLEDGE: 1.1,    # 固定知识次之
            RetrievalSource.EPISODIC_MEMORY: 1.0,    # 历史分析
            RetrievalSource.SEMANTIC_MEMORY: 0.9,    # 新闻资讯
            RetrievalSource.PROCEDURAL_MEMORY: 1.0,  # 交易模式
        }
        
        for item in items:
            weight = source_weights.get(item.source, 1.0)
            item.score = item.score * weight
        
        items.sort(key=lambda x: x.score, reverse=True)
        
        return items[:top_k]
    
    def _build_context(self, items: List[RetrievalItem]) -> str:
        """组装上下文"""
        if not items:
            return ""
        
        sections = {}
        
        for item in items:
            source_name = {
                RetrievalSource.FIXED_KNOWLEDGE: "参考知识",
                RetrievalSource.USER_KNOWLEDGE: "我的规则",
                RetrievalSource.SEMANTIC_MEMORY: "相关资讯",
                RetrievalSource.EPISODIC_MEMORY: "历史分析",
                RetrievalSource.PROCEDURAL_MEMORY: "交易模式",
            }.get(item.source, "其他")
            
            if source_name not in sections:
                sections[source_name] = []
            
            sections[source_name].append(item.to_context_str())
        
        # 组装
        parts = []
        for section_name, contents in sections.items():
            parts.append(f"## {section_name}")
            parts.extend(contents)
            parts.append("")
        
        return "\n".join(parts)
    
    async def _generate(
        self,
        query: str,
        context: str,
        system_prompt: Optional[str],
        trace_id: Optional[str],
    ) -> str:
        """生成回答"""
        llm = await self._get_llm()
        
        default_system_prompt = """你是一个专业的股票分析助手。基于提供的上下文信息回答用户问题。

回答要求:
1. 准确引用上下文中的信息
2. 结合用户的个人规则给出建议
3. 如果上下文信息不足，诚实说明
4. 保持专业、客观的语气"""
        
        prompt = system_prompt or default_system_prompt
        
        if context:
            prompt += f"\n\n以下是相关上下文信息:\n\n{context}"
        
        try:
            response = await llm.chat([
                {"role": "system", "content": prompt},
                {"role": "user", "content": query},
            ])
            return response
        except Exception as e:
            self.logger.error(f"[{trace_id}] Generate failed: {e}")
            return f"抱歉，生成回答时发生错误: {str(e)}"
    
    def _extract_citations(
        self,
        response: str,
        items: List[RetrievalItem],
    ) -> List[Dict[str, Any]]:
        """提取引用"""
        # 简单实现：列出所有使用的来源
        citations = []
        
        for i, item in enumerate(items):
            citations.append({
                "index": i + 1,
                "source": item.source.value,
                "title": item.title,
                "metadata": item.metadata,
            })
        
        return citations
    
    # ==================== 便捷方法 ====================
    
    async def query_technical(
        self,
        user_id: str,
        query: str,
        category: Optional[FixedKnowledgeCategory] = None,
        trace_id: Optional[str] = None,
    ) -> RAGResult:
        """技术分析查询"""
        categories = [category] if category else [
            FixedKnowledgeCategory.TECH_CHIP_PEAK,
            FixedKnowledgeCategory.TECH_CANDLESTICK_SINGLE,
            FixedKnowledgeCategory.TECH_CANDLESTICK_DOUBLE,
            FixedKnowledgeCategory.TECH_CANDLESTICK_PATTERN,
            FixedKnowledgeCategory.TECH_MOVING_AVERAGE,
            FixedKnowledgeCategory.TECH_VOLUME,
        ]
        
        config = RetrievalConfig(
            use_fixed_knowledge=True,
            use_user_knowledge=True,
            use_semantic_memory=False,
            use_episodic_memory=False,
            fixed_categories=categories,
            fixed_top_k=8,
        )
        
        return await self.query(user_id, query, config, trace_id=trace_id)
    
    async def query_review(
        self,
        user_id: str,
        query: str,
        ts_code: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> RAGResult:
        """复盘查询"""
        categories = [
            FixedKnowledgeCategory.MARKET_REVIEW_OPEN,
            FixedKnowledgeCategory.MARKET_REVIEW_INTRADAY,
            FixedKnowledgeCategory.MARKET_REVIEW_CLOSE,
        ]
        
        config = RetrievalConfig(
            use_fixed_knowledge=True,
            use_user_knowledge=True,
            use_semantic_memory=True,
            use_episodic_memory=True,
            use_procedural_memory=True,
            fixed_categories=categories,
            ts_code=ts_code,
        )
        
        return await self.query(user_id, query, config, trace_id=trace_id)
    
    async def query_stock(
        self,
        user_id: str,
        query: str,
        ts_code: str,
        trace_id: Optional[str] = None,
    ) -> RAGResult:
        """个股分析查询"""
        config = RetrievalConfig(
            use_fixed_knowledge=True,
            use_user_knowledge=True,
            use_semantic_memory=True,
            use_episodic_memory=True,
            use_procedural_memory=False,
            ts_code=ts_code,
            semantic_top_k=8,
        )
        
        return await self.query(user_id, query, config, trace_id=trace_id)


# 全局实例
rag_pipeline = UnifiedRAGPipeline()
