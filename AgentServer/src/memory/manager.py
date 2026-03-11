"""
统一记忆管理器

提供记忆系统的统一入口，管理记忆的完整生命周期。
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import asyncio

from .types import (
    BaseMemoryItem,
    SensoryMemoryItem,
    WorkingMemoryItem,
    LongTermMemoryItem,
    LongTermMemoryType,
    MemoryType,
    MemoryMetadata,
    MemoryVisibility,
    InsertResult,
    ConsolidationResult,
    DecayResult,
)
from .sensory import SensoryStream, AttentionGate
from .working import WorkingBuffer, ContextWindow
from .longterm import SemanticStore, EpisodicStore, ProceduralStore, TradingPattern
from .consolidation import ConsolidationEngine
from .decay import DecayEngine
from .retrieval import UnifiedRetriever, RetrievalQuery, RetrievalResult


class MemoryManager:
    """
    统一记忆管理器
    
    作为记忆系统的统一入口，提供:
    1. 记忆的存储和检索
    2. 记忆的生命周期管理 (巩固、衰减)
    3. 交易体系分析
    4. 上下文管理
    
    Example:
        manager = MemoryManager()
        
        # 存储用户输入到感觉记忆
        await manager.sensory.push(user_id, "input", {"text": "分析茅台"})
        
        # 添加工作记忆
        await manager.add_to_working(user_id, content, importance=0.8)
        
        # 统一检索
        result = await manager.search(user_id, "茅台分析", ts_code="600519.SH")
        
        # 分析持仓
        suggestions = await manager.analyze_holdings(user_id, holdings)
        
        # 获取任务上下文
        context = await manager.get_context(user_id, task_description)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("src.memory.MemoryManager")
        
        # 感觉记忆
        self.sensory = SensoryStream()
        self.attention = AttentionGate()
        
        # 工作记忆
        self.working = WorkingBuffer()
        self.context = ContextWindow()
        
        # 长期记忆
        self.semantic = SemanticStore()
        self.episodic = EpisodicStore()
        self.procedural = ProceduralStore()
        
        # 记忆流程
        self.consolidation = ConsolidationEngine()
        self.decay = DecayEngine()
        self.retriever = UnifiedRetriever()
        
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    # ==================== 便捷方法 ====================
    
    async def add_to_working(
        self,
        user_id: str,
        content: str,
        importance: float = 0.5,
        ts_code: Optional[str] = None,
        category: Optional[str] = None,
        task_id: Optional[str] = None,
        is_conclusion: bool = False,
        trace_id: Optional[str] = None,
    ) -> bool:
        """
        添加内容到工作记忆
        
        Args:
            user_id: 用户ID
            content: 内容
            importance: 重要性 (0-1)
            ts_code: 相关股票代码
            category: 分类
            task_id: 任务ID
            is_conclusion: 是否为结论
            trace_id: 追踪ID
            
        Returns:
            是否成功
        """
        item = WorkingMemoryItem(
            content=content,
            task_id=task_id,
            is_conclusion=is_conclusion,
            metadata=MemoryMetadata(
                user_id=user_id,
                importance_score=importance,
                ts_code=ts_code,
                category=category,
            ),
        )
        
        return await self.working.add(user_id, item, trace_id)
    
    async def store_news(
        self,
        content: str,
        ts_code: str,
        publish_date: str,
        source: str,
        title: Optional[str] = None,
        importance: float = 0.5,
        trace_id: Optional[str] = None,
    ) -> InsertResult:
        """
        存储新闻到语义记忆
        
        Args:
            content: 新闻内容
            ts_code: 股票代码
            publish_date: 发布日期 (YYYYMMDD)
            source: 来源
            title: 标题
            importance: 重要性
            trace_id: 追踪ID
        """
        llm = await self._get_llm()
        
        # 生成向量
        text = f"{title}\n{content}" if title else content
        vectors = await llm.embedding([text])
        vector = vectors[0] if vectors else []
        
        item = LongTermMemoryItem(
            memory_type="long_term",
            subtype=LongTermMemoryType.SEMANTIC,
            content=text,
            vector=vector,
            metadata=MemoryMetadata(
                user_id="system",
                visibility=MemoryVisibility.PUBLIC,
                ts_code=ts_code,
                source=source,
                category="news",
                importance_score=importance,
                extra={"publish_date": publish_date, "title": title},
            ),
        )
        
        return await self.semantic.insert([item], trace_id)
    
    async def store_analysis(
        self,
        user_id: str,
        content: str,
        ts_code: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        category: str = "analysis",
        importance: float = 0.7,
        trace_id: Optional[str] = None,
    ) -> InsertResult:
        """
        存储分析记录到情景记忆
        
        Args:
            user_id: 用户ID
            content: 分析内容
            ts_code: 股票代码
            ts_codes: 股票代码列表
            category: 分类
            importance: 重要性
            trace_id: 追踪ID
        """
        llm = await self._get_llm()
        
        vectors = await llm.embedding([content])
        vector = vectors[0] if vectors else []
        
        item = LongTermMemoryItem(
            memory_type="long_term",
            subtype=LongTermMemoryType.EPISODIC,
            content=content,
            vector=vector,
            metadata=MemoryMetadata(
                user_id=user_id,
                visibility=MemoryVisibility.PRIVATE,
                ts_code=ts_code,
                ts_codes=ts_codes or [],
                category=category,
                importance_score=importance,
            ),
        )
        
        return await self.episodic.insert(user_id, [item], trace_id)
    
    async def search(
        self,
        user_id: str,
        query: str,
        ts_code: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        top_k: int = 10,
        include_working: bool = True,
        include_semantic: bool = True,
        include_episodic: bool = True,
        trace_id: Optional[str] = None,
    ) -> RetrievalResult:
        """
        统一检索
        
        Args:
            user_id: 用户ID
            query: 查询文本
            ts_code: 股票代码
            ts_codes: 股票代码列表
            top_k: 返回数量
            include_working: 包含工作记忆
            include_semantic: 包含语义记忆
            include_episodic: 包含情景记忆
            trace_id: 追踪ID
            
        Returns:
            检索结果
        """
        retrieval_query = RetrievalQuery(
            text=query,
            user_id=user_id,
            ts_code=ts_code,
            ts_codes=ts_codes,
            top_k=top_k,
            include_working=include_working,
            include_semantic=include_semantic,
            include_episodic=include_episodic,
        )
        
        return await self.retriever.search(retrieval_query, trace_id)
    
    async def get_context(
        self,
        user_id: str,
        task_description: str,
        ts_codes: Optional[List[str]] = None,
        max_items: int = 10,
        trace_id: Optional[str] = None,
    ) -> str:
        """获取任务上下文"""
        return await self.retriever.get_context_for_task(
            user_id=user_id,
            task_description=task_description,
            ts_codes=ts_codes,
            max_items=max_items,
            trace_id=trace_id,
        )
    
    # ==================== 交易体系 ====================
    
    async def record_trade(
        self,
        user_id: str,
        ts_code: str,
        direction: str,
        entry_price: float,
        entry_date: str,
        entry_reason: str,
        quantity: int,
        ts_name: Optional[str] = None,
        pattern_ids: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
    ) -> bool:
        """记录一笔交易"""
        from .longterm.procedural import TradeRecord
        
        trade = TradeRecord(
            user_id=user_id,
            ts_code=ts_code,
            ts_name=ts_name,
            direction=direction,
            entry_price=entry_price,
            entry_date=entry_date,
            entry_reason=entry_reason,
            quantity=quantity,
            pattern_ids=pattern_ids or [],
        )
        
        return await self.procedural.record_trade(trade, trace_id)
    
    async def close_trade(
        self,
        user_id: str,
        trade_id: str,
        exit_price: float,
        exit_date: str,
        exit_reason: str,
        lessons_learned: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """关闭交易"""
        result = await self.procedural.close_trade(
            trade_id=trade_id,
            user_id=user_id,
            exit_price=exit_price,
            exit_date=exit_date,
            exit_reason=exit_reason,
            lessons_learned=lessons_learned,
            trace_id=trace_id,
        )
        
        if result:
            return result.model_dump()
        return None
    
    async def analyze_holdings(
        self,
        user_id: str,
        holdings: List[Dict[str, Any]],
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        分析用户持仓，提供交易体系建议
        
        Args:
            user_id: 用户ID
            holdings: 持仓列表
            trace_id: 追踪ID
            
        Returns:
            分析结果
        """
        return await self.procedural.analyze_holdings(user_id, holdings, trace_id)
    
    async def get_trading_patterns(
        self,
        user_id: str,
        pattern_type: Optional[str] = None,
        min_confidence: float = 0.0,
        trace_id: Optional[str] = None,
    ) -> List[Dict]:
        """获取用户的交易模式"""
        from .longterm.procedural import PatternType
        
        pt = PatternType(pattern_type) if pattern_type else None
        
        patterns = await self.procedural.get_patterns(
            user_id=user_id,
            pattern_type=pt,
            min_confidence=min_confidence,
            trace_id=trace_id,
        )
        
        return [p.model_dump() for p in patterns]
    
    async def learn_patterns(
        self,
        user_id: str,
        min_samples: int = 5,
        trace_id: Optional[str] = None,
    ) -> List[Dict]:
        """从历史交易中学习模式"""
        patterns = await self.procedural.learn_patterns_from_trades(
            user_id=user_id,
            min_samples=min_samples,
            trace_id=trace_id,
        )
        
        return [p.model_dump() for p in patterns]
    
    async def get_entry_suggestions(
        self,
        user_id: str,
        ts_code: str,
        current_price: float,
        market_data: Dict[str, Any],
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取入场建议"""
        return await self.procedural.get_entry_suggestions(
            user_id=user_id,
            ts_code=ts_code,
            current_price=current_price,
            market_data=market_data,
            trace_id=trace_id,
        )
    
    # ==================== 生命周期管理 ====================
    
    async def consolidate(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> ConsolidationResult:
        """执行记忆巩固"""
        return await self.consolidation.consolidate(user_id, trace_id=trace_id)
    
    async def consolidate_conversation(
        self,
        user_id: str,
        session_id: str,
        messages: List[Dict[str, str]],
        trace_id: Optional[str] = None,
    ) -> ConsolidationResult:
        """巩固对话到长期记忆"""
        return await self.consolidation.consolidate_conversation(
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            trace_id=trace_id,
        )
    
    async def run_decay(
        self,
        user_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> DecayResult:
        """执行记忆衰减"""
        return await self.decay.run_decay(user_id, trace_id)
    
    async def boost_memory(
        self,
        user_id: str,
        item_id: str,
        collection: str = "episodic_memory",
        trace_id: Optional[str] = None,
    ) -> bool:
        """提升记忆重要性 (被访问时调用)"""
        return await self.decay.boost_importance(user_id, item_id, collection, trace_id=trace_id)
    
    async def mark_permanent(
        self,
        user_id: str,
        item_id: str,
        collection: str = "episodic_memory",
        trace_id: Optional[str] = None,
    ) -> bool:
        """标记记忆为永久保留"""
        return await self.decay.mark_as_permanent(user_id, item_id, collection, trace_id)
    
    # ==================== 上下文管理 ====================
    
    async def add_message(
        self,
        user_id: str,
        role: str,
        content: str,
        session_id: str = "default",
        trace_id: Optional[str] = None,
    ) -> bool:
        """添加对话消息"""
        return await self.context.add_message(user_id, role, content, session_id, trace_id)
    
    async def build_llm_messages(
        self,
        user_id: str,
        system_prompt: str,
        session_id: str = "default",
        include_working_memory: bool = True,
        trace_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """构建 LLM 消息列表"""
        working_items = []
        
        if include_working_memory:
            working_items = await self.working.get_all(user_id, trace_id)
        
        return await self.context.build_messages(
            user_id=user_id,
            working_items=working_items,
            system_prompt=system_prompt,
            session_id=session_id,
            trace_id=trace_id,
        )
    
    async def clear_session(
        self,
        user_id: str,
        session_id: str = "default",
        clear_working: bool = True,
        trace_id: Optional[str] = None,
    ) -> bool:
        """清空会话"""
        success = await self.context.clear_session(user_id, session_id, trace_id)
        
        if clear_working:
            await self.working.clear(user_id, trace_id)
        
        return success
    
    # ==================== 注意力机制 ====================
    
    def set_watchlist(self, user_id: str, ts_codes: List[str]) -> None:
        """设置用户关注股票列表"""
        self.attention.set_watchlist(user_id, ts_codes)
    
    def set_task_context(self, user_id: str, context: Dict[str, Any]) -> None:
        """设置当前任务上下文"""
        self.attention.set_task_context(user_id, context)
    
    async def filter_sensory_to_working(
        self,
        user_id: str,
        items: List[SensoryMemoryItem],
        trace_id: Optional[str] = None,
    ) -> List[WorkingMemoryItem]:
        """过滤感觉记忆到工作记忆"""
        working_items = await self.attention.filter(items, user_id, trace_id)
        
        # 添加到工作缓冲区
        for item in working_items:
            await self.working.add(user_id, item, trace_id)
        
        return working_items


# 全局单例
memory_manager = MemoryManager()
