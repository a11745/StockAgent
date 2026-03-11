"""
注意力门控

过滤感觉记忆，将重要信息传递到工作记忆。
"""

import logging
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime

from ..types import SensoryMemoryItem, WorkingMemoryItem, MemoryMetadata, MemoryType


class AttentionGate:
    """
    注意力门控
    
    过滤感觉记忆中的信息，只让与当前任务相关的内容进入工作记忆。
    
    过滤规则:
    1. 与用户关注股票相关
    2. 与当前任务相关
    3. 异常/重要事件检测
    4. 自定义过滤器
    
    Example:
        gate = AttentionGate()
        
        # 添加关注股票
        gate.set_watchlist(user_id, ["000001.SZ", "600000.SH"])
        
        # 过滤感觉记忆
        working_items = await gate.filter(sensory_items, user_id, task_context)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("src.memory.sensory.AttentionGate")
        
        # 用户关注列表缓存
        self._watchlists: Dict[str, set] = {}
        
        # 当前任务上下文
        self._task_contexts: Dict[str, Dict[str, Any]] = {}
        
        # 自定义过滤器
        self._custom_filters: List[Callable] = []
        
        # 重要性阈值
        self.importance_threshold = 0.3
    
    def set_watchlist(self, user_id: str, ts_codes: List[str]) -> None:
        """设置用户关注股票列表"""
        self._watchlists[user_id] = set(ts_codes)
    
    def add_to_watchlist(self, user_id: str, ts_code: str) -> None:
        """添加关注股票"""
        if user_id not in self._watchlists:
            self._watchlists[user_id] = set()
        self._watchlists[user_id].add(ts_code)
    
    def set_task_context(self, user_id: str, context: Dict[str, Any]) -> None:
        """设置当前任务上下文"""
        self._task_contexts[user_id] = context
    
    def add_filter(self, filter_func: Callable[[SensoryMemoryItem], float]) -> None:
        """
        添加自定义过滤器
        
        Args:
            filter_func: 过滤函数，返回重要性分数 [0, 1]
        """
        self._custom_filters.append(filter_func)
    
    async def filter(
        self,
        items: List[SensoryMemoryItem],
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> List[WorkingMemoryItem]:
        """
        过滤感觉记忆，转换为工作记忆
        
        Args:
            items: 感觉记忆项列表
            user_id: 用户ID
            trace_id: 追踪ID
            
        Returns:
            通过过滤的工作记忆项
        """
        if not items:
            return []
        
        watchlist = self._watchlists.get(user_id, set())
        task_context = self._task_contexts.get(user_id, {})
        
        passed_items = []
        
        for item in items:
            # 计算重要性分数
            importance = self._calculate_importance(item, watchlist, task_context)
            
            # 应用自定义过滤器
            for custom_filter in self._custom_filters:
                try:
                    custom_score = custom_filter(item)
                    importance = max(importance, custom_score)
                except Exception as e:
                    self.logger.warning(f"Custom filter error: {e}")
            
            # 检查是否通过阈值
            if importance >= self.importance_threshold:
                working_item = self._to_working_memory(item, importance, user_id)
                passed_items.append(working_item)
        
        self.logger.debug(
            f"[{trace_id}] Attention gate: {len(items)} -> {len(passed_items)} items"
        )
        
        return passed_items
    
    def _calculate_importance(
        self,
        item: SensoryMemoryItem,
        watchlist: set,
        task_context: Dict[str, Any],
    ) -> float:
        """计算感觉记忆项的重要性分数"""
        score = 0.0
        
        raw_data = item.raw_data or {}
        
        # 1. 关注股票匹配
        ts_code = raw_data.get("ts_code") or item.metadata.ts_code
        if ts_code and ts_code in watchlist:
            score = max(score, 0.8)
        
        # 2. 任务相关性
        task_ts_codes = task_context.get("ts_codes", [])
        if ts_code and ts_code in task_ts_codes:
            score = max(score, 0.9)
        
        task_keywords = task_context.get("keywords", [])
        content = item.content.lower()
        for keyword in task_keywords:
            if keyword.lower() in content:
                score = max(score, 0.7)
        
        # 3. 异常事件检测
        if self._is_abnormal_event(raw_data):
            score = max(score, 0.95)
        
        # 4. 涨跌停相关
        if raw_data.get("limit") in ["U", "D"]:  # 涨停/跌停
            score = max(score, 0.85)
        
        # 5. 价格变动幅度
        pct_chg = raw_data.get("pct_chg", 0)
        if abs(pct_chg) > 5:  # 涨跌幅超过 5%
            score = max(score, 0.6 + abs(pct_chg) / 100)
        
        return min(score, 1.0)
    
    def _is_abnormal_event(self, data: Dict[str, Any]) -> bool:
        """检测是否为异常事件"""
        # 停牌
        if data.get("trade_status") == "停牌":
            return True
        
        # 涨跌幅异常
        pct_chg = data.get("pct_chg", 0)
        if abs(pct_chg) > 9.9:
            return True
        
        # 成交量异常 (放量)
        vol_ratio = data.get("vol_ratio", 1)
        if vol_ratio > 3:
            return True
        
        return False
    
    def _to_working_memory(
        self,
        item: SensoryMemoryItem,
        importance: float,
        user_id: str,
    ) -> WorkingMemoryItem:
        """将感觉记忆转换为工作记忆"""
        return WorkingMemoryItem(
            id=f"wm_{item.id}",
            memory_type=MemoryType.WORKING,
            content=item.content,
            metadata=MemoryMetadata(
                user_id=user_id,
                session_id=item.metadata.session_id,
                importance_score=importance,
                ts_code=item.metadata.ts_code,
                ts_codes=item.metadata.ts_codes,
                source=item.metadata.source,
                category=item.metadata.category,
                tags=item.metadata.tags,
                created_at=item.metadata.created_at,
            ),
        )
    
    async def auto_filter_stream(
        self,
        stream: "SensoryStream",
        user_id: str,
        stream_type: str,
        callback: Callable[[List[WorkingMemoryItem]], None],
        trace_id: Optional[str] = None,
    ) -> None:
        """
        自动过滤流数据 (持续监听)
        
        Args:
            stream: 感觉记忆流
            user_id: 用户ID
            stream_type: 流类型
            callback: 通过过滤后的回调函数
            trace_id: 追踪ID
        """
        async for item in stream.subscribe(user_id, stream_type, trace_id):
            working_items = await self.filter([item], user_id, trace_id)
            if working_items:
                callback(working_items)
