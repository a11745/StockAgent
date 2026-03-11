"""
记忆巩固

将工作记忆转化为长期记忆的过程。

巩固策略:
1. 重要性筛选: 只巩固重要性超过阈值的记忆
2. 分类存储: 根据内容类型分配到语义/情景/程序性记忆
3. 去重合并: 避免重复存储相似内容
4. 关系建立: 建立新记忆与已有记忆的关联
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .types import (
    WorkingMemoryItem,
    LongTermMemoryItem,
    LongTermMemoryType,
    MemoryMetadata,
    MemoryVisibility,
    ConsolidationResult,
)
from .working import WorkingBuffer
from .longterm import SemanticStore, EpisodicStore, ProceduralStore


class ConsolidationEngine:
    """
    记忆巩固引擎
    
    将工作记忆转化为长期记忆，模拟人脑的记忆巩固过程。
    
    巩固流程:
    1. 从工作记忆中筛选高重要性项
    2. 分析内容类型 (语义/情景/程序性)
    3. 检查是否已存在相似记忆
    4. 存储到对应的长期记忆库
    5. 建立记忆之间的关联
    
    Args:
        min_importance: 最小重要性阈值
        similarity_threshold: 去重相似度阈值
    
    Example:
        engine = ConsolidationEngine()
        
        # 手动触发巩固
        result = await engine.consolidate(user_id)
        
        # 或者定时巩固
        await engine.schedule_consolidation(interval_seconds=300)
    """
    
    def __init__(
        self,
        min_importance: float = 0.6,
        similarity_threshold: float = 0.85,
    ):
        self.min_importance = min_importance
        self.similarity_threshold = similarity_threshold
        self.logger = logging.getLogger("src.memory.ConsolidationEngine")
        
        # 存储组件
        self.working_buffer = WorkingBuffer()
        self.semantic_store = SemanticStore()
        self.episodic_store = EpisodicStore()
        self.procedural_store = ProceduralStore()
        
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    async def consolidate(
        self,
        user_id: str,
        session_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> ConsolidationResult:
        """
        执行记忆巩固
        
        Args:
            user_id: 用户ID
            session_id: 会话ID (可选，用于筛选特定会话的记忆)
            trace_id: 追踪ID
            
        Returns:
            巩固结果
        """
        result = ConsolidationResult()
        
        try:
            # 1. 获取待巩固的工作记忆
            candidates = await self.working_buffer.get_for_consolidation(
                user_id, 
                min_importance=self.min_importance,
                trace_id=trace_id,
            )
            
            if not candidates:
                self.logger.debug(f"[{trace_id}] No memories to consolidate")
                return result
            
            self.logger.info(f"[{trace_id}] Found {len(candidates)} candidates for consolidation")
            
            # 2. 分类并存储
            for item in candidates:
                memory_type = await self._classify_memory(item, trace_id)
                
                if memory_type == LongTermMemoryType.SEMANTIC:
                    success = await self._consolidate_to_semantic(user_id, item, trace_id)
                    if success:
                        result.to_semantic += 1
                        
                elif memory_type == LongTermMemoryType.EPISODIC:
                    success = await self._consolidate_to_episodic(user_id, item, trace_id)
                    if success:
                        result.to_episodic += 1
                        
                elif memory_type == LongTermMemoryType.PROCEDURAL:
                    pattern = await self._extract_pattern(user_id, item, trace_id)
                    if pattern:
                        result.to_procedural += 1
                        result.patterns_detected.append(pattern)
                
                if success:
                    result.consolidated_count += 1
                    # 从工作记忆中移除已巩固的项
                    await self.working_buffer.remove(user_id, item.id, trace_id)
            
            self.logger.info(
                f"[{trace_id}] Consolidated {result.consolidated_count} memories: "
                f"semantic={result.to_semantic}, episodic={result.to_episodic}, "
                f"procedural={result.to_procedural}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Consolidation failed: {e}")
            return result
    
    async def _classify_memory(
        self,
        item: WorkingMemoryItem,
        trace_id: Optional[str] = None,
    ) -> LongTermMemoryType:
        """
        分类记忆类型
        
        分类规则:
        - 语义记忆: 通用知识、事实信息、新闻内容
        - 情景记忆: 个人经历、分析记录、对话历史
        - 程序性记忆: 交易决策、策略模式、操作方法
        """
        content = item.content.lower()
        category = item.metadata.category or ""
        
        # 关键词匹配
        semantic_keywords = ["新闻", "公告", "研报", "行业", "概念", "政策", "财报"]
        procedural_keywords = ["买入", "卖出", "止损", "止盈", "策略", "仓位", "入场", "出场"]
        
        # 检查程序性
        for kw in procedural_keywords:
            if kw in content or kw in category:
                return LongTermMemoryType.PROCEDURAL
        
        # 检查语义性
        for kw in semantic_keywords:
            if kw in content or kw in category:
                return LongTermMemoryType.SEMANTIC
        
        # 默认为情景记忆
        return LongTermMemoryType.EPISODIC
    
    async def _consolidate_to_semantic(
        self,
        user_id: str,
        item: WorkingMemoryItem,
        trace_id: Optional[str] = None,
    ) -> bool:
        """巩固到语义记忆"""
        try:
            llm = await self._get_llm()
            
            # 生成向量
            vectors = await llm.embedding([item.content])
            vector = vectors[0] if vectors else []
            
            # 检查重复
            if vector:
                # 这里可以添加去重逻辑
                pass
            
            # 创建长期记忆项
            long_term_item = LongTermMemoryItem(
                id=f"ltm_sem_{item.id}",
                memory_type="long_term",
                subtype=LongTermMemoryType.SEMANTIC,
                content=item.content,
                vector=vector,
                metadata=item.metadata,
            )
            
            # 公共新闻/公告设为公开
            if item.metadata.source in ["新闻", "公告", "研报"]:
                long_term_item.metadata.visibility = MemoryVisibility.PUBLIC
            
            result = await self.semantic_store.insert([long_term_item], trace_id)
            return result.success and len(result.inserted_ids) > 0
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Consolidate to semantic failed: {e}")
            return False
    
    async def _consolidate_to_episodic(
        self,
        user_id: str,
        item: WorkingMemoryItem,
        trace_id: Optional[str] = None,
    ) -> bool:
        """巩固到情景记忆"""
        try:
            llm = await self._get_llm()
            
            # 生成向量
            vectors = await llm.embedding([item.content])
            vector = vectors[0] if vectors else []
            
            # 创建长期记忆项
            long_term_item = LongTermMemoryItem(
                id=f"ltm_epi_{item.id}",
                memory_type="long_term",
                subtype=LongTermMemoryType.EPISODIC,
                content=item.content,
                vector=vector,
                metadata=item.metadata,
            )
            
            # 情景记忆始终私有
            long_term_item.metadata.visibility = MemoryVisibility.PRIVATE
            
            result = await self.episodic_store.insert(user_id, [long_term_item], trace_id)
            return result.success and len(result.inserted_ids) > 0
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Consolidate to episodic failed: {e}")
            return False
    
    async def _extract_pattern(
        self,
        user_id: str,
        item: WorkingMemoryItem,
        trace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        从工作记忆中提取交易模式
        
        Returns:
            提取的模式名称，如果未提取到则返回 None
        """
        try:
            llm = await self._get_llm()
            
            # 使用 LLM 分析是否包含可学习的模式
            prompt = f"""分析以下内容，判断是否包含可学习的交易模式：

内容: {item.content}

如果包含交易模式（入场策略、出场策略、风险管理等），请提取：
1. 模式名称
2. 模式类型 (entry/exit/risk_management/position_sizing)
3. 触发条件
4. 执行动作

如果不包含可学习的模式，返回 null。

输出 JSON 格式：
```json
{{
  "has_pattern": true/false,
  "pattern": {{
    "name": "模式名称",
    "type": "entry",
    "conditions": ["条件"],
    "actions": ["动作"],
    "description": "描述"
  }}
}}
```"""
            
            response = await llm.chat([
                {"role": "system", "content": "你是交易模式识别专家。"},
                {"role": "user", "content": prompt},
            ])
            
            # 解析响应
            import json
            import re
            
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(1))
                
                if data.get("has_pattern") and data.get("pattern"):
                    from .longterm.procedural import TradingPattern, PatternType
                    
                    p = data["pattern"]
                    pattern = TradingPattern(
                        user_id=user_id,
                        pattern_type=PatternType(p.get("type", "entry")),
                        name=p.get("name", "未命名"),
                        description=p.get("description", ""),
                        conditions=p.get("conditions", []),
                        actions=p.get("actions", []),
                        confidence=0.3,
                    )
                    
                    await self.procedural_store.create_pattern(pattern, trace_id)
                    return pattern.name
            
            return None
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Extract pattern failed: {e}")
            return None
    
    async def consolidate_conversation(
        self,
        user_id: str,
        session_id: str,
        messages: List[Dict[str, str]],
        trace_id: Optional[str] = None,
    ) -> ConsolidationResult:
        """
        巩固一段对话到长期记忆
        
        Args:
            user_id: 用户ID
            session_id: 会话ID
            messages: 对话消息列表
            trace_id: 追踪ID
            
        Returns:
            巩固结果
        """
        result = ConsolidationResult()
        
        if not messages:
            return result
        
        try:
            llm = await self._get_llm()
            
            # 构建对话文本
            conversation = "\n".join([
                f"{m['role']}: {m['content']}"
                for m in messages
            ])
            
            # 使用 LLM 提取要点
            extract_prompt = f"""请从以下对话中提取值得记住的要点：

{conversation}

请提取：
1. 关键结论和决策
2. 重要的市场观点
3. 交易相关的建议
4. 用户表达的偏好

输出 JSON 格式：
```json
{{
  "key_points": [
    {{"content": "要点内容", "type": "conclusion/opinion/suggestion/preference", "importance": 0.8}}
  ],
  "related_stocks": ["股票代码"],
  "summary": "对话摘要"
}}
```"""
            
            response = await llm.chat([
                {"role": "system", "content": "你是对话分析专家，擅长提取关键信息。"},
                {"role": "user", "content": extract_prompt},
            ])
            
            # 解析并存储
            import json
            import re
            
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(1))
                
                for point in data.get("key_points", []):
                    item = LongTermMemoryItem(
                        memory_type="long_term",
                        subtype=LongTermMemoryType.EPISODIC,
                        content=point["content"],
                        metadata=MemoryMetadata(
                            user_id=user_id,
                            session_id=session_id,
                            importance_score=point.get("importance", 0.5),
                            category=point.get("type", "conclusion"),
                            ts_codes=data.get("related_stocks", []),
                        ),
                    )
                    
                    store_result = await self.episodic_store.insert(
                        user_id, [item], trace_id
                    )
                    
                    if store_result.success:
                        result.to_episodic += 1
                        result.consolidated_count += 1
                
                # 存储摘要
                if data.get("summary"):
                    summary_item = LongTermMemoryItem(
                        memory_type="long_term",
                        subtype=LongTermMemoryType.EPISODIC,
                        content=f"[对话摘要] {data['summary']}",
                        metadata=MemoryMetadata(
                            user_id=user_id,
                            session_id=session_id,
                            importance_score=0.7,
                            category="summary",
                        ),
                    )
                    await self.episodic_store.insert(user_id, [summary_item], trace_id)
            
            self.logger.info(
                f"[{trace_id}] Consolidated conversation: {result.consolidated_count} items"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Consolidate conversation failed: {e}")
            return result
