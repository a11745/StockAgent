"""
上下文窗口

管理当前任务的上下文信息，支持 LLM 调用的上下文组装。
"""

import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..types import WorkingMemoryItem


class ContextWindow:
    """
    上下文窗口
    
    管理当前任务的对话历史和相关信息，用于 LLM 调用。
    
    功能:
    1. 维护对话历史
    2. 组装 LLM 上下文
    3. 控制上下文长度
    4. 支持多轮对话的记忆滑动
    
    Args:
        max_tokens: 最大 token 数
        max_turns: 最大对话轮数
    
    Example:
        context = ContextWindow(max_tokens=4000)
        
        # 添加对话
        context.add_message(user_id, "user", "分析一下 000001.SZ")
        context.add_message(user_id, "assistant", "好的，我来分析...")
        
        # 组装 LLM 上下文
        messages = context.build_messages(user_id, working_items, system_prompt)
    """
    
    def __init__(
        self,
        context_prefix: str = "context",
        max_tokens: int = 4000,
        max_turns: int = 10,
        default_ttl_seconds: int = 3600,  # 1 小时
    ):
        self.context_prefix = context_prefix
        self.max_tokens = max_tokens
        self.max_turns = max_turns
        self.default_ttl_seconds = default_ttl_seconds
        self.logger = logging.getLogger("src.memory.working.ContextWindow")
        self._redis_manager = None
        
        # 简单的 token 估算 (中文约 2 字符/token，英文约 4 字符/token)
        self.avg_chars_per_token = 3
    
    async def _get_redis(self):
        """延迟导入 redis_manager"""
        if self._redis_manager is None:
            from core.managers import redis_manager
            self._redis_manager = redis_manager
        return self._redis_manager
    
    def _build_history_key(self, user_id: str, session_id: str = "default") -> str:
        """构建对话历史键名"""
        return f"{self.context_prefix}:history:{user_id}:{session_id}"
    
    def _estimate_tokens(self, text: str) -> int:
        """估算文本的 token 数"""
        return len(text) // self.avg_chars_per_token
    
    async def add_message(
        self,
        user_id: str,
        role: str,
        content: str,
        session_id: str = "default",
        trace_id: Optional[str] = None,
    ) -> bool:
        """
        添加对话消息
        
        Args:
            user_id: 用户ID
            role: 角色 (user/assistant/system)
            content: 消息内容
            session_id: 会话ID
            trace_id: 追踪ID
        """
        redis = await self._get_redis()
        client = redis._client
        
        history_key = self._build_history_key(user_id, session_id)
        
        try:
            message = {
                "role": role,
                "content": content,
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            # 添加到列表末尾
            await client.rpush(history_key, json.dumps(message, ensure_ascii=False))
            
            # 修剪列表，保留最近的 N 条
            await client.ltrim(history_key, -self.max_turns * 2, -1)
            
            # 设置 TTL
            await client.expire(history_key, self.default_ttl_seconds)
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Add message failed: {e}")
            return False
    
    async def get_history(
        self,
        user_id: str,
        session_id: str = "default",
        limit: Optional[int] = None,
        trace_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """获取对话历史"""
        redis = await self._get_redis()
        client = redis._client
        
        history_key = self._build_history_key(user_id, session_id)
        
        try:
            count = limit or self.max_turns * 2
            raw_messages = await client.lrange(history_key, -count, -1)
            
            messages = []
            for raw in raw_messages:
                msg = json.loads(raw)
                messages.append({
                    "role": msg["role"],
                    "content": msg["content"],
                })
            
            return messages
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get history failed: {e}")
            return []
    
    async def build_messages(
        self,
        user_id: str,
        working_items: List[WorkingMemoryItem],
        system_prompt: str,
        session_id: str = "default",
        trace_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        构建 LLM 调用的消息列表
        
        Args:
            user_id: 用户ID
            working_items: 工作记忆项 (作为额外上下文)
            system_prompt: 系统提示词
            session_id: 会话ID
            trace_id: 追踪ID
            
        Returns:
            LLM 消息列表
        """
        messages = []
        used_tokens = 0
        
        # 1. System prompt
        messages.append({"role": "system", "content": system_prompt})
        used_tokens += self._estimate_tokens(system_prompt)
        
        # 2. 工作记忆上下文
        if working_items:
            context_text = self._format_working_memory(working_items)
            context_tokens = self._estimate_tokens(context_text)
            
            # 如果上下文太长，截断
            if used_tokens + context_tokens < self.max_tokens * 0.4:
                messages.append({
                    "role": "system",
                    "content": f"相关上下文信息:\n{context_text}",
                })
                used_tokens += context_tokens
        
        # 3. 对话历史
        history = await self.get_history(user_id, session_id, trace_id=trace_id)
        
        # 从最新向最旧添加，直到达到 token 限制
        selected_history = []
        for msg in reversed(history):
            msg_tokens = self._estimate_tokens(msg["content"])
            if used_tokens + msg_tokens < self.max_tokens:
                selected_history.insert(0, msg)
                used_tokens += msg_tokens
            else:
                break
        
        messages.extend(selected_history)
        
        self.logger.debug(
            f"[{trace_id}] Built context: {len(messages)} messages, ~{used_tokens} tokens"
        )
        
        return messages
    
    def _format_working_memory(
        self,
        items: List[WorkingMemoryItem],
    ) -> str:
        """格式化工作记忆为上下文文本"""
        if not items:
            return ""
        
        sections = []
        
        # 按重要性排序
        sorted_items = sorted(
            items,
            key=lambda x: x.metadata.importance_score,
            reverse=True,
        )
        
        for item in sorted_items[:5]:  # 最多 5 条
            meta = item.metadata
            
            # 构建元信息
            meta_parts = []
            if meta.ts_code:
                meta_parts.append(f"股票: {meta.ts_code}")
            if meta.source:
                meta_parts.append(f"来源: {meta.source}")
            if meta.category:
                meta_parts.append(f"类型: {meta.category}")
            
            meta_str = " | ".join(meta_parts) if meta_parts else ""
            
            if meta_str:
                sections.append(f"[{meta_str}]\n{item.content}")
            else:
                sections.append(item.content)
        
        return "\n\n---\n\n".join(sections)
    
    async def clear_session(
        self,
        user_id: str,
        session_id: str = "default",
        trace_id: Optional[str] = None,
    ) -> bool:
        """清空会话历史"""
        redis = await self._get_redis()
        client = redis._client
        
        history_key = self._build_history_key(user_id, session_id)
        
        try:
            await client.delete(history_key)
            return True
        except Exception as e:
            self.logger.error(f"[{trace_id}] Clear session failed: {e}")
            return False
    
    async def summarize_and_compress(
        self,
        user_id: str,
        session_id: str = "default",
        trace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        总结并压缩对话历史
        
        当对话历史过长时，使用 LLM 进行总结压缩。
        
        Returns:
            总结文本
        """
        history = await self.get_history(user_id, session_id, trace_id=trace_id)
        
        if len(history) < 6:  # 少于 6 条不需要压缩
            return None
        
        # TODO: 调用 LLM 进行总结
        # 这里先返回简单的拼接
        from core.managers import llm_manager
        
        history_text = "\n".join([
            f"{msg['role']}: {msg['content']}"
            for msg in history[:-2]  # 保留最后 2 条
        ])
        
        try:
            summary_prompt = f"""请简洁总结以下对话的要点，保留关键信息:

{history_text}

总结要点:"""
            
            summary = await llm_manager.chat([
                {"role": "system", "content": "你是一个对话总结助手，擅长提取关键信息。"},
                {"role": "user", "content": summary_prompt},
            ])
            
            # 替换历史为总结 + 最后 2 条
            redis = await self._get_redis()
            client = redis._client
            history_key = self._build_history_key(user_id, session_id)
            
            await client.delete(history_key)
            await self.add_message(user_id, "system", f"[对话总结]: {summary}", session_id)
            
            for msg in history[-2:]:
                await self.add_message(user_id, msg["role"], msg["content"], session_id)
            
            self.logger.info(f"[{trace_id}] Compressed history to summary")
            return summary
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Summarize failed: {e}")
            return None
