"""
知识助手

通过对话帮助用户整理和构建交易体系。
"""

import logging
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from ..types import (
    UserKnowledgeItem,
    UserKnowledgeType,
)
from .store import UserKnowledgeStore


class KnowledgeAssistant:
    """
    知识助手
    
    通过对话帮助用户:
    1. 整理交易规则
    2. 总结交易心得
    3. 创建复盘模板
    4. 学习笔记管理
    
    Example:
        assistant = KnowledgeAssistant()
        
        # 从对话中提取规则
        result = await assistant.extract_rules_from_conversation(
            user_id="user1",
            messages=[
                {"role": "user", "content": "我一般会在股价突破年线并且放量的时候入场"},
                {"role": "assistant", "content": "明白，这是一个基于技术突破的入场策略..."},
            ],
        )
        
        # 帮助用户整理交易规则
        result = await assistant.organize_trading_rule(
            user_id="user1",
            raw_input="我喜欢在早盘低开高走的时候买入，仓位一般是三分之一",
        )
        
        # 生成复盘模板
        template = await assistant.generate_review_template(
            user_id="user1",
            style="detailed",  # brief/standard/detailed
        )
    """
    
    def __init__(
        self,
        user_store: Optional[UserKnowledgeStore] = None,
    ):
        self.user_store = user_store or UserKnowledgeStore()
        self.logger = logging.getLogger("src.rag.knowledge.KnowledgeAssistant")
        self._llm_manager = None
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    # ==================== 规则提取 ====================
    
    async def extract_rules_from_conversation(
        self,
        user_id: str,
        messages: List[Dict[str, str]],
        auto_save: bool = False,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        从对话中提取交易规则
        
        Args:
            user_id: 用户ID
            messages: 对话消息列表
            auto_save: 是否自动保存提取的规则
            trace_id: 追踪ID
            
        Returns:
            提取结果，包含识别的规则列表
        """
        llm = await self._get_llm()
        
        # 构建对话文本
        conversation = "\n".join([
            f"{m['role']}: {m['content']}"
            for m in messages
        ])
        
        prompt = f"""分析以下对话，提取用户的交易规则和习惯。

对话内容:
{conversation}

请识别以下类型的规则:
1. 入场规则 (什么条件下买入)
2. 出场规则 (什么条件下卖出)
3. 仓位规则 (如何分配仓位)
4. 风控规则 (如何控制风险)

输出 JSON 格式:
```json
{{
  "rules": [
    {{
      "type": "entry/exit/position/risk",
      "title": "规则标题",
      "conditions": ["条件1", "条件2"],
      "actions": ["动作1", "动作2"],
      "description": "规则描述",
      "confidence": 0.8
    }}
  ],
  "insights": ["发现的交易习惯或特点"],
  "suggestions": ["建议补充的规则"]
}}
```

如果没有发现明确的规则，返回空数组。"""
        
        try:
            response = await llm.chat([
                {"role": "system", "content": "你是交易规则分析专家，擅长从对话中提取结构化的交易规则。"},
                {"role": "user", "content": prompt},
            ])
            
            # 解析响应
            result = self._parse_json_response(response)
            
            if not result:
                result = {"rules": [], "insights": [], "suggestions": []}
            
            # 转换为 UserKnowledgeItem
            items = []
            for rule in result.get("rules", []):
                if rule.get("confidence", 0) < 0.5:
                    continue
                
                knowledge_type = self._map_rule_type(rule.get("type", ""))
                
                item = UserKnowledgeItem(
                    user_id=user_id,
                    knowledge_type=knowledge_type,
                    title=rule.get("title", ""),
                    content=rule.get("description", ""),
                    conditions=rule.get("conditions", []),
                    actions=rule.get("actions", []),
                    tags=[rule.get("type", "")],
                    source="ai_assisted",
                )
                items.append(item)
            
            # 自动保存
            saved_ids = []
            if auto_save and items:
                for item in items:
                    success = await self.user_store.create(item, trace_id)
                    if success:
                        saved_ids.append(item.id)
            
            self.logger.info(
                f"[{trace_id}] Extracted {len(items)} rules from conversation"
            )
            
            return {
                "rules": [item.model_dump() for item in items],
                "insights": result.get("insights", []),
                "suggestions": result.get("suggestions", []),
                "saved_ids": saved_ids,
            }
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Extract rules failed: {e}")
            return {"rules": [], "insights": [], "suggestions": [], "error": str(e)}
    
    async def organize_trading_rule(
        self,
        user_id: str,
        raw_input: str,
        rule_type: Optional[str] = None,
        auto_save: bool = False,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        帮助用户整理单条交易规则
        
        Args:
            user_id: 用户ID
            raw_input: 用户的原始描述
            rule_type: 规则类型 (entry/exit/position/risk)，可自动推断
            auto_save: 是否自动保存
            trace_id: 追踪ID
            
        Returns:
            整理后的规则
        """
        llm = await self._get_llm()
        
        # 获取用户已有规则作为参考
        existing_rules = await self.user_store.get_trading_rules(user_id, trace_id=trace_id)
        existing_summary = ""
        if existing_rules:
            existing_summary = "\n用户已有的规则:\n" + "\n".join([
                f"- {r.title}: {r.content[:100]}"
                for r in existing_rules[:5]
            ])
        
        prompt = f"""帮助用户整理交易规则。

用户描述:
{raw_input}
{existing_summary}

请将用户的描述整理成结构化的交易规则:
1. 明确的触发条件
2. 具体的执行动作
3. 清晰的标题和描述

输出 JSON 格式:
```json
{{
  "rule": {{
    "type": "entry/exit/position/risk",
    "title": "规则标题 (简洁)",
    "description": "规则的完整描述",
    "conditions": ["条件1 (具体可执行)", "条件2"],
    "actions": ["动作1 (具体可执行)", "动作2"],
    "tags": ["标签1", "标签2"],
    "notes": "补充说明或注意事项"
  }},
  "clarification_needed": ["如果有歧义，列出需要用户澄清的问题"],
  "related_rules": ["与用户已有规则的关系说明"]
}}
```"""
        
        try:
            response = await llm.chat([
                {"role": "system", "content": "你是交易规则整理专家，帮助用户将模糊的交易想法整理成清晰可执行的规则。"},
                {"role": "user", "content": prompt},
            ])
            
            result = self._parse_json_response(response)
            
            if not result or "rule" not in result:
                return {"error": "无法解析规则", "raw_response": response}
            
            rule_data = result["rule"]
            
            # 创建 UserKnowledgeItem
            item = UserKnowledgeItem(
                user_id=user_id,
                knowledge_type=self._map_rule_type(rule_data.get("type", rule_type or "entry")),
                title=rule_data.get("title", ""),
                content=rule_data.get("description", ""),
                conditions=rule_data.get("conditions", []),
                actions=rule_data.get("actions", []),
                tags=rule_data.get("tags", []),
                source="ai_assisted",
            )
            
            # 自动保存
            saved = False
            if auto_save:
                saved = await self.user_store.create(item, trace_id)
            
            return {
                "rule": item.model_dump(),
                "clarification_needed": result.get("clarification_needed", []),
                "related_rules": result.get("related_rules", []),
                "notes": rule_data.get("notes", ""),
                "saved": saved,
                "item_id": item.id if saved else None,
            }
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Organize rule failed: {e}")
            return {"error": str(e)}
    
    # ==================== 复盘模板 ====================
    
    async def generate_review_template(
        self,
        user_id: str,
        style: str = "standard",
        focus_areas: Optional[List[str]] = None,
        auto_save: bool = False,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        生成复盘模板
        
        Args:
            user_id: 用户ID
            style: 模板风格 (brief/standard/detailed)
            focus_areas: 重点关注领域
            auto_save: 是否自动保存
            trace_id: 追踪ID
            
        Returns:
            复盘模板
        """
        llm = await self._get_llm()
        
        # 获取用户的交易规则作为参考
        rules = await self.user_store.get_trading_rules(user_id, trace_id=trace_id)
        rules_summary = ""
        if rules:
            rules_summary = "\n用户的交易规则:\n" + "\n".join([
                f"- {r.title}"
                for r in rules[:10]
            ])
        
        focus_str = ""
        if focus_areas:
            focus_str = f"\n重点关注: {', '.join(focus_areas)}"
        
        style_desc = {
            "brief": "简洁版，只包含核心检查项，适合快速复盘",
            "standard": "标准版，包含完整的复盘流程",
            "detailed": "详细版，包含深入分析和反思环节",
        }
        
        prompt = f"""为用户生成交易复盘模板。

风格要求: {style_desc.get(style, style_desc['standard'])}
{focus_str}
{rules_summary}

请生成一个实用的复盘模板，包含:
1. 交易基本信息记录
2. 入场分析
3. 持仓期间分析
4. 出场分析
5. 经验总结
6. 改进计划

输出 JSON 格式:
```json
{{
  "template": {{
    "title": "模板标题",
    "description": "模板说明",
    "sections": [
      {{
        "name": "章节名",
        "questions": ["问题1", "问题2"],
        "checklist": ["检查项1", "检查项2"]
      }}
    ]
  }},
  "usage_tips": ["使用建议"]
}}
```"""
        
        try:
            response = await llm.chat([
                {"role": "system", "content": "你是交易复盘专家，帮助用户建立系统化的复盘流程。"},
                {"role": "user", "content": prompt},
            ])
            
            result = self._parse_json_response(response)
            
            if not result or "template" not in result:
                return {"error": "无法生成模板"}
            
            template_data = result["template"]
            
            # 格式化为 Markdown
            content = self._format_template_to_markdown(template_data)
            
            # 创建 UserKnowledgeItem
            item = UserKnowledgeItem(
                user_id=user_id,
                knowledge_type=UserKnowledgeType.REVIEW_TEMPLATE,
                title=template_data.get("title", f"{style}复盘模板"),
                content=content,
                tags=[style, "复盘", "模板"] + (focus_areas or []),
                source="ai_assisted",
            )
            
            # 自动保存
            saved = False
            if auto_save:
                saved = await self.user_store.create(item, trace_id)
            
            return {
                "template": item.model_dump(),
                "sections": template_data.get("sections", []),
                "usage_tips": result.get("usage_tips", []),
                "saved": saved,
                "item_id": item.id if saved else None,
            }
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Generate template failed: {e}")
            return {"error": str(e)}
    
    # ==================== 心得总结 ====================
    
    async def summarize_lesson(
        self,
        user_id: str,
        trade_description: str,
        outcome: str,
        reflection: str,
        auto_save: bool = False,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        总结交易心得
        
        Args:
            user_id: 用户ID
            trade_description: 交易描述
            outcome: 交易结果
            reflection: 用户反思
            auto_save: 是否自动保存
            trace_id: 追踪ID
            
        Returns:
            总结后的心得
        """
        llm = await self._get_llm()
        
        prompt = f"""帮助用户总结交易心得。

交易描述:
{trade_description}

交易结果:
{outcome}

用户反思:
{reflection}

请帮助用户:
1. 提炼核心教训
2. 归纳可复用的经验
3. 提出改进建议

输出 JSON 格式:
```json
{{
  "lesson": {{
    "title": "心得标题 (简洁有力)",
    "summary": "核心总结 (一两句话)",
    "key_learnings": ["关键教训1", "关键教训2"],
    "actionable_improvements": ["具体改进措施1", "具体改进措施2"],
    "applicable_scenarios": ["适用场景1", "适用场景2"],
    "tags": ["标签"]
  }},
  "pattern_detected": "是否发现重复出现的问题模式",
  "related_rules_suggestion": "建议添加/修改的规则"
}}
```"""
        
        try:
            response = await llm.chat([
                {"role": "system", "content": "你是交易心理教练，帮助用户从交易中学习和成长。"},
                {"role": "user", "content": prompt},
            ])
            
            result = self._parse_json_response(response)
            
            if not result or "lesson" not in result:
                return {"error": "无法总结心得"}
            
            lesson_data = result["lesson"]
            
            # 格式化内容
            content = f"""## 核心总结
{lesson_data.get('summary', '')}

## 关键教训
{chr(10).join(['- ' + l for l in lesson_data.get('key_learnings', [])])}

## 改进措施
{chr(10).join(['- ' + i for i in lesson_data.get('actionable_improvements', [])])}

## 适用场景
{chr(10).join(['- ' + s for s in lesson_data.get('applicable_scenarios', [])])}

---
原始交易: {trade_description}
结果: {outcome}
"""
            
            item = UserKnowledgeItem(
                user_id=user_id,
                knowledge_type=UserKnowledgeType.LESSON,
                title=lesson_data.get("title", "交易心得"),
                content=content,
                tags=lesson_data.get("tags", []),
                source="ai_assisted",
            )
            
            saved = False
            if auto_save:
                saved = await self.user_store.create(item, trace_id)
            
            return {
                "lesson": item.model_dump(),
                "key_learnings": lesson_data.get("key_learnings", []),
                "improvements": lesson_data.get("actionable_improvements", []),
                "pattern_detected": result.get("pattern_detected"),
                "related_rules_suggestion": result.get("related_rules_suggestion"),
                "saved": saved,
                "item_id": item.id if saved else None,
            }
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Summarize lesson failed: {e}")
            return {"error": str(e)}
    
    # ==================== 交易体系评估 ====================
    
    async def evaluate_trading_system(
        self,
        user_id: str,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        评估用户的交易体系完整性
        
        Returns:
            评估结果，包含完整性分析和建议
        """
        llm = await self._get_llm()
        
        # 获取用户所有知识
        all_items = await self.user_store.get_all(user_id, only_active=True, trace_id=trace_id)
        
        # 按类型统计
        stats = {}
        for item in all_items:
            t = item.knowledge_type.value
            if t not in stats:
                stats[t] = []
            stats[t].append(item.title)
        
        # 获取交易规则详情
        rules = [item for item in all_items if item.knowledge_type == UserKnowledgeType.TRADING_RULE]
        rules_detail = "\n".join([
            f"- {r.title}: 条件={r.conditions}, 动作={r.actions}"
            for r in rules[:20]
        ])
        
        prompt = f"""评估用户交易体系的完整性。

用户知识库统计:
{json.dumps(stats, ensure_ascii=False, indent=2)}

交易规则详情:
{rules_detail if rules_detail else "暂无交易规则"}

请评估:
1. 交易体系的完整性 (入场/出场/仓位/风控是否齐全)
2. 规则的具体性和可执行性
3. 可能存在的漏洞或不一致
4. 改进建议

输出 JSON 格式:
```json
{{
  "completeness_score": 0.7,
  "analysis": {{
    "strengths": ["优势1", "优势2"],
    "weaknesses": ["不足1", "不足2"],
    "gaps": ["缺失的部分"]
  }},
  "rule_coverage": {{
    "entry": {{"status": "complete/partial/missing", "count": 3}},
    "exit": {{"status": "complete/partial/missing", "count": 2}},
    "position": {{"status": "complete/partial/missing", "count": 1}},
    "risk": {{"status": "complete/partial/missing", "count": 0}}
  }},
  "recommendations": [
    {{"priority": "high/medium/low", "suggestion": "建议内容"}}
  ],
  "next_steps": ["下一步行动1", "下一步行动2"]
}}
```"""
        
        try:
            response = await llm.chat([
                {"role": "system", "content": "你是交易系统评估专家，帮助用户完善交易体系。"},
                {"role": "user", "content": prompt},
            ])
            
            result = self._parse_json_response(response)
            
            if not result:
                result = {
                    "completeness_score": 0,
                    "analysis": {"strengths": [], "weaknesses": ["无法评估"], "gaps": []},
                    "recommendations": [],
                }
            
            result["stats"] = {
                "total_items": len(all_items),
                "by_type": {k: len(v) for k, v in stats.items()},
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Evaluate system failed: {e}")
            return {"error": str(e)}
    
    # ==================== 辅助方法 ====================
    
    def _parse_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析 LLM 返回的 JSON"""
        # 尝试提取 JSON 块
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # 尝试直接解析
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass
        
        return None
    
    def _map_rule_type(self, rule_type: str) -> UserKnowledgeType:
        """映射规则类型"""
        mapping = {
            "entry": UserKnowledgeType.TRADING_RULE,
            "exit": UserKnowledgeType.TRADING_RULE,
            "position": UserKnowledgeType.TRADING_RULE,
            "risk": UserKnowledgeType.TRADING_RULE,
            "strategy": UserKnowledgeType.STRATEGY,
        }
        return mapping.get(rule_type.lower(), UserKnowledgeType.TRADING_RULE)
    
    def _format_template_to_markdown(self, template_data: Dict[str, Any]) -> str:
        """将模板数据格式化为 Markdown"""
        lines = [
            f"# {template_data.get('title', '复盘模板')}",
            "",
            template_data.get('description', ''),
            "",
        ]
        
        for section in template_data.get("sections", []):
            lines.append(f"## {section.get('name', '')}")
            lines.append("")
            
            if section.get("questions"):
                lines.append("### 思考问题")
                for q in section["questions"]:
                    lines.append(f"- [ ] {q}")
                lines.append("")
            
            if section.get("checklist"):
                lines.append("### 检查清单")
                for c in section["checklist"]:
                    lines.append(f"- [ ] {c}")
                lines.append("")
        
        return "\n".join(lines)
