"""
程序性记忆存储

存储用户的交易体系和策略模式。
使用 Neo4j (知识图谱) + MongoDB (详细数据) 实现。

核心功能:
1. 分析用户持仓，提取交易模式
2. 记录成功/失败的交易策略
3. 建立交易决策的因果关系图
4. 持续优化交易体系
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
import uuid

from ..types import (
    LongTermMemoryItem,
    LongTermMemoryType,
    MemoryMetadata,
    MemoryVisibility,
    TradingPatternMetadata,
    InsertResult,
)


class PatternType(str, Enum):
    """交易模式类型"""
    ENTRY = "entry"                      # 入场策略
    EXIT = "exit"                        # 出场策略
    POSITION_SIZING = "position_sizing"  # 仓位管理
    RISK_MANAGEMENT = "risk_management"  # 风险控制
    SECTOR_ROTATION = "sector_rotation"  # 板块轮动
    TIMING = "timing"                    # 时机选择
    STOCK_SELECTION = "stock_selection"  # 选股策略


class TradeOutcome(str, Enum):
    """交易结果"""
    WIN = "win"           # 盈利
    LOSS = "loss"         # 亏损
    BREAKEVEN = "breakeven"  # 持平
    PENDING = "pending"   # 未完成


class TradingPattern(BaseModel):
    """交易模式"""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    user_id: str
    pattern_type: PatternType
    name: str = Field(description="模式名称")
    description: str = Field(description="模式描述")
    
    # 模式规则
    conditions: List[str] = Field(default_factory=list, description="触发条件")
    actions: List[str] = Field(default_factory=list, description="执行动作")
    
    # 统计数据
    sample_count: int = Field(default=0, description="样本数量")
    success_count: int = Field(default=0, description="成功次数")
    success_rate: float = Field(default=0.0, description="成功率")
    avg_return: float = Field(default=0.0, description="平均收益率")
    max_profit: float = Field(default=0.0, description="最大盈利")
    max_loss: float = Field(default=0.0, description="最大亏损")
    max_drawdown: float = Field(default=0.0, description="最大回撤")
    
    # 适用范围
    applicable_sectors: List[str] = Field(default_factory=list, description="适用板块")
    applicable_market_conditions: List[str] = Field(default_factory=list, description="适用市场环境")
    
    # 关联信息
    related_trades: List[str] = Field(default_factory=list, description="相关交易ID")
    derived_from: Optional[str] = Field(default=None, description="衍生自哪个模式")
    
    # 时间戳
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_used_at: Optional[datetime] = Field(default=None)
    
    # 状态
    is_active: bool = Field(default=True)
    confidence: float = Field(default=0.5, ge=0, le=1, description="置信度")
    
    def update_stats(self, outcome: TradeOutcome, return_pct: float) -> None:
        """更新统计数据"""
        self.sample_count += 1
        
        if outcome == TradeOutcome.WIN:
            self.success_count += 1
            self.max_profit = max(self.max_profit, return_pct)
        elif outcome == TradeOutcome.LOSS:
            self.max_loss = min(self.max_loss, return_pct)
        
        # 更新成功率
        self.success_rate = self.success_count / self.sample_count
        
        # 更新平均收益
        old_total = self.avg_return * (self.sample_count - 1)
        self.avg_return = (old_total + return_pct) / self.sample_count
        
        # 更新置信度 (基于样本量和成功率)
        sample_factor = min(1.0, self.sample_count / 20)  # 20 个样本达到最大
        self.confidence = sample_factor * self.success_rate
        
        self.updated_at = datetime.utcnow()
        self.last_used_at = datetime.utcnow()


class TradeRecord(BaseModel):
    """交易记录"""
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    user_id: str
    ts_code: str
    ts_name: Optional[str] = None
    
    # 交易信息
    direction: str = Field(description="方向: buy/sell")
    entry_price: float
    entry_date: str
    entry_reason: str = Field(description="入场原因")
    
    exit_price: Optional[float] = None
    exit_date: Optional[str] = None
    exit_reason: Optional[str] = None
    
    quantity: int
    
    # 结果
    outcome: TradeOutcome = TradeOutcome.PENDING
    return_pct: Optional[float] = None
    holding_days: Optional[int] = None
    
    # 关联模式
    pattern_ids: List[str] = Field(default_factory=list)
    
    # 上下文
    market_condition: Optional[str] = Field(default=None, description="市场环境")
    sector: Optional[str] = Field(default=None, description="所属板块")
    
    # 反思
    lessons_learned: Optional[str] = Field(default=None, description="复盘总结")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ProceduralStore:
    """
    程序性记忆存储
    
    管理用户的交易体系:
    1. 记录交易决策及其结果
    2. 提取并归纳交易模式
    3. 建立模式之间的关系图
    4. 提供交易建议
    
    Args:
        mongo_collection: MongoDB 集合名
    
    Example:
        store = ProceduralStore()
        
        # 记录一笔交易
        trade = TradeRecord(
            user_id="user1",
            ts_code="000001.SZ",
            direction="buy",
            entry_price=10.5,
            entry_date="20240101",
            entry_reason="突破年线，放量上涨",
            quantity=1000,
        )
        await store.record_trade(trade)
        
        # 交易结束后更新
        await store.close_trade(trade.id, exit_price=12.0, exit_reason="止盈")
        
        # 获取用户的交易模式
        patterns = await store.get_patterns(user_id, pattern_type=PatternType.ENTRY)
        
        # 分析持仓，获取建议
        suggestions = await store.analyze_holdings(user_id, holdings)
    """
    
    def __init__(
        self,
        patterns_collection: str = "trading_patterns",
        trades_collection: str = "trade_records",
    ):
        self.patterns_collection = patterns_collection
        self.trades_collection = trades_collection
        self.logger = logging.getLogger("src.memory.longterm.ProceduralStore")
        self._mongo_manager = None
        self._llm_manager = None
    
    async def _get_mongo(self):
        if self._mongo_manager is None:
            from core.managers import mongo_manager
            self._mongo_manager = mongo_manager
        return self._mongo_manager
    
    async def _get_llm(self):
        if self._llm_manager is None:
            from core.managers import llm_manager
            self._llm_manager = llm_manager
        return self._llm_manager
    
    # ==================== 交易记录管理 ====================
    
    async def record_trade(
        self,
        trade: TradeRecord,
        trace_id: Optional[str] = None,
    ) -> bool:
        """记录一笔交易"""
        mongo = await self._get_mongo()
        
        try:
            doc = trade.model_dump()
            doc["_id"] = trade.id
            
            await mongo.insert_one(self.trades_collection, doc)
            self.logger.info(f"[{trace_id}] Recorded trade: {trade.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Record trade failed: {e}")
            return False
    
    async def close_trade(
        self,
        trade_id: str,
        user_id: str,
        exit_price: float,
        exit_date: str,
        exit_reason: str,
        lessons_learned: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> Optional[TradeRecord]:
        """
        关闭交易并计算结果
        
        Returns:
            更新后的交易记录
        """
        mongo = await self._get_mongo()
        
        try:
            # 获取原交易
            doc = await mongo.find_one(
                self.trades_collection,
                {"_id": trade_id, "user_id": user_id},
            )
            
            if not doc:
                return None
            
            trade = TradeRecord(**{k: v for k, v in doc.items() if k != "_id"})
            trade.id = trade_id
            
            # 计算收益
            if trade.direction == "buy":
                return_pct = (exit_price - trade.entry_price) / trade.entry_price * 100
            else:
                return_pct = (trade.entry_price - exit_price) / trade.entry_price * 100
            
            # 确定结果
            if return_pct > 0.5:
                outcome = TradeOutcome.WIN
            elif return_pct < -0.5:
                outcome = TradeOutcome.LOSS
            else:
                outcome = TradeOutcome.BREAKEVEN
            
            # 计算持仓天数
            from datetime import datetime
            entry_dt = datetime.strptime(trade.entry_date, "%Y%m%d")
            exit_dt = datetime.strptime(exit_date, "%Y%m%d")
            holding_days = (exit_dt - entry_dt).days
            
            # 更新交易
            update_data = {
                "exit_price": exit_price,
                "exit_date": exit_date,
                "exit_reason": exit_reason,
                "outcome": outcome.value,
                "return_pct": return_pct,
                "holding_days": holding_days,
                "lessons_learned": lessons_learned,
                "updated_at": datetime.utcnow(),
            }
            
            await mongo.update_one(
                self.trades_collection,
                {"_id": trade_id},
                {"$set": update_data},
            )
            
            # 更新关联的交易模式
            for pattern_id in trade.pattern_ids:
                await self._update_pattern_stats(pattern_id, outcome, return_pct, trace_id)
            
            # 更新本地对象
            for k, v in update_data.items():
                if k != "outcome":
                    setattr(trade, k, v)
            trade.outcome = outcome
            
            self.logger.info(
                f"[{trace_id}] Closed trade: {trade_id}, "
                f"outcome={outcome.value}, return={return_pct:.2f}%"
            )
            
            return trade
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Close trade failed: {e}")
            return None
    
    async def get_trades(
        self,
        user_id: str,
        ts_code: Optional[str] = None,
        outcome: Optional[TradeOutcome] = None,
        limit: int = 50,
        trace_id: Optional[str] = None,
    ) -> List[TradeRecord]:
        """获取用户的交易记录"""
        mongo = await self._get_mongo()
        
        try:
            query: Dict[str, Any] = {"user_id": user_id}
            
            if ts_code:
                query["ts_code"] = ts_code
            
            if outcome:
                query["outcome"] = outcome.value
            
            docs = await mongo.find(
                self.trades_collection,
                query,
                sort=[("created_at", -1)],
                limit=limit,
            )
            
            trades = []
            for doc in docs:
                trade = TradeRecord(**{k: v for k, v in doc.items() if k != "_id"})
                trade.id = doc["_id"]
                trades.append(trade)
            
            return trades
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get trades failed: {e}")
            return []
    
    # ==================== 交易模式管理 ====================
    
    async def create_pattern(
        self,
        pattern: TradingPattern,
        trace_id: Optional[str] = None,
    ) -> bool:
        """创建交易模式"""
        mongo = await self._get_mongo()
        
        try:
            doc = pattern.model_dump()
            doc["_id"] = pattern.id
            
            await mongo.insert_one(self.patterns_collection, doc)
            self.logger.info(f"[{trace_id}] Created pattern: {pattern.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Create pattern failed: {e}")
            return False
    
    async def get_patterns(
        self,
        user_id: str,
        pattern_type: Optional[PatternType] = None,
        only_active: bool = True,
        min_confidence: float = 0.0,
        trace_id: Optional[str] = None,
    ) -> List[TradingPattern]:
        """获取用户的交易模式"""
        mongo = await self._get_mongo()
        
        try:
            query: Dict[str, Any] = {"user_id": user_id}
            
            if pattern_type:
                query["pattern_type"] = pattern_type.value
            
            if only_active:
                query["is_active"] = True
            
            if min_confidence > 0:
                query["confidence"] = {"$gte": min_confidence}
            
            docs = await mongo.find(
                self.patterns_collection,
                query,
                sort=[("confidence", -1), ("sample_count", -1)],
            )
            
            patterns = []
            for doc in docs:
                pattern = TradingPattern(**{k: v for k, v in doc.items() if k != "_id"})
                pattern.id = doc["_id"]
                patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Get patterns failed: {e}")
            return []
    
    async def _update_pattern_stats(
        self,
        pattern_id: str,
        outcome: TradeOutcome,
        return_pct: float,
        trace_id: Optional[str] = None,
    ) -> bool:
        """更新模式统计数据"""
        mongo = await self._get_mongo()
        
        try:
            doc = await mongo.find_one(self.patterns_collection, {"_id": pattern_id})
            if not doc:
                return False
            
            pattern = TradingPattern(**{k: v for k, v in doc.items() if k != "_id"})
            pattern.id = pattern_id
            pattern.update_stats(outcome, return_pct)
            
            await mongo.update_one(
                self.patterns_collection,
                {"_id": pattern_id},
                {"$set": pattern.model_dump()},
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Update pattern stats failed: {e}")
            return False
    
    # ==================== 持仓分析 ====================
    
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
            holdings: 持仓列表，每项包含:
                - ts_code: 股票代码
                - ts_name: 股票名称
                - quantity: 持仓数量
                - cost_price: 成本价
                - current_price: 现价
                - entry_date: 入场日期
                - entry_reason: 入场原因 (可选)
            trace_id: 追踪ID
            
        Returns:
            分析结果，包含:
            - holdings_analysis: 每只股票的分析
            - patterns_matched: 匹配到的交易模式
            - suggestions: 交易建议
            - trading_system_review: 交易体系评估
        """
        llm = await self._get_llm()
        
        try:
            # 获取用户的交易模式
            patterns = await self.get_patterns(user_id, trace_id=trace_id)
            
            # 获取历史交易
            trades = await self.get_trades(user_id, limit=100, trace_id=trace_id)
            
            # 构建分析上下文
            context = self._build_analysis_context(holdings, patterns, trades)
            
            # 调用 LLM 进行分析
            analysis_prompt = self._build_analysis_prompt(context)
            
            response = await llm.chat([
                {"role": "system", "content": self._get_trading_system_prompt()},
                {"role": "user", "content": analysis_prompt},
            ])
            
            # 解析 LLM 响应
            analysis_result = self._parse_analysis_response(response, holdings, patterns)
            
            # 记录分析到情景记忆
            # (这里可以调用 EpisodicStore)
            
            self.logger.info(f"[{trace_id}] Analyzed {len(holdings)} holdings for user {user_id}")
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Analyze holdings failed: {e}")
            return {
                "error": str(e),
                "holdings_analysis": [],
                "suggestions": [],
            }
    
    def _build_analysis_context(
        self,
        holdings: List[Dict[str, Any]],
        patterns: List[TradingPattern],
        trades: List[TradeRecord],
    ) -> Dict[str, Any]:
        """构建分析上下文"""
        # 计算持仓统计
        total_value = sum(
            h.get("quantity", 0) * h.get("current_price", 0) 
            for h in holdings
        )
        
        holdings_summary = []
        for h in holdings:
            cost = h.get("cost_price", 0)
            current = h.get("current_price", 0)
            profit_pct = ((current - cost) / cost * 100) if cost > 0 else 0
            
            position_value = h.get("quantity", 0) * current
            position_pct = (position_value / total_value * 100) if total_value > 0 else 0
            
            holdings_summary.append({
                **h,
                "profit_pct": round(profit_pct, 2),
                "position_pct": round(position_pct, 2),
            })
        
        # 计算交易统计
        completed_trades = [t for t in trades if t.outcome != TradeOutcome.PENDING]
        win_trades = [t for t in completed_trades if t.outcome == TradeOutcome.WIN]
        
        trade_stats = {
            "total_trades": len(completed_trades),
            "win_rate": len(win_trades) / len(completed_trades) if completed_trades else 0,
            "avg_return": sum(t.return_pct or 0 for t in completed_trades) / len(completed_trades) if completed_trades else 0,
            "avg_holding_days": sum(t.holding_days or 0 for t in completed_trades) / len(completed_trades) if completed_trades else 0,
        }
        
        # 汇总模式
        patterns_summary = []
        for p in patterns[:10]:  # 取置信度最高的 10 个
            patterns_summary.append({
                "name": p.name,
                "type": p.pattern_type.value,
                "conditions": p.conditions,
                "success_rate": p.success_rate,
                "sample_count": p.sample_count,
                "avg_return": p.avg_return,
                "confidence": p.confidence,
            })
        
        return {
            "holdings": holdings_summary,
            "total_value": total_value,
            "trade_stats": trade_stats,
            "patterns": patterns_summary,
        }
    
    def _build_analysis_prompt(self, context: Dict[str, Any]) -> str:
        """构建分析提示词"""
        holdings_text = "\n".join([
            f"- {h.get('ts_name', h.get('ts_code'))}: "
            f"持仓{h.get('quantity')}股, 成本{h.get('cost_price')}, 现价{h.get('current_price')}, "
            f"盈亏{h.get('profit_pct')}%, 仓位占比{h.get('position_pct')}%"
            f"{', 入场原因: ' + h.get('entry_reason') if h.get('entry_reason') else ''}"
            for h in context["holdings"]
        ])
        
        patterns_text = "\n".join([
            f"- {p['name']} ({p['type']}): "
            f"成功率{p['success_rate']*100:.1f}%, 样本{p['sample_count']}次, "
            f"平均收益{p['avg_return']:.2f}%"
            for p in context["patterns"]
        ]) if context["patterns"] else "暂无已识别的交易模式"
        
        stats = context["trade_stats"]
        
        return f"""请分析以下持仓情况，并提供交易体系建议：

## 当前持仓
总市值: {context['total_value']:.2f} 元
{holdings_text}

## 历史交易统计
- 总交易次数: {stats['total_trades']}
- 胜率: {stats['win_rate']*100:.1f}%
- 平均收益: {stats['avg_return']:.2f}%
- 平均持仓天数: {stats['avg_holding_days']:.1f}

## 已识别的交易模式
{patterns_text}

请从以下几个方面进行分析：
1. 持仓结构分析（分散度、行业集中度、仓位分配）
2. 每只股票的操作建议
3. 交易体系的优势和待改进之处
4. 新的交易模式识别（如果发现规律）
5. 风险提示

请用 JSON 格式输出分析结果。"""
    
    def _get_trading_system_prompt(self) -> str:
        """交易体系分析的系统提示词"""
        return """你是一个专业的量化交易分析师，擅长帮助投资者完善交易体系。

你的分析应该：
1. 客观、理性，基于数据
2. 关注风险控制和仓位管理
3. 识别用户的交易习惯和模式
4. 提供可操作的具体建议
5. 使用简洁专业的语言

输出格式要求：
```json
{
  "holdings_analysis": [
    {
      "ts_code": "股票代码",
      "status": "good/warning/danger",
      "suggestion": "操作建议",
      "reason": "原因分析"
    }
  ],
  "patterns_identified": [
    {
      "name": "模式名称",
      "type": "entry/exit/risk_management/position_sizing",
      "description": "描述",
      "conditions": ["条件1", "条件2"],
      "confidence": 0.7
    }
  ],
  "trading_system_review": {
    "strengths": ["优势1", "优势2"],
    "weaknesses": ["待改进1", "待改进2"],
    "suggestions": ["建议1", "建议2"]
  },
  "risk_warnings": ["风险1", "风险2"]
}
```"""
    
    def _parse_analysis_response(
        self,
        response: str,
        holdings: List[Dict[str, Any]],
        existing_patterns: List[TradingPattern],
    ) -> Dict[str, Any]:
        """解析 LLM 分析响应"""
        import json
        import re
        
        # 尝试提取 JSON
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        
        if json_match:
            try:
                result = json.loads(json_match.group(1))
                return result
            except json.JSONDecodeError:
                pass
        
        # 尝试直接解析
        try:
            result = json.loads(response)
            return result
        except json.JSONDecodeError:
            pass
        
        # 返回原始响应
        return {
            "raw_analysis": response,
            "holdings_analysis": [],
            "patterns_identified": [],
            "trading_system_review": {},
            "risk_warnings": [],
        }
    
    # ==================== 模式学习 ====================
    
    async def learn_patterns_from_trades(
        self,
        user_id: str,
        min_samples: int = 5,
        trace_id: Optional[str] = None,
    ) -> List[TradingPattern]:
        """
        从历史交易中学习新模式
        
        Args:
            user_id: 用户ID
            min_samples: 最小样本数
            trace_id: 追踪ID
            
        Returns:
            识别出的新模式
        """
        llm = await self._get_llm()
        
        try:
            # 获取已完成的交易
            trades = await self.get_trades(user_id, limit=200, trace_id=trace_id)
            completed = [t for t in trades if t.outcome != TradeOutcome.PENDING]
            
            if len(completed) < min_samples:
                self.logger.info(f"[{trace_id}] Not enough trades for pattern learning")
                return []
            
            # 获取现有模式
            existing = await self.get_patterns(user_id, trace_id=trace_id)
            existing_names = {p.name for p in existing}
            
            # 构建学习提示
            trades_text = "\n".join([
                f"- {t.ts_code} {t.direction}: "
                f"入场{t.entry_date}@{t.entry_price}, "
                f"出场{t.exit_date}@{t.exit_price}, "
                f"收益{t.return_pct:.2f}%, 原因: {t.entry_reason} -> {t.exit_reason}"
                for t in completed[:50]
            ])
            
            prompt = f"""分析以下交易记录，识别交易模式：

{trades_text}

请识别出重复出现的交易模式（入场策略、出场策略、仓位管理等）。
每个模式需要：
1. 明确的触发条件
2. 具体的执行动作
3. 至少出现 {min_samples} 次

输出 JSON 格式：
```json
[
  {{
    "name": "模式名称",
    "type": "entry/exit/risk_management/position_sizing",
    "conditions": ["条件1", "条件2"],
    "actions": ["动作1", "动作2"],
    "description": "描述",
    "sample_count": 5
  }}
]
```"""
            
            response = await llm.chat([
                {"role": "system", "content": "你是交易模式识别专家，擅长从交易记录中发现规律。"},
                {"role": "user", "content": prompt},
            ])
            
            # 解析响应
            new_patterns = self._parse_patterns_response(response, user_id)
            
            # 过滤已存在的模式
            new_patterns = [p for p in new_patterns if p.name not in existing_names]
            
            # 保存新模式
            for pattern in new_patterns:
                await self.create_pattern(pattern, trace_id)
            
            self.logger.info(f"[{trace_id}] Learned {len(new_patterns)} new patterns")
            
            return new_patterns
            
        except Exception as e:
            self.logger.error(f"[{trace_id}] Learn patterns failed: {e}")
            return []
    
    def _parse_patterns_response(
        self,
        response: str,
        user_id: str,
    ) -> List[TradingPattern]:
        """解析模式学习响应"""
        import json
        import re
        
        patterns = []
        
        # 提取 JSON
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        
        if json_match:
            try:
                items = json.loads(json_match.group(1))
                for item in items:
                    pattern = TradingPattern(
                        user_id=user_id,
                        pattern_type=PatternType(item.get("type", "entry")),
                        name=item.get("name", "未命名模式"),
                        description=item.get("description", ""),
                        conditions=item.get("conditions", []),
                        actions=item.get("actions", []),
                        sample_count=item.get("sample_count", 0),
                        confidence=0.3,  # 初始置信度
                    )
                    patterns.append(pattern)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"Failed to parse patterns response: {e}")
        
        return patterns
    
    # ==================== 交易建议 ====================
    
    async def get_entry_suggestions(
        self,
        user_id: str,
        ts_code: str,
        current_price: float,
        market_data: Dict[str, Any],
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        根据用户交易体系，获取入场建议
        
        Args:
            user_id: 用户ID
            ts_code: 股票代码
            current_price: 当前价格
            market_data: 市场数据 (成交量、均线等)
            trace_id: 追踪ID
            
        Returns:
            入场建议
        """
        # 获取入场模式
        entry_patterns = await self.get_patterns(
            user_id, 
            pattern_type=PatternType.ENTRY,
            min_confidence=0.4,
            trace_id=trace_id,
        )
        
        # 获取该股票的历史交易
        stock_trades = await self.get_trades(user_id, ts_code=ts_code, trace_id=trace_id)
        
        suggestions = {
            "ts_code": ts_code,
            "current_price": current_price,
            "matched_patterns": [],
            "historical_performance": None,
            "recommendation": "neutral",
            "reasons": [],
        }
        
        # 检查匹配的模式
        for pattern in entry_patterns:
            # TODO: 实现条件匹配逻辑
            # 这里简化处理，实际应该解析 pattern.conditions 并与 market_data 匹配
            suggestions["matched_patterns"].append({
                "name": pattern.name,
                "success_rate": pattern.success_rate,
                "avg_return": pattern.avg_return,
                "conditions": pattern.conditions,
            })
        
        # 历史表现
        if stock_trades:
            wins = sum(1 for t in stock_trades if t.outcome == TradeOutcome.WIN)
            suggestions["historical_performance"] = {
                "total_trades": len(stock_trades),
                "win_rate": wins / len(stock_trades) if stock_trades else 0,
                "avg_return": sum(t.return_pct or 0 for t in stock_trades) / len(stock_trades) if stock_trades else 0,
            }
        
        return suggestions
