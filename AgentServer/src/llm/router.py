"""
模型路由器

根据任务类型自动选择最合适的模型:
- fast: 简单任务，低成本 (如分类、提取)
- balanced: 中等任务 (如摘要、问答)
- quality: 复杂任务，高质量 (如分析、推理)
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel


logger = logging.getLogger(__name__)


class ModelTier(str, Enum):
    """模型等级"""
    FAST = "fast"           # 快速、低成本
    BALANCED = "balanced"   # 平衡
    QUALITY = "quality"     # 高质量


@dataclass
class ModelConfig:
    """模型配置"""
    name: str               # 模型名称
    provider: str           # 提供商 (deepseek, openai, zhipu, ollama)
    tier: ModelTier         # 等级
    cost_per_1k_tokens: float  # 成本 ($/1k tokens)
    max_context: int        # 最大上下文长度
    supports_json: bool = True
    supports_stream: bool = True
    
    # API 配置 (可选覆盖)
    api_base: Optional[str] = None
    api_key: Optional[str] = None


# 预定义模型配置
PREDEFINED_MODELS: Dict[str, ModelConfig] = {
    # DeepSeek
    "deepseek-chat": ModelConfig(
        name="deepseek-chat",
        provider="deepseek",
        tier=ModelTier.BALANCED,
        cost_per_1k_tokens=0.001,
        max_context=64000,
    ),
    "deepseek-reasoner": ModelConfig(
        name="deepseek-reasoner",
        provider="deepseek",
        tier=ModelTier.QUALITY,
        cost_per_1k_tokens=0.002,
        max_context=64000,
    ),
    
    # OpenAI
    "gpt-4o-mini": ModelConfig(
        name="gpt-4o-mini",
        provider="openai",
        tier=ModelTier.FAST,
        cost_per_1k_tokens=0.00015,
        max_context=128000,
    ),
    "gpt-4o": ModelConfig(
        name="gpt-4o",
        provider="openai",
        tier=ModelTier.QUALITY,
        cost_per_1k_tokens=0.005,
        max_context=128000,
    ),
    
    # 智谱
    "glm-4-flash": ModelConfig(
        name="glm-4-flash",
        provider="zhipu",
        tier=ModelTier.FAST,
        cost_per_1k_tokens=0.0001,
        max_context=128000,
    ),
    "glm-4-plus": ModelConfig(
        name="glm-4-plus",
        provider="zhipu",
        tier=ModelTier.BALANCED,
        cost_per_1k_tokens=0.0005,
        max_context=128000,
    ),
    
    # 通义千问
    "qwen-turbo": ModelConfig(
        name="qwen-turbo",
        provider="dashscope",
        tier=ModelTier.FAST,
        cost_per_1k_tokens=0.0003,
        max_context=128000,
    ),
    "qwen-plus": ModelConfig(
        name="qwen-plus",
        provider="dashscope",
        tier=ModelTier.BALANCED,
        cost_per_1k_tokens=0.0008,
        max_context=128000,
    ),
    "qwen-max": ModelConfig(
        name="qwen-max",
        provider="dashscope",
        tier=ModelTier.QUALITY,
        cost_per_1k_tokens=0.002,
        max_context=32000,
    ),
    
    # Ollama (本地)
    "qwen2.5:7b": ModelConfig(
        name="qwen2.5:7b",
        provider="ollama",
        tier=ModelTier.FAST,
        cost_per_1k_tokens=0,  # 本地免费
        max_context=32000,
    ),
    "qwen2.5:14b": ModelConfig(
        name="qwen2.5:14b",
        provider="ollama",
        tier=ModelTier.BALANCED,
        cost_per_1k_tokens=0,
        max_context=32000,
    ),
}


class ModelRouter:
    """
    模型路由器
    
    根据任务需求自动选择模型，支持:
    - 按等级选择
    - 按成本优化
    - 降级策略
    
    Example:
        router = ModelRouter()
        
        # 按等级选择
        model = router.select(tier=ModelTier.FAST)
        
        # 按 Prompt 偏好选择
        model = router.select_for_prompt(template)
        
        # 带降级的选择
        model, fallbacks = router.select_with_fallback(tier=ModelTier.QUALITY)
    """
    
    def __init__(self):
        self.models: Dict[str, ModelConfig] = PREDEFINED_MODELS.copy()
        self._tier_priority: Dict[ModelTier, List[str]] = {
            ModelTier.FAST: [],
            ModelTier.BALANCED: [],
            ModelTier.QUALITY: [],
        }
        self._default_tier = ModelTier.BALANCED
        self._configured = False
    
    def configure(
        self,
        models: Optional[Dict[str, ModelConfig]] = None,
        tier_priority: Optional[Dict[ModelTier, List[str]]] = None,
        default_tier: Optional[ModelTier] = None,
    ) -> None:
        """
        配置路由器
        
        Args:
            models: 自定义模型配置
            tier_priority: 各等级的模型优先级列表
            default_tier: 默认等级
        """
        if models:
            self.models.update(models)
        
        if tier_priority:
            self._tier_priority = tier_priority
        
        if default_tier:
            self._default_tier = default_tier
        
        self._configured = True
        logger.info(f"ModelRouter configured with {len(self.models)} models")
    
    def configure_from_env(self) -> None:
        """从环境变量配置"""
        from core.settings import settings
        
        llm_config = settings.llm
        
        provider = llm_config.provider
        model_name = llm_config.model_name
        
        # 读取各等级的模型配置
        fast_model = llm_config.fast_model or model_name
        balanced_model = llm_config.balanced_model or model_name
        quality_model = llm_config.quality_model or model_name
        
        # 设置各等级的模型优先级
        self._tier_priority[ModelTier.FAST] = [fast_model]
        self._tier_priority[ModelTier.BALANCED] = [balanced_model]
        self._tier_priority[ModelTier.QUALITY] = [quality_model]
        
        # 如果配置的模型不在预定义列表中，动态添加
        for tier_model in [fast_model, balanced_model, quality_model]:
            if tier_model and tier_model not in self.models:
                # 根据主 provider 创建配置
                self.models[tier_model] = ModelConfig(
                    name=tier_model,
                    provider=provider,
                    tier=ModelTier.BALANCED,  # 默认等级
                    cost_per_1k_tokens=0.001,
                    max_context=32000,
                )
        
        # 添加同 provider 的其他模型作为 fallback
        same_provider_models = [
            name for name, config in self.models.items()
            if config.provider == provider and name not in [fast_model, balanced_model, quality_model]
        ]
        
        for tier in ModelTier:
            tier_models = [
                name for name in same_provider_models
                if self.models[name].tier == tier
            ]
            self._tier_priority[tier].extend(tier_models)
        
        self._configured = True
        logger.info(
            f"ModelRouter configured: fast={fast_model}, "
            f"balanced={balanced_model}, quality={quality_model}"
        )
    
    def select(
        self,
        tier: Optional[ModelTier] = None,
        provider: Optional[str] = None,
        max_cost: Optional[float] = None,
    ) -> Optional[ModelConfig]:
        """
        选择模型
        
        Args:
            tier: 目标等级
            provider: 指定提供商
            max_cost: 最大成本
            
        Returns:
            匹配的模型配置
        """
        if not self._configured:
            self.configure_from_env()
        
        tier = tier or self._default_tier
        
        # 按优先级查找
        candidates = self._tier_priority.get(tier, [])
        
        for model_name in candidates:
            if model_name not in self.models:
                continue
            
            config = self.models[model_name]
            
            # 过滤条件
            if provider and config.provider != provider:
                continue
            if max_cost and config.cost_per_1k_tokens > max_cost:
                continue
            
            return config
        
        # 如果优先级列表为空，从所有模型中选择
        for name, config in self.models.items():
            if config.tier == tier:
                if provider and config.provider != provider:
                    continue
                if max_cost and config.cost_per_1k_tokens > max_cost:
                    continue
                return config
        
        return None
    
    def select_for_prompt(self, template: Any) -> Optional[ModelConfig]:
        """
        根据 Prompt 模板选择模型
        
        Args:
            template: PromptTemplate 实例
            
        Returns:
            匹配的模型配置
        """
        preference = getattr(template, "model_preference", None)
        
        if preference:
            try:
                tier = ModelTier(preference)
                return self.select(tier=tier)
            except ValueError:
                # preference 不是有效的 tier，可能是具体模型名
                if preference in self.models:
                    return self.models[preference]
        
        return self.select()
    
    def select_with_fallback(
        self,
        tier: Optional[ModelTier] = None,
        fallback_count: int = 2,
    ) -> Tuple[Optional[ModelConfig], List[ModelConfig]]:
        """
        选择模型并返回降级选项
        
        Args:
            tier: 目标等级
            fallback_count: 降级选项数量
            
        Returns:
            (primary_model, fallback_models)
        """
        primary = self.select(tier=tier)
        
        if not primary:
            return None, []
        
        # 获取同等级或更低等级的模型作为降级选项
        fallbacks = []
        tier_order = [ModelTier.QUALITY, ModelTier.BALANCED, ModelTier.FAST]
        
        current_tier_idx = tier_order.index(tier or self._default_tier)
        
        for i in range(current_tier_idx, len(tier_order)):
            for name, config in self.models.items():
                if config.tier == tier_order[i] and config.name != primary.name:
                    fallbacks.append(config)
                    if len(fallbacks) >= fallback_count:
                        return primary, fallbacks
        
        return primary, fallbacks
    
    def get_cheapest(self, tier: Optional[ModelTier] = None) -> Optional[ModelConfig]:
        """获取最便宜的模型"""
        candidates = [
            config for config in self.models.values()
            if tier is None or config.tier == tier
        ]
        
        if not candidates:
            return None
        
        return min(candidates, key=lambda c: c.cost_per_1k_tokens)
    
    def estimate_cost(
        self,
        model_name: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> float:
        """估算调用成本"""
        config = self.models.get(model_name)
        if not config:
            return 0.0
        
        total_tokens = prompt_tokens + completion_tokens
        return (total_tokens / 1000) * config.cost_per_1k_tokens


# 全局单例
model_router = ModelRouter()
