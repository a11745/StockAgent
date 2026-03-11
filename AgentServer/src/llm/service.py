"""
LLM 服务

整合 Prompt 管理、模型路由、输出解析、缓存的高级服务。
"""

import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel

from .prompts import PromptTemplate, prompt_registry, OutputFormat
from .router import ModelRouter, ModelTier, model_router
from .parser import OutputParser, output_parser, ParseError
from .cache import LLMCache, llm_cache


logger = logging.getLogger(__name__)


T = TypeVar("T", bound=BaseModel)


class LLMService:
    """
    LLM 高级服务
    
    整合:
    - Prompt 模板管理
    - 模型路由
    - 输出解析
    - 缓存
    - 重试机制
    
    Example:
        service = LLMService()
        await service.initialize()
        
        # 使用模板
        result = await service.invoke_template(
            "event_extract",
            title="央行降准",
            content="中国人民银行宣布..."
        )
        
        # 直接调用 (带缓存)
        response = await service.chat(
            messages=[{"role": "user", "content": "分析..."}],
            tier=ModelTier.BALANCED,
        )
        
        # 解析为模型
        data = await service.invoke_and_parse(
            template_name="event_extract",
            output_model=EventData,
            title="xxx",
            content="yyy",
        )
    """
    
    def __init__(
        self,
        use_cache: Optional[bool] = None,
        use_redis_cache: Optional[bool] = None,
        max_retries: int = 3,
    ):
        self._llm = None
        self._cache = None  # 延迟初始化
        self._router = model_router
        self._parser = output_parser
        self._registry = prompt_registry
        
        self._use_cache = use_cache
        self._use_redis_cache = use_redis_cache
        self._max_retries = max_retries
        self._initialized = False
    
    async def initialize(self) -> None:
        """初始化服务"""
        if self._initialized:
            return
        
        # 读取配置
        from core.settings import settings
        llm_config = settings.llm
        
        # 初始化 LLM Manager
        from core.managers import llm_manager
        if not llm_manager.is_initialized:
            await llm_manager.initialize()
        self._llm = llm_manager
        
        # 初始化缓存 (从配置读取)
        use_cache = self._use_cache if self._use_cache is not None else llm_config.cache_enabled
        use_redis = self._use_redis_cache if self._use_redis_cache is not None else llm_config.cache_use_redis
        
        if use_cache:
            self._cache = LLMCache(
                use_redis=use_redis,
                chat_ttl=llm_config.cache_chat_ttl,
                embedding_ttl=llm_config.cache_embedding_ttl,
            )
        
        # 配置路由器
        self._router.configure_from_env()
        
        # 加载模板
        self._registry.load_templates()
        
        self._initialized = True
        logger.info(
            f"LLMService initialized: cache={use_cache}, "
            f"redis={use_redis if use_cache else 'N/A'}"
        )
    
    def _ensure_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("LLMService not initialized")
    
    # ==================== 模板调用 ====================
    
    async def invoke_template(
        self,
        template_name: str,
        use_cache: bool = True,
        **variables,
    ) -> str:
        """
        使用模板调用 LLM
        
        Args:
            template_name: 模板名称
            use_cache: 是否使用缓存
            **variables: 模板变量
            
        Returns:
            LLM 响应
        """
        self._ensure_initialized()
        
        # 获取模板
        template = self._registry.get_or_raise(template_name)
        
        # 渲染模板
        rendered = template.render(**variables)
        
        # 选择模型
        model_config = self._router.select_for_prompt(template)
        model_name = model_config.name if model_config else None
        
        # 调用
        return await self.chat(
            messages=rendered["messages"],
            model=model_name,
            temperature=rendered["temperature"],
            max_tokens=rendered["max_tokens"],
            use_cache=use_cache,
        )
    
    async def invoke_and_parse(
        self,
        template_name: str,
        output_model: Optional[Type[T]] = None,
        strict: bool = False,
        **variables,
    ) -> Optional[T]:
        """
        使用模板调用并解析为模型
        
        Args:
            template_name: 模板名称
            output_model: Pydantic 模型类
            strict: 是否严格模式
            **variables: 模板变量
            
        Returns:
            解析后的模型实例
        """
        response = await self.invoke_template(template_name, **variables)
        
        if output_model:
            return self._parser.parse_model(response, output_model, strict)
        else:
            return self._parser.parse_json(response, strict)
    
    # ==================== 直接调用 ====================
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        tier: Optional[ModelTier] = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        use_cache: bool = True,
        **kwargs,
    ) -> str:
        """
        Chat 调用
        
        Args:
            messages: 消息列表
            model: 模型名称 (可选)
            tier: 模型等级 (可选)
            temperature: 温度
            max_tokens: 最大 token
            use_cache: 是否使用缓存
            
        Returns:
            响应文本
        """
        self._ensure_initialized()
        
        # 确定模型
        if model is None and tier:
            model_config = self._router.select(tier=tier)
            model = model_config.name if model_config else None
        
        # 检查缓存
        if use_cache and self._cache:
            cached = await self._cache.get_chat(messages, model or "", temperature)
            if cached:
                return cached
        
        # 调用 LLM (带重试)
        response = await self._call_with_retry(
            self._llm.chat,
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        
        # 存入缓存
        if use_cache and self._cache:
            await self._cache.set_chat(messages, model or "", response, temperature)
        
        return response
    
    async def embedding(
        self,
        texts: List[str],
        model: Optional[str] = None,
        use_cache: bool = True,
    ) -> List[List[float]]:
        """
        获取 Embedding
        
        Args:
            texts: 文本列表
            model: 模型名称
            use_cache: 是否使用缓存
            
        Returns:
            向量列表
        """
        self._ensure_initialized()
        
        model = model or ""
        
        if use_cache and self._cache:
            # 批量检查缓存
            cached, missing_indices = await self._cache.get_embeddings_batch(texts, model)
            
            if not missing_indices:
                # 全部命中缓存
                return cached
            
            # 只请求未缓存的
            missing_texts = [texts[i] for i in missing_indices]
            new_embeddings = await self._call_with_retry(
                self._llm.embedding,
                texts=missing_texts,
                model=model if model else None,
            )
            
            # 合并结果
            result = cached.copy()
            for i, idx in enumerate(missing_indices):
                result[idx] = new_embeddings[i]
            
            # 缓存新结果
            await self._cache.set_embeddings_batch(missing_texts, model, new_embeddings)
            
            return result
        else:
            return await self._call_with_retry(
                self._llm.embedding,
                texts=texts,
                model=model if model else None,
            )
    
    async def _call_with_retry(self, func, **kwargs) -> Any:
        """带重试的调用"""
        import asyncio
        
        last_error = None
        
        for attempt in range(self._max_retries):
            try:
                return await func(**kwargs)
            except Exception as e:
                last_error = e
                
                if attempt < self._max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    logger.warning(
                        f"LLM call failed (attempt {attempt + 1}/{self._max_retries}): {e}, "
                        f"retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
        
        raise last_error
    
    # ==================== 便捷方法 ====================
    
    async def extract_json(
        self,
        prompt: str,
        tier: ModelTier = ModelTier.FAST,
        **kwargs,
    ) -> Optional[Dict[str, Any]]:
        """快速提取 JSON"""
        response = await self.chat(
            messages=[{"role": "user", "content": prompt}],
            tier=tier,
            temperature=0.1,
            **kwargs,
        )
        return self._parser.parse_json(response)
    
    async def summarize(
        self,
        text: str,
        max_length: int = 100,
        tier: ModelTier = ModelTier.BALANCED,
    ) -> str:
        """文本摘要"""
        prompt = f"请用{max_length}字以内总结以下内容:\n\n{text}"
        return await self.chat(
            messages=[{"role": "user", "content": prompt}],
            tier=tier,
            temperature=0.3,
            max_tokens=max_length * 2,
        )
    
    async def classify(
        self,
        text: str,
        categories: List[str],
        tier: ModelTier = ModelTier.FAST,
    ) -> str:
        """文本分类"""
        categories_str = ", ".join(categories)
        prompt = f"""将以下文本分类到这些类别之一: {categories_str}

文本: {text}

只返回类别名称，不要其他内容。"""
        
        response = await self.chat(
            messages=[{"role": "user", "content": prompt}],
            tier=tier,
            temperature=0,
            max_tokens=50,
        )
        return response.strip()
    
    # ==================== 管理 ====================
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取服务统计"""
        stats = {
            "initialized": self._initialized,
            "templates": self._registry.list_templates(),
        }
        
        if self._cache:
            stats["cache"] = await self._cache.stats()
        
        if self._llm:
            stats["token_usage"] = self._llm.get_token_usage()
        
        return stats
    
    async def clear_cache(self) -> None:
        """清空缓存"""
        if self._cache:
            await self._cache.clear()


# 全局单例
llm_service = LLMService()
