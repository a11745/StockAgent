"""
LLM 模块

提供增强的 LLM 能力:
- Prompt 模板管理 (prompts/)
- 模型路由 (router.py)
- 输出解析 (parser.py)
- 缓存层 (cache.py)
- 高级服务 (service.py)

Example:
    from src.llm import llm_service, ModelTier
    
    await llm_service.initialize()
    
    # 使用模板
    result = await llm_service.invoke_template(
        "event_extract",
        title="央行降准",
        content="中国人民银行宣布..."
    )
    
    # 直接调用
    response = await llm_service.chat(
        messages=[{"role": "user", "content": "分析..."}],
        tier=ModelTier.BALANCED,
    )
"""

# Prompt 管理
from .prompts import (
    PromptTemplate,
    OutputFormat,
    PromptRegistry,
    prompt_registry,
    # 内置模板
    EVENT_EXTRACT_TEMPLATE,
    IMPORTANCE_ASSESS_TEMPLATE,
    REPORT_SUMMARY_TEMPLATE,
    REPORT_OVERVIEW_TEMPLATE,
    STOCK_ANALYSIS_TEMPLATE,
)

# 模型路由
from .router import (
    ModelTier,
    ModelConfig,
    ModelRouter,
    model_router,
)

# 输出解析
from .parser import (
    OutputParser,
    ParseError,
    output_parser,
    parse_json,
    parse_model,
    extract_code_block,
)

# 缓存
from .cache import (
    LLMCache,
    llm_cache,
)

# 高级服务
from .service import (
    LLMService,
    llm_service,
)


__all__ = [
    # Prompt
    "PromptTemplate",
    "OutputFormat",
    "PromptRegistry",
    "prompt_registry",
    "EVENT_EXTRACT_TEMPLATE",
    "IMPORTANCE_ASSESS_TEMPLATE",
    "REPORT_SUMMARY_TEMPLATE",
    "REPORT_OVERVIEW_TEMPLATE",
    "STOCK_ANALYSIS_TEMPLATE",
    
    # Router
    "ModelTier",
    "ModelConfig",
    "ModelRouter",
    "model_router",
    
    # Parser
    "OutputParser",
    "ParseError",
    "output_parser",
    "parse_json",
    "parse_model",
    "extract_code_block",
    
    # Cache
    "LLMCache",
    "llm_cache",
    
    # Service
    "LLMService",
    "llm_service",
]
