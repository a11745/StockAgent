"""
AI 核心服务层 (src)

独立、可重用、易迁移的 AI 核心服务模块。
从具体业务节点中剥离"记忆系统"、"数据处理流水线"和"RAG 功能"。

架构原则:
    1. 严禁向上引用：src/ 下的代码严禁引用 nodes/ 中的任何类或函数
    2. 单例复用：必须使用 core.managers 中已有的单例访问资源
    3. 全异步化：所有 I/O 密集型操作必须使用 async/await
    4. 日志 Trace：日志记录需透传 trace_id

模块结构:
    - processor: 数据处理流水线（清洗、切割、向量化）
    - memory: 三层记忆系统（感觉/工作/长期记忆，完整遗忘机制）
    - rag: RAG 检索增强生成（知识库 + 统一检索）
    - collector: 新闻采集（多源采集、智能去重、向量入库）
    - llm: LLM 增强（Prompt 管理、模型路由、输出解析、缓存）
    - report: 报告生成（早报/午报）

Example:
    from src.processor import ProcessingPipeline, TextCleaner, TextSplitter, Vectorizer
    from src.memory import MemoryManager, memory_manager
    from src.rag import rag_pipeline, KnowledgeAssistant, RetrievalConfig
    from src.collector import news_collector
    from src.llm import llm_service, ModelTier
    
    # 使用 LLM 服务
    await llm_service.initialize()
    result = await llm_service.invoke_template("event_extract", title="...", content="...")
    
    # 使用记忆管理器
    await memory_manager.add_to_working(user_id, content, importance=0.8)
    
    # 使用 RAG 系统
    result = await rag_pipeline.query(user_id, "如何分析筹码峰")
"""

from .processor import (
    BaseProcessor,
    TextCleaner,
    TextSplitter,
    Vectorizer,
    ProcessingPipeline,
    Document,
    DocumentProcessor,
    DocumentVectorizer,
)
from .memory import (
    # 类型
    MemoryType,
    LongTermMemoryType,
    MemoryVisibility,
    DecayStrategy,
    MemoryMetadata,
    BaseMemoryItem,
    SensoryMemoryItem,
    WorkingMemoryItem,
    LongTermMemoryItem,
    # 组件
    SensoryStream,
    AttentionGate,
    WorkingBuffer,
    ContextWindow,
    SemanticStore,
    EpisodicStore,
    ProceduralStore,
    TradingPattern,
    # 流程
    ConsolidationEngine,
    DecayEngine,
    UnifiedRetriever,
    # 管理器
    MemoryManager,
    memory_manager,
)
from .rag import (
    # 基础
    VectorRetriever,
    RAGPipeline,
    # 知识库类型
    KnowledgeCategory,
    FixedKnowledgeCategory,
    UserKnowledgeType,
    KnowledgeItem,
    FixedKnowledgeItem,
    UserKnowledgeItem,
    # 固定知识
    FixedKnowledgeStore,
    KnowledgeLoader,
    # 用户知识
    UserKnowledgeStore,
    KnowledgeAssistant,
    # 统一 Pipeline
    RetrievalSource,
    RetrievalConfig,
    RetrievalItem,
    RAGResult,
    UnifiedRAGPipeline,
    rag_pipeline,
)
from .collector import (
    # 类型
    NewsItem,
    NewsCategory,
    NewsSource,
    CollectResult,
    # 去重
    DeduplicationEngine,
    # 存储
    NewsStorage,
    # 框架基类
    BaseNewsCollector,
    BaseSource,
)
from .llm import (
    # Prompt
    PromptTemplate,
    OutputFormat,
    prompt_registry,
    # 路由
    ModelTier,
    ModelConfig,
    model_router,
    # 解析
    OutputParser,
    output_parser,
    parse_json,
    # 缓存
    LLMCache,
    llm_cache,
    # 服务
    LLMService,
    llm_service,
)

__all__ = [
    # Processor - 基础
    "BaseProcessor",
    "TextCleaner",
    "TextSplitter",
    "Vectorizer",
    "ProcessingPipeline",
    # Processor - 文档
    "Document",
    "DocumentProcessor",
    "DocumentVectorizer",
    # Memory - 类型
    "MemoryType",
    "LongTermMemoryType",
    "MemoryVisibility",
    "DecayStrategy",
    "MemoryMetadata",
    "BaseMemoryItem",
    "SensoryMemoryItem",
    "WorkingMemoryItem",
    "LongTermMemoryItem",
    # Memory - 组件
    "SensoryStream",
    "AttentionGate",
    "WorkingBuffer",
    "ContextWindow",
    "SemanticStore",
    "EpisodicStore",
    "ProceduralStore",
    "TradingPattern",
    # Memory - 流程
    "ConsolidationEngine",
    "DecayEngine",
    "UnifiedRetriever",
    # Memory - 管理器
    "MemoryManager",
    "memory_manager",
    # RAG - 基础
    "VectorRetriever",
    "RAGPipeline",
    # RAG - 知识库类型
    "KnowledgeCategory",
    "FixedKnowledgeCategory",
    "UserKnowledgeType",
    "KnowledgeItem",
    "FixedKnowledgeItem",
    "UserKnowledgeItem",
    # RAG - 固定知识
    "FixedKnowledgeStore",
    "KnowledgeLoader",
    # RAG - 用户知识
    "UserKnowledgeStore",
    "KnowledgeAssistant",
    # RAG - 统一 Pipeline
    "RetrievalSource",
    "RetrievalConfig",
    "RetrievalItem",
    "RAGResult",
    "UnifiedRAGPipeline",
    "rag_pipeline",
    # Collector - 类型
    "NewsItem",
    "NewsSource",
    "NewsCategory",
    "CollectResult",
    # Collector - 去重
    "DeduplicationEngine",
    # Collector - 存储
    "NewsStorage",
    # Collector - 框架基类
    "BaseNewsCollector",
    "BaseSource",
    # LLM - Prompt
    "PromptTemplate",
    "OutputFormat",
    "prompt_registry",
    # LLM - 路由
    "ModelTier",
    "ModelConfig",
    "model_router",
    # LLM - 解析
    "OutputParser",
    "output_parser",
    "parse_json",
    # LLM - 缓存
    "LLMCache",
    "llm_cache",
    # LLM - 服务
    "LLMService",
    "llm_service",
]
