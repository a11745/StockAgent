"""
Prompt 注册表

集中管理所有 Prompt 模板:
- 自动加载模板文件
- 按名称/任务类型检索
- 版本管理
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from .template import PromptTemplate, OutputFormat


logger = logging.getLogger(__name__)


# 模板目录
TEMPLATES_DIR = Path(__file__).parent / "templates"


class PromptRegistry:
    """
    Prompt 注册表
    
    Example:
        registry = PromptRegistry()
        registry.load_templates()
        
        template = registry.get("event_extract")
        rendered = template.render(title="xxx", content="yyy")
    """
    
    _instance: Optional["PromptRegistry"] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._templates = {}
            cls._instance._loaded = False
        return cls._instance
    
    def __init__(self):
        self._templates: Dict[str, PromptTemplate] = {}
        self._loaded: bool = False
    
    def load_templates(self, templates_dir: Optional[Path] = None) -> int:
        """
        加载模板目录下的所有 YAML 文件
        
        Returns:
            加载的模板数量
        """
        if templates_dir is None:
            templates_dir = TEMPLATES_DIR
        
        if not templates_dir.exists():
            logger.warning(f"Templates directory not found: {templates_dir}")
            return 0
        
        count = 0
        for yaml_file in templates_dir.glob("*.yaml"):
            try:
                template = PromptTemplate.from_file(yaml_file)
                self.register(template)
                count += 1
                logger.debug(f"Loaded template: {template.name} v{template.version}")
            except Exception as e:
                logger.error(f"Failed to load template {yaml_file}: {e}")
        
        self._loaded = True
        logger.info(f"Loaded {count} prompt templates")
        return count
    
    def register(self, template: PromptTemplate) -> None:
        """注册模板"""
        self._templates[template.name] = template
    
    def get(self, name: str) -> Optional[PromptTemplate]:
        """获取模板"""
        if not self._loaded:
            self.load_templates()
        return self._templates.get(name)
    
    def get_or_raise(self, name: str) -> PromptTemplate:
        """获取模板，不存在则抛出异常"""
        template = self.get(name)
        if template is None:
            raise KeyError(f"Template not found: {name}")
        return template
    
    def list_templates(self) -> List[str]:
        """列出所有模板名称"""
        if not self._loaded:
            self.load_templates()
        return list(self._templates.keys())
    
    def list_by_task(self, task_prefix: str) -> List[PromptTemplate]:
        """按任务前缀列出模板"""
        if not self._loaded:
            self.load_templates()
        return [
            t for name, t in self._templates.items()
            if name.startswith(task_prefix)
        ]


# 全局单例
prompt_registry = PromptRegistry()


# ==================== 内置模板 (代码定义) ====================

# 事件提取模板
EVENT_EXTRACT_TEMPLATE = PromptTemplate(
    name="event_extract",
    version="1.0",
    description="从新闻中提取事件指纹",
    system_prompt="你是一位专业的财经分析师，擅长从新闻中提取关键事件信息。",
    user_prompt="""分析以下新闻，提取事件指纹信息。

新闻标题: {title}
新闻内容: {content}

请提取以下信息 (JSON格式):
{{
    "subject": "事件主体 (公司名/行业/政策名称)",
    "action": "核心动作 (如: 发布, 上涨, 下跌, 收购, 推出, 暂停)",
    "time_ref": "时间参照 (如: 今日, 本周, 2024Q1, 近期)",
    "keywords": ["关键词1", "关键词2", "关键词3"],
    "importance": "high/medium/low",
    "summary": "一句话事件摘要 (20字以内)",
    "category": "事件分类 (policy/company/industry/market/tech)"
}}

只返回JSON，不要其他内容。""",
    variables=["title", "content"],
    output_format=OutputFormat.JSON,
    output_schema={
        "type": "object",
        "properties": {
            "subject": {"type": "string"},
            "action": {"type": "string"},
            "time_ref": {"type": "string"},
            "keywords": {"type": "array", "items": {"type": "string"}},
            "importance": {"type": "string", "enum": ["high", "medium", "low"]},
            "summary": {"type": "string"},
            "category": {"type": "string"},
        },
        "required": ["subject", "action", "summary"],
    },
    model_preference="fast",
    temperature=0.1,
    max_tokens=512,
)

# 重要性评估模板
IMPORTANCE_ASSESS_TEMPLATE = PromptTemplate(
    name="importance_assess",
    version="1.0",
    description="批量评估事件重要性",
    system_prompt="你是一位资深财经分析师，请评估财经事件的重要性。",
    user_prompt="""评估以下财经事件的重要性。

事件列表:
{events_json}

请为每个事件评估重要性等级:
- high: 重大事件，影响市场走势或特定板块
- medium: 一般事件，有一定参考价值
- low: 次要事件，影响有限

返回 JSON 格式:
```json
{{
  "results": [
    {{"event_id": "xxx", "importance": "high/medium/low", "reason": "简短原因"}}
  ]
}}
```

只返回 JSON，不要其他内容。""",
    variables=["events_json"],
    output_format=OutputFormat.JSON,
    model_preference="fast",
    temperature=0.3,
    max_tokens=2048,
)

# 报告摘要模板
REPORT_SUMMARY_TEMPLATE = PromptTemplate(
    name="report_summary",
    version="1.0",
    description="生成报告段落摘要",
    system_prompt="你是一位财经编辑，擅长撰写简洁专业的财经摘要。",
    user_prompt="""为以下{category}类事件撰写一段简洁的汇总（50-100字）。

事件列表:
{events_text}

要求:
1. 突出最重要的1-2个事件
2. 语言简洁专业
3. 不要使用"首先"、"其次"等词
4. 直接输出汇总内容，不要有前缀""",
    variables=["category", "events_text"],
    output_format=OutputFormat.TEXT,
    model_preference="balanced",
    temperature=0.5,
    max_tokens=256,
)

# 报告概述模板
REPORT_OVERVIEW_TEMPLATE = PromptTemplate(
    name="report_overview",
    version="1.0",
    description="生成报告总体概述",
    system_prompt="你是一位财经编辑，擅长撰写市场概述。",
    user_prompt="""为今日{report_type}撰写一段总体概述（80-120字）。

今日要点:
- 宏观政策: {macro_summary}
- 行业动态: {industry_summary}
- 个股异动: {stock_summary}
- 热点事件: {hot_summary}

要求:
1. 概括今日市场整体情况
2. 突出最值得关注的1-2个方向
3. 语言简洁专业
4. 直接输出概述内容""",
    variables=["report_type", "macro_summary", "industry_summary", "stock_summary", "hot_summary"],
    output_format=OutputFormat.TEXT,
    model_preference="balanced",
    temperature=0.5,
    max_tokens=256,
)

# 股票分析模板
STOCK_ANALYSIS_TEMPLATE = PromptTemplate(
    name="stock_analysis",
    version="1.0",
    description="个股综合分析",
    system_prompt="""你是一位专业的股票分析师，擅长从多维度分析股票:
- 基本面分析 (财务指标、估值)
- 技术面分析 (K线形态、趋势)
- 资金面分析 (主力资金、北向资金)
- 消息面分析 (新闻、公告)""",
    user_prompt="""分析股票 {ts_code} ({name})

基本面数据:
{fundamental_data}

技术面数据:
{technical_data}

资金流向:
{money_flow}

近期新闻:
{recent_news}

请给出:
1. 综合评分 (1-10)
2. 核心观点 (3句话以内)
3. 风险提示
4. 操作建议 (买入/观望/卖出)""",
    variables=["ts_code", "name", "fundamental_data", "technical_data", "money_flow", "recent_news"],
    output_format=OutputFormat.MARKDOWN,
    model_preference="quality",
    temperature=0.3,
    max_tokens=1024,
)


# 注册内置模板
def register_builtin_templates():
    """注册所有内置模板"""
    builtin = [
        EVENT_EXTRACT_TEMPLATE,
        IMPORTANCE_ASSESS_TEMPLATE,
        REPORT_SUMMARY_TEMPLATE,
        REPORT_OVERVIEW_TEMPLATE,
        STOCK_ANALYSIS_TEMPLATE,
    ]
    for template in builtin:
        prompt_registry.register(template)
    logger.info(f"Registered {len(builtin)} builtin templates")


# 自动注册
register_builtin_templates()
