"""
Prompt 管理模块
"""

from .template import PromptTemplate, OutputFormat, create_json_output_instruction
from .registry import (
    PromptRegistry,
    prompt_registry,
    # 内置模板
    EVENT_EXTRACT_TEMPLATE,
    IMPORTANCE_ASSESS_TEMPLATE,
    REPORT_SUMMARY_TEMPLATE,
    REPORT_OVERVIEW_TEMPLATE,
    STOCK_ANALYSIS_TEMPLATE,
)


__all__ = [
    "PromptTemplate",
    "OutputFormat",
    "create_json_output_instruction",
    "PromptRegistry",
    "prompt_registry",
    # 内置模板
    "EVENT_EXTRACT_TEMPLATE",
    "IMPORTANCE_ASSESS_TEMPLATE",
    "REPORT_SUMMARY_TEMPLATE",
    "REPORT_OVERVIEW_TEMPLATE",
    "STOCK_ANALYSIS_TEMPLATE",
]
