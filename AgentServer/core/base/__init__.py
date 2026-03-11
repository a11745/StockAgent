"""
基类定义

所有 Manager、Node、Collector、Task、Generator、Tool 的基类。
"""

from .manager import BaseManager
from .node import BaseNode, TraceContextFilter, trace_filter
from .scheduled_job import ScheduledJob
from .collector import BaseCollector
from .task import BaseTask
from .generator import BaseGenerator
from .tool import BaseTool, ToolResult

__all__ = [
    # Manager
    "BaseManager",
    # Node
    "BaseNode",
    "TraceContextFilter",
    "trace_filter",
    # Scheduled Jobs
    "ScheduledJob",
    "BaseCollector",
    "BaseTask",
    "BaseGenerator",
    # Tool
    "BaseTool",
    "ToolResult",
]
