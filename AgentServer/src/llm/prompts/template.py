"""
Prompt 模板引擎

支持:
- YAML 模板定义
- 变量替换 (Jinja2)
- 版本管理
- 输出格式验证
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Type
from dataclasses import dataclass, field
from enum import Enum

import yaml
from pydantic import BaseModel


class OutputFormat(str, Enum):
    """输出格式"""
    TEXT = "text"
    JSON = "json"
    MARKDOWN = "markdown"
    LIST = "list"


@dataclass
class PromptTemplate:
    """
    Prompt 模板
    
    Attributes:
        name: 模板名称 (唯一标识)
        version: 版本号
        description: 描述
        system_prompt: 系统提示词
        user_prompt: 用户提示词模板
        variables: 必需变量列表
        output_format: 输出格式
        output_schema: 输出 JSON Schema (用于验证)
        examples: Few-shot 示例
        model_preference: 推荐模型
        temperature: 推荐温度
        max_tokens: 推荐最大 token
    """
    name: str
    version: str = "1.0"
    description: str = ""
    
    system_prompt: str = ""
    user_prompt: str = ""
    
    variables: List[str] = field(default_factory=list)
    output_format: OutputFormat = OutputFormat.TEXT
    output_schema: Optional[Dict[str, Any]] = None
    
    examples: List[Dict[str, str]] = field(default_factory=list)
    
    # 模型推荐
    model_preference: Optional[str] = None  # fast, balanced, quality
    temperature: float = 0.7
    max_tokens: int = 2048
    
    def render(self, **kwargs) -> Dict[str, Any]:
        """
        渲染模板
        
        Args:
            **kwargs: 模板变量
            
        Returns:
            包含 system, user, examples 的字典
        """
        # 检查必需变量
        missing = set(self.variables) - set(kwargs.keys())
        if missing:
            raise ValueError(f"Missing required variables: {missing}")
        
        # 渲染系统提示词
        system = self._render_string(self.system_prompt, kwargs)
        
        # 渲染用户提示词
        user = self._render_string(self.user_prompt, kwargs)
        
        # 构建消息列表
        messages = []
        
        if system:
            messages.append({"role": "system", "content": system})
        
        # 添加 few-shot 示例
        for example in self.examples:
            if "user" in example:
                messages.append({
                    "role": "user",
                    "content": self._render_string(example["user"], kwargs)
                })
            if "assistant" in example:
                messages.append({
                    "role": "assistant",
                    "content": self._render_string(example["assistant"], kwargs)
                })
        
        # 添加实际用户消息
        messages.append({"role": "user", "content": user})
        
        return {
            "messages": messages,
            "model_preference": self.model_preference,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "output_format": self.output_format,
            "output_schema": self.output_schema,
        }
    
    def _render_string(self, template: str, variables: Dict[str, Any]) -> str:
        """简单的变量替换"""
        result = template
        for key, value in variables.items():
            # 支持 {var} 和 {{var}} 格式
            result = result.replace(f"{{{key}}}", str(value))
            result = result.replace(f"{{{{{key}}}}}", str(value))
        return result
    
    @classmethod
    def from_yaml(cls, yaml_content: str) -> "PromptTemplate":
        """从 YAML 内容创建模板"""
        data = yaml.safe_load(yaml_content)
        
        # 处理输出格式
        if "output_format" in data:
            data["output_format"] = OutputFormat(data["output_format"])
        
        return cls(**data)
    
    @classmethod
    def from_file(cls, file_path: Path) -> "PromptTemplate":
        """从 YAML 文件加载模板"""
        with open(file_path, "r", encoding="utf-8") as f:
            return cls.from_yaml(f.read())
    
    def to_yaml(self) -> str:
        """导出为 YAML"""
        data = {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "variables": self.variables,
            "output_format": self.output_format.value,
            "model_preference": self.model_preference,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        
        if self.output_schema:
            data["output_schema"] = self.output_schema
        if self.examples:
            data["examples"] = self.examples
        
        return yaml.dump(data, allow_unicode=True, sort_keys=False)


def create_json_output_instruction(schema_description: str) -> str:
    """生成 JSON 输出指令"""
    return f"""
请严格按照以下 JSON 格式返回结果:
```json
{schema_description}
```

只返回 JSON，不要有其他内容。
"""
