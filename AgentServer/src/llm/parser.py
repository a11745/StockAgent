"""
输出解析器

解析 LLM 输出:
- JSON 提取和验证
- Markdown 解析
- 结构化数据提取
"""

import json
import re
import logging
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, ValidationError


logger = logging.getLogger(__name__)


T = TypeVar("T", bound=BaseModel)


class ParseError(Exception):
    """解析错误"""
    def __init__(self, message: str, raw_content: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.raw_content = raw_content
        self.details = details or {}


class OutputParser:
    """
    输出解析器
    
    Example:
        parser = OutputParser()
        
        # 解析 JSON
        data = parser.parse_json(response)
        
        # 解析为 Pydantic 模型
        result = parser.parse_model(response, MyModel)
        
        # 提取代码块
        code = parser.extract_code_block(response, "json")
    """
    
    def parse_json(
        self,
        content: str,
        strict: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        解析 JSON 输出
        
        支持:
        - 纯 JSON
        - Markdown 代码块中的 JSON
        - 混合文本中的 JSON
        
        Args:
            content: LLM 输出内容
            strict: 是否严格模式（失败时抛出异常）
            
        Returns:
            解析后的字典，失败返回 None
        """
        if not content:
            if strict:
                raise ParseError("Empty content", content)
            return None
        
        content = content.strip()
        
        # 1. 尝试直接解析
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        
        # 2. 尝试从 Markdown 代码块提取
        json_str = self.extract_code_block(content, "json")
        if json_str:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        # 3. 尝试提取第一个 JSON 对象
        json_str = self._extract_first_json(content)
        if json_str:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        if strict:
            raise ParseError(
                "Failed to parse JSON from content",
                content,
                {"attempted_extraction": json_str},
            )
        
        logger.warning(f"Failed to parse JSON: {content[:200]}...")
        return None
    
    def parse_json_list(
        self,
        content: str,
        strict: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """解析 JSON 数组"""
        result = self.parse_json(content, strict)
        
        if result is None:
            return None
        
        if isinstance(result, list):
            return result
        
        # 如果是对象，尝试提取常见的列表字段
        for key in ["results", "items", "data", "list"]:
            if key in result and isinstance(result[key], list):
                return result[key]
        
        if strict:
            raise ParseError("Expected JSON array", content)
        
        return None
    
    def parse_model(
        self,
        content: str,
        model: Type[T],
        strict: bool = False,
    ) -> Optional[T]:
        """
        解析为 Pydantic 模型
        
        Args:
            content: LLM 输出
            model: Pydantic 模型类
            strict: 是否严格模式
            
        Returns:
            模型实例
        """
        data = self.parse_json(content, strict)
        
        if data is None:
            return None
        
        try:
            return model(**data)
        except ValidationError as e:
            if strict:
                raise ParseError(
                    f"Validation failed: {e}",
                    content,
                    {"validation_errors": e.errors()},
                )
            logger.warning(f"Model validation failed: {e}")
            return None
    
    def extract_code_block(
        self,
        content: str,
        language: Optional[str] = None,
    ) -> Optional[str]:
        """
        提取 Markdown 代码块
        
        Args:
            content: 内容
            language: 语言标识 (如 "json", "python")
            
        Returns:
            代码块内容
        """
        if language:
            # 匹配特定语言的代码块
            pattern = rf'```{language}\s*([\s\S]*?)\s*```'
        else:
            # 匹配任意代码块
            pattern = r'```(?:\w+)?\s*([\s\S]*?)\s*```'
        
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        return None
    
    def extract_all_code_blocks(
        self,
        content: str,
    ) -> List[Dict[str, str]]:
        """提取所有代码块"""
        pattern = r'```(\w+)?\s*([\s\S]*?)\s*```'
        matches = re.findall(pattern, content)
        
        return [
            {"language": lang or "text", "code": code.strip()}
            for lang, code in matches
        ]
    
    def _extract_first_json(self, content: str) -> Optional[str]:
        """提取第一个 JSON 对象或数组"""
        # 查找 { 或 [ 开头的 JSON
        for start_char, end_char in [("{", "}"), ("[", "]")]:
            start = content.find(start_char)
            if start == -1:
                continue
            
            # 使用栈匹配括号
            depth = 0
            in_string = False
            escape = False
            
            for i, char in enumerate(content[start:], start):
                if escape:
                    escape = False
                    continue
                
                if char == "\\":
                    escape = True
                    continue
                
                if char == '"' and not escape:
                    in_string = not in_string
                    continue
                
                if in_string:
                    continue
                
                if char == start_char:
                    depth += 1
                elif char == end_char:
                    depth -= 1
                    if depth == 0:
                        return content[start:i+1]
        
        return None
    
    def extract_key_value(
        self,
        content: str,
        key: str,
    ) -> Optional[str]:
        """从文本中提取键值对"""
        # 匹配 "key: value" 或 "key：value" 格式
        patterns = [
            rf'{key}\s*[:：]\s*(.+?)(?:\n|$)',
            rf'"{key}"\s*[:：]\s*"?([^"\n]+)"?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def extract_list_items(
        self,
        content: str,
    ) -> List[str]:
        """提取列表项"""
        # 匹配 "- item" 或 "1. item" 格式
        pattern = r'(?:^|\n)\s*(?:[-*•]|\d+\.)\s*(.+)'
        matches = re.findall(pattern, content)
        return [m.strip() for m in matches if m.strip()]
    
    def clean_response(self, content: str) -> str:
        """清理响应内容"""
        # 移除思考过程
        content = re.sub(r'<think>[\s\S]*?</think>', '', content)
        
        # 移除前后空白
        content = content.strip()
        
        # 移除常见的前缀
        prefixes = [
            "好的，",
            "以下是",
            "根据您的要求，",
            "Here is",
            "Sure,",
        ]
        for prefix in prefixes:
            if content.startswith(prefix):
                content = content[len(prefix):].strip()
        
        return content


# 全局单例
output_parser = OutputParser()


# ==================== 便捷函数 ====================

def parse_json(content: str, strict: bool = False) -> Optional[Dict[str, Any]]:
    """解析 JSON"""
    return output_parser.parse_json(content, strict)


def parse_model(content: str, model: Type[T], strict: bool = False) -> Optional[T]:
    """解析为 Pydantic 模型"""
    return output_parser.parse_model(content, model, strict)


def extract_code_block(content: str, language: Optional[str] = None) -> Optional[str]:
    """提取代码块"""
    return output_parser.extract_code_block(content, language)
