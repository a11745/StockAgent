"""
文本清洗处理器

实现文本预处理逻辑，包括:
- HTML 标签去除
- 冗余空格处理
- 特殊字符清理
- Unicode 标准化
"""

import re
import html
from typing import Any, List, Optional, Union

from .base import BaseProcessor


class TextCleaner(BaseProcessor):
    """
    文本清洗处理器
    
    支持单文本或文本列表的清洗处理。
    
    Args:
        remove_html: 是否去除 HTML 标签
        remove_urls: 是否去除 URL
        normalize_whitespace: 是否标准化空白字符
        strip_extra_newlines: 是否压缩多余换行
        decode_html_entities: 是否解码 HTML 实体
        min_length: 最小文本长度，低于此长度的文本将被过滤
    
    Example:
        cleaner = TextCleaner(remove_html=True, min_length=10)
        cleaned = await cleaner.process(["<p>Hello World</p>", "短"], trace_id="xxx")
        # Returns: ["Hello World"]
    """
    
    def __init__(
        self,
        remove_html: bool = True,
        remove_urls: bool = True,
        normalize_whitespace: bool = True,
        strip_extra_newlines: bool = True,
        decode_html_entities: bool = True,
        min_length: int = 0,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.remove_html = remove_html
        self.remove_urls = remove_urls
        self.normalize_whitespace = normalize_whitespace
        self.strip_extra_newlines = strip_extra_newlines
        self.decode_html_entities = decode_html_entities
        self.min_length = min_length
        
        # 预编译正则表达式
        self._html_pattern = re.compile(r'<[^>]+>')
        self._url_pattern = re.compile(
            r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[^\s]*'
        )
        self._whitespace_pattern = re.compile(r'[ \t]+')
        self._newline_pattern = re.compile(r'\n{3,}')
    
    async def process(
        self,
        data: Union[str, List[str]],
        trace_id: Optional[str] = None,
        **kwargs
    ) -> Union[str, List[str]]:
        """
        清洗文本
        
        Args:
            data: 单个文本字符串或文本列表
            trace_id: 分布式追踪 ID
            
        Returns:
            清洗后的文本或文本列表（保持输入类型一致）
        """
        is_single = isinstance(data, str)
        texts = [data] if is_single else data
        
        self._log("debug", f"Cleaning {len(texts)} texts", trace_id)
        
        cleaned = []
        for text in texts:
            result = self._clean_text(text)
            if len(result) >= self.min_length:
                cleaned.append(result)
        
        filtered_count = len(texts) - len(cleaned)
        if filtered_count > 0:
            self._log("debug", f"Filtered {filtered_count} texts below min_length", trace_id)
        
        return cleaned[0] if is_single and cleaned else cleaned
    
    def _clean_text(self, text: str) -> str:
        """执行单个文本的清洗"""
        if not text:
            return ""
        
        result = text
        
        # HTML 实体解码
        if self.decode_html_entities:
            result = html.unescape(result)
        
        # 去除 HTML 标签
        if self.remove_html:
            result = self._html_pattern.sub('', result)
        
        # 去除 URL
        if self.remove_urls:
            result = self._url_pattern.sub('', result)
        
        # 标准化空白字符
        if self.normalize_whitespace:
            result = self._whitespace_pattern.sub(' ', result)
        
        # 压缩多余换行
        if self.strip_extra_newlines:
            result = self._newline_pattern.sub('\n\n', result)
        
        return result.strip()
