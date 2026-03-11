"""
文本切割处理器

实现文本分块逻辑，支持:
- 递归字符切割
- 重叠区域 (Overlap)
- 自定义分隔符
"""

from typing import Any, List, Optional, Union

from .base import BaseProcessor


class TextSplitter(BaseProcessor):
    """
    文本切割处理器
    
    使用递归字符切割策略，支持带 Overlap 的分块。
    
    Args:
        chunk_size: 每块的目标大小（字符数）
        overlap: 相邻块之间的重叠大小
        separators: 分隔符列表，按优先级排序
        keep_separator: 是否保留分隔符
    
    Example:
        splitter = TextSplitter(chunk_size=512, overlap=50)
        chunks = await splitter.process("长文本...", trace_id="xxx")
        # Returns: ["chunk1...", "chunk2...", ...]
    """
    
    DEFAULT_SEPARATORS = ["\n\n", "\n", "。", "！", "？", ".", "!", "?", " ", ""]
    
    def __init__(
        self,
        chunk_size: int = 512,
        overlap: int = 50,
        separators: Optional[List[str]] = None,
        keep_separator: bool = True,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.separators = separators or self.DEFAULT_SEPARATORS
        self.keep_separator = keep_separator
        
        if overlap >= chunk_size:
            raise ValueError(f"overlap ({overlap}) must be smaller than chunk_size ({chunk_size})")
    
    async def process(
        self,
        data: Union[str, List[str]],
        trace_id: Optional[str] = None,
        **kwargs
    ) -> List[str]:
        """
        切割文本
        
        Args:
            data: 单个文本字符串或文本列表
            trace_id: 分布式追踪 ID
            
        Returns:
            切割后的文本块列表
        """
        texts = [data] if isinstance(data, str) else data
        
        all_chunks = []
        for text in texts:
            chunks = self._split_text(text)
            all_chunks.extend(chunks)
        
        self._log(
            "debug", 
            f"Split {len(texts)} texts into {len(all_chunks)} chunks", 
            trace_id
        )
        
        return all_chunks
    
    def _split_text(self, text: str) -> List[str]:
        """递归切割单个文本"""
        if len(text) <= self.chunk_size:
            return [text] if text.strip() else []
        
        return self._recursive_split(text, self.separators)
    
    def _recursive_split(self, text: str, separators: List[str]) -> List[str]:
        """递归使用分隔符切割"""
        if not separators:
            # 无分隔符可用，强制按长度切割
            return self._force_split(text)
        
        separator = separators[0]
        remaining_separators = separators[1:]
        
        if not separator:
            # 空分隔符表示按字符切割
            return self._force_split(text)
        
        if separator not in text:
            # 当前分隔符不存在，尝试下一个
            return self._recursive_split(text, remaining_separators)
        
        # 使用当前分隔符切割
        splits = text.split(separator)
        
        chunks = []
        current_chunk = ""
        
        for i, split in enumerate(splits):
            # 决定是否添加分隔符
            piece = split
            if self.keep_separator and i < len(splits) - 1:
                piece = split + separator
            
            # 检查合并后是否超过限制
            if len(current_chunk) + len(piece) <= self.chunk_size:
                current_chunk += piece
            else:
                # 当前块已满
                if current_chunk:
                    chunks.append(current_chunk.strip())
                
                # 检查单个 piece 是否超过限制
                if len(piece) > self.chunk_size:
                    # 递归处理超长片段
                    sub_chunks = self._recursive_split(piece, remaining_separators)
                    chunks.extend(sub_chunks)
                    current_chunk = ""
                else:
                    # 开始新块，带重叠
                    if chunks and self.overlap > 0:
                        overlap_text = chunks[-1][-self.overlap:]
                        current_chunk = overlap_text + piece
                    else:
                        current_chunk = piece
        
        # 处理最后一块
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _force_split(self, text: str) -> List[str]:
        """按字符强制切割"""
        chunks = []
        start = 0
        
        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            # 下一块的起始位置，考虑重叠
            start = end - self.overlap if end < len(text) else end
        
        return chunks
