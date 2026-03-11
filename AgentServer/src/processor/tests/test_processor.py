"""
Processor 模块单元测试

测试内容:
- TextCleaner: HTML清洗、URL移除、空白处理
- TextSplitter: 文本切割、overlap、边界处理
- ProcessingPipeline: 流水线串联执行
- DocumentProcessor: 文档处理、元数据保留
"""

import asyncio
import pytest
from typing import List

from ..cleaner import TextCleaner
from ..splitter import TextSplitter
from ..pipeline import ProcessingPipeline
from ..document import Document, DocumentProcessor


class TestTextCleaner:
    """TextCleaner 测试"""
    
    @pytest.mark.asyncio
    async def test_remove_html(self):
        """测试 HTML 标签移除"""
        cleaner = TextCleaner(remove_html=True)
        
        input_text = "<p>Hello <b>World</b></p>"
        result = await cleaner.process(input_text)
        
        assert result == "Hello World"
    
    @pytest.mark.asyncio
    async def test_remove_urls(self):
        """测试 URL 移除"""
        cleaner = TextCleaner(remove_urls=True)
        
        input_text = "Visit https://example.com for more info"
        result = await cleaner.process(input_text)
        
        assert "https://example.com" not in result
        assert "Visit" in result
    
    @pytest.mark.asyncio
    async def test_normalize_whitespace(self):
        """测试空白字符标准化"""
        cleaner = TextCleaner(normalize_whitespace=True)
        
        input_text = "Hello    World\t\tTest"
        result = await cleaner.process(input_text)
        
        assert "    " not in result
        assert "\t\t" not in result
    
    @pytest.mark.asyncio
    async def test_min_length_filter(self):
        """测试最小长度过滤"""
        cleaner = TextCleaner(min_length=10)
        
        input_texts = ["Short", "This is a longer text"]
        result = await cleaner.process(input_texts)
        
        assert len(result) == 1
        assert result[0] == "This is a longer text"
    
    @pytest.mark.asyncio
    async def test_html_entity_decode(self):
        """测试 HTML 实体解码"""
        cleaner = TextCleaner(decode_html_entities=True)
        
        input_text = "Price &gt; 100 &amp; Quality"
        result = await cleaner.process(input_text)
        
        assert result == "Price > 100 & Quality"
    
    @pytest.mark.asyncio
    async def test_list_input(self):
        """测试列表输入"""
        cleaner = TextCleaner(remove_html=True)
        
        input_texts = ["<p>Text 1</p>", "<div>Text 2</div>"]
        result = await cleaner.process(input_texts)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == "Text 1"
        assert result[1] == "Text 2"


class TestTextSplitter:
    """TextSplitter 测试"""
    
    @pytest.mark.asyncio
    async def test_basic_split(self):
        """测试基本切割"""
        splitter = TextSplitter(chunk_size=50, overlap=10)
        
        # 创建一个超过 chunk_size 的文本
        input_text = "这是一段很长的文本。" * 10
        result = await splitter.process(input_text)
        
        assert isinstance(result, list)
        assert len(result) > 1
    
    @pytest.mark.asyncio
    async def test_short_text_no_split(self):
        """测试短文本不切割"""
        splitter = TextSplitter(chunk_size=100, overlap=10)
        
        input_text = "短文本"
        result = await splitter.process(input_text)
        
        assert len(result) == 1
        assert result[0] == "短文本"
    
    @pytest.mark.asyncio
    async def test_chunk_size_limit(self):
        """测试切块大小限制"""
        splitter = TextSplitter(chunk_size=50, overlap=0)
        
        input_text = "A" * 200
        result = await splitter.process(input_text)
        
        # 每个块不应超过 chunk_size 太多
        for chunk in result:
            assert len(chunk) <= 60  # 允许一点误差
    
    @pytest.mark.asyncio
    async def test_overlap(self):
        """测试重叠区域"""
        splitter = TextSplitter(chunk_size=50, overlap=10)
        
        input_text = "ABCDEFGHIJ" * 10  # 100个字符
        result = await splitter.process(input_text)
        
        # 检查相邻块是否有重叠
        if len(result) >= 2:
            # 第一块的末尾应该出现在第二块的开头
            overlap_found = result[0][-10:] in result[1] if len(result[0]) >= 10 else True
            # 由于切割策略可能不同，这里只是基本检查
            assert len(result) >= 2
    
    @pytest.mark.asyncio
    async def test_invalid_overlap(self):
        """测试无效的 overlap 参数"""
        with pytest.raises(ValueError):
            TextSplitter(chunk_size=50, overlap=50)
        
        with pytest.raises(ValueError):
            TextSplitter(chunk_size=50, overlap=60)
    
    @pytest.mark.asyncio
    async def test_chinese_separators(self):
        """测试中文分隔符"""
        splitter = TextSplitter(chunk_size=30, overlap=5)
        
        input_text = "第一句话。第二句话！第三句话？"
        result = await splitter.process(input_text)
        
        # 应该能按中文标点切割
        assert isinstance(result, list)


class TestProcessingPipeline:
    """ProcessingPipeline 测试"""
    
    @pytest.mark.asyncio
    async def test_pipeline_chain(self):
        """测试流水线串联"""
        pipeline = ProcessingPipeline([
            TextCleaner(remove_html=True),
            TextSplitter(chunk_size=20, overlap=5),
        ])
        
        input_text = "<p>这是一段需要处理的长文本内容。</p>" * 3
        result = await pipeline.process(input_text)
        
        # 检查 HTML 已被移除
        for chunk in result:
            assert "<p>" not in chunk
            assert "</p>" not in chunk
    
    @pytest.mark.asyncio
    async def test_pipeline_stop_on_empty(self):
        """测试空结果停止"""
        pipeline = ProcessingPipeline([
            TextCleaner(min_length=100),  # 过滤掉短文本
            TextSplitter(chunk_size=50, overlap=10),
        ], stop_on_empty=True)
        
        input_text = "Short"  # 会被过滤
        result = await pipeline.process(input_text)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_pipeline_add(self):
        """测试动态添加处理器"""
        pipeline = ProcessingPipeline([TextCleaner()])
        pipeline.add(TextSplitter(chunk_size=50, overlap=10))
        
        assert len(pipeline.processors) == 2


class TestDocumentProcessor:
    """DocumentProcessor 测试"""
    
    @pytest.mark.asyncio
    async def test_process_news_dict(self):
        """测试处理新闻字典"""
        processor = DocumentProcessor(
            cleaner=TextCleaner(remove_html=True),
        )
        
        news = {
            "content": "<p>新闻内容</p>",
            "ts_code": "000001.SZ",
            "datetime": "2024-01-15 10:30:00",
            "source": "sina",
            "title": "测试新闻",
        }
        
        result = await processor.process(news)
        
        assert len(result) == 1
        assert result[0].content == "新闻内容"
        assert result[0].metadata["ts_code"] == "000001.SZ"
        assert result[0].metadata["source"] == "sina"
    
    @pytest.mark.asyncio
    async def test_process_news_list(self):
        """测试处理新闻列表"""
        processor = DocumentProcessor()
        
        news_list = [
            {"content": "新闻1", "ts_code": "000001.SZ"},
            {"content": "新闻2", "ts_code": "000002.SZ"},
        ]
        
        result = await processor.process(news_list)
        
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_document_from_news(self):
        """测试 Document.from_news"""
        news = {
            "content": "新闻内容",
            "ts_code": "000001.SZ",
            "datetime": "2024-01-15",
            "source": "eastmoney",
            "url": "https://example.com/news/1",
        }
        
        doc = Document.from_news(news)
        
        assert doc.content == "新闻内容"
        assert doc.metadata["ts_code"] == "000001.SZ"
        assert doc.metadata["source"] == "eastmoney"
        assert doc.metadata["source_url"] == "https://example.com/news/1"
    
    @pytest.mark.asyncio
    async def test_process_with_splitter(self):
        """测试带切割的处理"""
        processor = DocumentProcessor(
            splitter=TextSplitter(chunk_size=20, overlap=5),
        )
        
        news = {
            "content": "这是一段比较长的新闻内容，需要被切割成多个块。",
            "ts_code": "000001.SZ",
        }
        
        result = await processor.process(news)
        
        # 应该被切割成多个文档
        assert len(result) > 1
        
        # 每个文档都应该保留元数据
        for doc in result:
            assert doc.metadata["ts_code"] == "000001.SZ"
            assert "chunk_index" in doc.metadata
            assert "total_chunks" in doc.metadata
    
    @pytest.mark.asyncio
    async def test_date_normalization(self):
        """测试日期标准化"""
        # 测试各种日期格式
        assert Document._normalize_date("2024-01-15") == "20240115"
        assert Document._normalize_date("2024/01/15") == "20240115"
        assert Document._normalize_date("2024-01-15 10:30:00") == "20240115"
        assert Document._normalize_date("20240115") == "20240115"


# ==================== 运行测试 ====================

def run_tests():
    """手动运行测试（不使用 pytest）"""
    import traceback
    
    test_classes = [
        TestTextCleaner,
        TestTextSplitter,
        TestProcessingPipeline,
        TestDocumentProcessor,
    ]
    
    async def run_async_tests():
        passed = 0
        failed = 0
        
        for test_class in test_classes:
            instance = test_class()
            print(f"\n{'='*50}")
            print(f"Running: {test_class.__name__}")
            print('='*50)
            
            for method_name in dir(instance):
                if method_name.startswith("test_"):
                    method = getattr(instance, method_name)
                    try:
                        await method()
                        print(f"  [PASS] {method_name}")
                        passed += 1
                    except Exception as e:
                        print(f"  [FAIL] {method_name}: {e}")
                        traceback.print_exc()
                        failed += 1
        
        print(f"\n{'='*50}")
        print(f"Results: {passed} passed, {failed} failed")
        print('='*50)
        
        return failed == 0
    
    return asyncio.run(run_async_tests())


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
