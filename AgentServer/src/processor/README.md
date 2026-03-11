# Processor 模块

数据处理流水线模块，提供文本清洗、切割、向量化等数据预处理功能。

## 目录结构

```
processor/
├── __init__.py          # 模块入口
├── base.py              # BaseProcessor 抽象基类
├── cleaner.py           # TextCleaner 文本清洗器
├── splitter.py          # TextSplitter 文本切割器
├── vectorizer.py        # Vectorizer 向量化器
├── pipeline.py          # ProcessingPipeline 流水线编排器
├── document.py          # Document 文档处理器
├── README.md            # 本文档
└── tests/
    └── test_processor.py  # 单元测试
```

## 核心组件

### 1. BaseProcessor (base.py)

所有处理器的抽象基类，定义了统一的接口。

```python
from src.processor import BaseProcessor

class MyProcessor(BaseProcessor):
    async def process(self, data, trace_id=None, **kwargs):
        # 自定义处理逻辑
        return processed_data
```

**特性：**
- 抽象方法 `process()` 必须由子类实现
- 内置 `_log()` 方法支持 `trace_id` 透传
- 自动创建以处理器名称命名的 logger

---

### 2. TextCleaner (cleaner.py)

文本清洗处理器，支持多种清洗策略。

```python
from src.processor import TextCleaner

cleaner = TextCleaner(
    remove_html=True,           # 去除 HTML 标签
    remove_urls=True,           # 去除 URL
    normalize_whitespace=True,  # 标准化空白字符
    strip_extra_newlines=True,  # 压缩多余换行
    decode_html_entities=True,  # 解码 HTML 实体 (&gt; -> >)
    min_length=10,              # 最小长度过滤
)

# 处理单个文本
result = await cleaner.process("<p>Hello World</p>", trace_id="xxx")
# 返回: "Hello World"

# 处理文本列表
results = await cleaner.process(["<p>Text 1</p>", "<div>Text 2</div>"])
# 返回: ["Text 1", "Text 2"]
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `remove_html` | bool | True | 去除 HTML 标签 |
| `remove_urls` | bool | True | 去除 URL 链接 |
| `normalize_whitespace` | bool | True | 将连续空格/制表符合并为单个空格 |
| `strip_extra_newlines` | bool | True | 将 3+ 个换行压缩为 2 个 |
| `decode_html_entities` | bool | True | 解码 HTML 实体 |
| `min_length` | int | 0 | 最小文本长度，低于此值被过滤 |

---

### 3. TextSplitter (splitter.py)

文本切割处理器，使用递归字符切割策略，支持 Overlap。

```python
from src.processor import TextSplitter

splitter = TextSplitter(
    chunk_size=512,    # 每块目标大小（字符数）
    overlap=50,        # 相邻块重叠大小
    separators=None,   # 自定义分隔符列表
    keep_separator=True,  # 是否保留分隔符
)

chunks = await splitter.process("长文本内容...", trace_id="xxx")
# 返回: ["chunk1...", "chunk2...", ...]
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `chunk_size` | int | 512 | 每块的目标大小（字符数） |
| `overlap` | int | 50 | 相邻块之间的重叠大小 |
| `separators` | List[str] | 见下文 | 分隔符列表，按优先级排序 |
| `keep_separator` | bool | True | 切割后是否保留分隔符 |

**默认分隔符（按优先级）：**
```python
["\n\n", "\n", "。", "！", "？", ".", "!", "?", " ", ""]
```

**切割策略：**
1. 优先使用高优先级分隔符（如段落分隔 `\n\n`）
2. 如果切割后仍超过 `chunk_size`，递归使用下一级分隔符
3. 最终使用空字符串按字符强制切割
4. 每个新块开头包含上一块末尾的 `overlap` 个字符

---

### 4. Vectorizer (vectorizer.py)

向量化处理器，封装 `llm_manager.embedding()` 方法。

```python
from src.processor import Vectorizer

vectorizer = Vectorizer(batch_size=32)

vectors = await vectorizer.process(["text1", "text2"], trace_id="xxx")
# 返回: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `batch_size` | int | 32 | 批量处理大小 |

**注意：**
- 使用 `core.managers.llm_manager` 单例
- 延迟导入避免循环依赖
- 自动分批处理大量文本

---

### 5. ProcessingPipeline (pipeline.py)

流水线编排器，将多个处理器串联执行。

```python
from src.processor import ProcessingPipeline, TextCleaner, TextSplitter, Vectorizer

pipeline = ProcessingPipeline([
    TextCleaner(remove_html=True),
    TextSplitter(chunk_size=512, overlap=50),
    Vectorizer(batch_size=32),
], stop_on_empty=True)

# 输入文本，输出向量列表
vectors = await pipeline.process(html_text, trace_id="xxx")

# 动态添加处理器
pipeline.add(AnotherProcessor())
pipeline.insert(0, FirstProcessor())
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `processors` | List[BaseProcessor] | 必填 | 处理器列表 |
| `stop_on_empty` | bool | True | 中间结果为空时是否停止 |

**方法：**

| 方法 | 说明 |
|------|------|
| `add(processor)` | 添加处理器到末尾 |
| `insert(index, processor)` | 在指定位置插入处理器 |

---

### 6. Document & DocumentProcessor (document.py)

处理带元数据的文档，适用于新闻、公告等结构化数据。

#### Document 数据结构

```python
from src.processor import Document

# 从新闻字典创建
doc = Document.from_news({
    "content": "新闻内容",
    "ts_code": "000001.SZ",
    "datetime": "2024-01-15 10:30:00",
    "source": "sina",
    "url": "https://example.com/news/1",
})

# 从公告创建
doc = Document.from_announcement({
    "content": "公告内容",
    "ts_code": "000001.SZ",
    "ann_date": "20240115",
})

# 访问属性
print(doc.content)           # 文本内容
print(doc.metadata)          # 元数据字典
print(doc.chunks)            # 切割后的文本块
print(doc.vectors)           # 向量化结果
```

#### DocumentProcessor

```python
from src.processor import DocumentProcessor, TextCleaner, TextSplitter

processor = DocumentProcessor(
    cleaner=TextCleaner(remove_html=True),
    splitter=TextSplitter(chunk_size=512, overlap=50),
    content_field="content",     # 内容字段名
    metadata_fields=None,        # 要保留的元数据字段
)

# 处理新闻列表
news_list = [
    {"content": "<p>新闻1</p>", "ts_code": "000001.SZ", "source": "sina"},
    {"content": "<p>新闻2</p>", "ts_code": "000002.SZ", "source": "eastmoney"},
]

documents = await processor.process(news_list, trace_id="xxx")

# 每个文档保留元数据
for doc in documents:
    print(doc.content)              # 清洗后的内容
    print(doc.metadata["ts_code"])  # 股票代码
    print(doc.metadata["source"])   # 数据来源
```

#### DocumentVectorizer

```python
from src.processor import DocumentVectorizer

vectorizer = DocumentVectorizer(batch_size=32)

# 为文档生成向量
documents = await vectorizer.process(documents, trace_id="xxx")

# 向量存储在 doc.vectors 中
for doc in documents:
    print(len(doc.vectors[0]))  # 向量维度
```

**默认保留的元数据字段：**
```python
["ts_code", "ts_codes", "publish_date", "source", 
 "source_url", "title", "category", "datetime", "url"]
```

---

## 完整使用示例

### 示例 1: 简单文本处理流水线

```python
from src.processor import ProcessingPipeline, TextCleaner, TextSplitter, Vectorizer

async def process_texts(texts: list[str], trace_id: str):
    pipeline = ProcessingPipeline([
        TextCleaner(remove_html=True, remove_urls=True),
        TextSplitter(chunk_size=512, overlap=50),
        Vectorizer(batch_size=32),
    ])
    
    vectors = await pipeline.process(texts, trace_id=trace_id)
    return vectors
```

### 示例 2: 新闻处理并存入向量库

```python
from src.processor import DocumentProcessor, DocumentVectorizer, TextCleaner, TextSplitter
from src.memory import MilvusStore, MemoryItem

async def process_and_store_news(news_list: list[dict], trace_id: str):
    # 1. 处理新闻
    processor = DocumentProcessor(
        cleaner=TextCleaner(remove_html=True, min_length=50),
        splitter=TextSplitter(chunk_size=512, overlap=50),
    )
    documents = await processor.process(news_list, trace_id=trace_id)
    
    # 2. 向量化
    vectorizer = DocumentVectorizer(batch_size=32)
    documents = await vectorizer.process(documents, trace_id=trace_id)
    
    # 3. 转换为 MemoryItem 并存储
    store = MilvusStore(collection_name="news_memory")
    items = []
    for doc in documents:
        if doc.vectors and doc.vectors[0]:
            item = MemoryItem(
                content=doc.content,
                vector=doc.vectors[0],
                metadata=MemoryMetadata(**doc.metadata),
            )
            items.append(item)
    
    await store.upsert(items, trace_id=trace_id)
    return len(items)
```

### 示例 3: 自定义处理器

```python
from src.processor import BaseProcessor

class SentimentProcessor(BaseProcessor):
    """情感分析处理器"""
    
    async def process(self, data, trace_id=None, **kwargs):
        texts = [data] if isinstance(data, str) else data
        
        self._log("debug", f"Analyzing sentiment for {len(texts)} texts", trace_id)
        
        results = []
        for text in texts:
            # 调用情感分析逻辑
            sentiment = await self._analyze(text)
            results.append({"text": text, "sentiment": sentiment})
        
        return results
    
    async def _analyze(self, text: str) -> str:
        # 实际的情感分析逻辑
        return "positive"

# 在流水线中使用
pipeline = ProcessingPipeline([
    TextCleaner(),
    SentimentProcessor(),
])
```

---

## 设计原则

1. **严禁向上引用**：不引用 `nodes/` 中的任何代码
2. **单例复用**：使用 `core.managers` 中的单例（如 `llm_manager`）
3. **全异步化**：所有 I/O 操作使用 `async/await`
4. **日志透传**：所有方法支持 `trace_id` 参数用于分布式追踪
5. **类型安全**：使用类型注解，支持 IDE 自动补全

---

## 运行测试

```bash
cd AgentServer
python -m src.processor.tests.test_processor
```

预期输出：
```
==================================================
Results: 20 passed, 0 failed
==================================================
```
