# 爬虫优化实现说明

## 概述

本次优化的核心目标是**提高信噪比**和**控制爬取范围**，通过在爬取阶段就过滤掉无关内容，只保留正文信息，从而减小存储开销并提升后续知识抽取的准确性。

## 优化实现详情

### 1. 过滤噪声，保留有效信息

**实现位置**: `scraper.py` - `_clean_content()` 方法

**核心功能**:
- **针对维基百科优化**: 自动识别并提取 `<div id="mw-content-text">` 中的核心内容
- **移除无用元素**: 自动删除参考文献、导航框、编辑链接等噪声内容
- **通用内容提取**: 对于非维基百科网站，尝试识别 `<article>`、`<main>` 等语义化标签
- **智能回退**: 如果找不到特定结构，则移除 `<nav>`、`<header>`、`<footer>` 等常见非内容元素

**技术特点**:
```python
# 移除的元素类型
unwanted_selectors = [
    'ol.references',      # 参考文献
    'div.navbox',         # 导航框
    'table.ambox',        # 消歧义框
    'div.hatnote',        # 帽子注释
    'span.mw-editsection', # 编辑链接
    'div.printfooter',    # 打印页脚
    'div.catlinks',       # 分类链接
]
```

### 2. 基于链接文本的深度遍历过滤

**实现位置**: `scraper.py` - `_should_follow_link()` 方法

**核心功能**:
- **停用词过滤**: 在 `config.py` 中定义停用词列表，过滤包含这些词的链接
- **文本质量检查**: 过滤过短、纯数字或无字母的链接文本
- **智能过滤**: 避免爬取"宏大叙事"、"不确定性"等价值较低的页面

**配置的停用词**:
```python
FILTER_WORDS = [
    '宏大叙事', '不确定性', '争议', '参考文献', '外部链接', 
    '参见', '注释', '脚注', '分类', '隐私政策', '免责声明',
    '版权', '联系我们', '帮助', '用户手册', '使用条款',
    '存档', '讨论', '编辑', '历史', '上传文件', '特殊页面'
]
```

### 3. 最大爬取数量限制

**实现位置**: 
- `config.py` - `MAX_PAGES_TO_CRAWL` 配置项
- `scraper.py` - `Scraper.__init__()` 和 `run()` 方法
- `main.py` - 更新 `Scraper` 调用

**核心功能**:
- **可配置限制**: 默认最大爬取1000个页面，可在配置文件中调整
- **实时计数**: 在爬虫进度条中显示当前进度 `(已爬取/总限制)`
- **智能终止**: 达到限制时优雅地终止爬取过程

## 代码结构改进

### 新增配置项 (`config.py`)
```python
# 最大爬取页面数量限制
MAX_PAGES_TO_CRAWL = 1000

# 链接文本过滤停用词列表
FILTER_WORDS = [...]
```

### 增强的爬虫类 (`scraper.py`)
```python
class Scraper:
    def __init__(self, seed_urls, max_depth, max_pages=None):
        # 支持可选的最大页面数参数
        self.max_pages = max_pages if max_pages is not None else config.MAX_PAGES_TO_CRAWL
        self.crawled_count = 0  # 新增计数器
```

### 更新的主程序 (`main.py`)
```python
# 传递最大页面数参数
scraper = Scraper(seed_urls, config.MAX_DEPTH, config.MAX_PAGES_TO_CRAWL)
```

## 优化效果

### 1. 存储空间优化
- **减少无用内容**: 移除导航、页脚、广告等噪声，存储的HTML文件大小预计减少50-70%
- **提高信噪比**: 保存的内容主要为正文，有利于后续处理

### 2. 爬取效率优化  
- **智能链接过滤**: 避免爬取低价值页面，提高有效页面比例
- **数量控制**: 防止无限制爬取，控制资源消耗

### 3. 后续处理优化
- **提高知识抽取准确性**: 清洁的内容有助于LLM更准确地提取事实和关系
- **减少处理时间**: 较小的文件和更少的噪声内容加快处理速度

## 使用方法

### 1. 基本使用
```bash
python main.py --run_scraper
```

### 2. 自定义配置
修改 `config.py` 中的配置项：
- `MAX_PAGES_TO_CRAWL`: 调整最大爬取页面数
- `FILTER_WORDS`: 添加或删除停用词

### 3. 测试功能
```bash
python test_optimized_scraper.py
```

## 扩展性

### 1. 网站特定优化
可以在 `_clean_content()` 方法中添加针对其他网站的特定清洗逻辑：
```python
# 为新网站添加特定的内容提取逻辑
if 'example.com' in url:
    content_div = soup.find('div', {'class': 'main-content'})
```

### 2. 动态停用词
可以实现动态更新停用词列表的功能，比如从外部文件读取或根据爬取结果自动学习。

### 3. 质量评估
可以添加页面质量评估机制，根据内容长度、链接密度等指标进一步筛选高质量页面。

## 兼容性说明

本次优化完全向后兼容，不会影响现有功能：
- 保持了原有的API接口
- 保留了所有原有功能
- 新增功能均为可选或有合理默认值

所有优化都经过测试验证，确保功能正确性和稳定性。
