# 知识图谱构建系统

一个基于LLM的知识图谱构建和问答数据集生成系统。

## 项目结构

```
e:\LLM\0720_data\
├── config/                    # 配置模块
│   ├── __init__.py
│   └── config.py             # 主配置文件
├── scraper_module/           # 网页爬取模块
│   ├── __init__.py
│   ├── fetch.py             # 网页获取
│   └── scraper.py           # 爬虫主逻辑
├── graph_module/             # 知识图谱模块
│   ├── __init__.py
│   ├── poster.py            # 文本处理
│   ├── fact_extractor.py    # 事实提取
│   └── graph_manager.py     # 图谱管理
├── generate_module/          # 数据生成模块
│   ├── __init__.py
│   ├── path_sampler.py      # 路径采样
│   ├── question_generator.py # 问题生成
│   └── validator.py         # 验证器
├── tests/                   # 测试模块
│   ├── __init__.py
│   ├── test_api*.py         # API测试
│   ├── test_json*.py        # JSON解析测试
│   ├── test_relation*.py    # 关系处理测试
│   ├── test_*_kg.py         # 知识图谱测试
│   └── test_merge*.py       # 实体合并测试
├── debug/                   # 调试模块
│   ├── __init__.py
│   ├── debug_extraction.py  # 提取调试
│   ├── debug_entity_merging.py # 实体合并调试
│   └── debug_entity_resolution.py # 实体对齐调试
├── tools/                   # 工具模块
│   ├── __init__.py
│   ├── kg_editor.py         # 知识图谱编辑器
│   ├── config_update_advisor.py # 配置建议工具
│   ├── threshold_optimizer.py # 阈值优化工具
│   └── switch_api_mode.py   # API模式切换
├── docs/                    # 文档和演示
│   ├── __init__.py
│   ├── enhanced_kg_demo.py  # 功能演示
│   ├── similarity_analysis.py # 相似度分析
│   ├── solution_summary.py  # 解决方案总结
│   └── OPTIMIZATION_README.md # 优化文档
├── data/                    # 数据目录
│   ├── seed_urls.txt        # 种子URL
│   ├── raw_html/           # 原始HTML文件
│   ├── output/             # 输出数据
│   └── *.gpickle           # 图谱文件
├── main.py                 # 主程序
└── requirements.txt        # 依赖包
```

## 主要功能

### 1. 知识图谱构建
- **网页爬取**: 从种子URL开始爬取相关网页
- **文本处理**: 解析HTML并提取有效文本块
- **事实提取**: 使用LLM从文本中提取三元组
- **实体对齐**: 智能合并相似实体，避免错误合并
- **图谱存储**: 使用NetworkX存储和管理知识图谱

### 2. 问答数据生成
- **路径采样**: 从知识图谱中采样有意义的路径
- **问题生成**: 基于路径生成自然语言问题
- **答案验证**: 验证生成的问答对的质量

### 3. 质量保障
- **实体合并验证**: 防止语义不同的实体错误合并
- **关系类型扩展**: 支持预定义和自定义关系类型
- **JSON解析修复**: 处理LLM响应中的截断和格式问题

## 快速开始

### 1. 环境配置
```bash
pip install -r requirements.txt
```

### 2. 配置API密钥
```bash
# 设置环境变量
set API_KEY=your-api-key-here
```

### 3. 构建知识图谱
```bash
python main.py --build-graph
```

### 4. 生成问答数据
```bash
python main.py --generate-qa
```

## 工具使用

### 知识图谱编辑器
```bash
python tools/kg_editor.py
```
功能：
- 分析潜在错误合并
- 交互式修复错误
- 导出错误报告

### 配置优化建议
```bash
python tools/config_update_advisor.py
```
功能：
- 分析关系使用统计
- 生成配置更新建议
- 预测优化效果

### 阈值优化
```bash
python tools/threshold_optimizer.py
```
功能：
- 分析相似度阈值影响
- 提供阈值调优建议

## 测试和调试

### 运行测试
```bash
# API测试
python tests/test_api.py

# 关系处理测试
python tests/test_relation_flexibility.py

# 实体合并测试
python tests/test_merge_validation.py
```

### 调试问题
```bash
# 实体对齐问题
python debug/debug_entity_resolution.py

# 实体合并问题
python debug/debug_entity_merging.py

# 数据提取问题
python debug/debug_extraction.py
```

## 演示和文档

### 功能演示
```bash
python docs/enhanced_kg_demo.py
```

### 解决方案总结
```bash
python docs/solution_summary.py
```

### 相似度分析
```bash
python docs/similarity_analysis.py
```

## 配置说明

主要配置在 `config/config.py` 中：

- **LLM_API_***: LLM API相关配置
- **SIMILARITY_THRESHOLD**: 实体对齐相似度阈值 (建议0.90)
- **RELATION_TYPES**: 预定义关系类型列表
- **MAX_TOKENS**: LLM最大生成Token数 (建议16384)

## 最近更新

### v2.1 (2025-01-22)
- ✅ 增强实体合并验证逻辑
- ✅ 添加知识图谱后期干预工具
- ✅ 支持灵活关系类型处理
- ✅ 完善JSON解析容错机制
- ✅ 项目结构重组和文档完善

### v2.0
- ✅ 关系类型转换为中文
- ✅ 修复实体错误合并问题
- ✅ 添加关系使用统计功能
- ✅ 优化LLM响应处理

## 技术栈

- **Python 3.8+**
- **NetworkX**: 图谱存储和操作
- **recordlinkage**: 实体对齐
- **jarowinkler**: 字符串相似度
- **requests**: HTTP请求
- **beautifulsoup4**: HTML解析
- **pandas**: 数据处理

## 贡献指南

1. 测试新功能请在 `tests/` 目录添加测试脚本
2. 调试工具请放在 `debug/` 目录
3. 实用工具请放在 `tools/` 目录
4. 文档和演示请放在 `docs/` 目录

## 许可证

[请添加适当的许可证信息]
