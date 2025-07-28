# 知识图谱实时保存优化系统

## 系统概述

本系统实现了知识图谱构建过程中的实时保存和增量持久化功能，确保数据安全和处理的连续性。主要特性包括：

- ✅ **三元组实时保存**: 每次提取的三元组立即保存到文件
- ✅ **知识图谱增量保存**: 处理每个URL后自动保存图谱
- ✅ **自动备份机制**: 定期创建图谱备份文件
- ✅ **详细日志记录**: 完整的处理过程和统计信息
- ✅ **错误恢复能力**: 支持从备份和日志恢复数据
- ✅ **性能监控**: 实时显示处理进度和统计信息

## 核心组件

### 1. EnhancedGraphManager (增强图谱管理器)

**文件**: `graph_module/enhanced_graph_manager.py`

**主要功能**:
- 自动保存: 每处理指定数量的URL后自动保存图谱
- 备份管理: 自动创建带时间戳的备份文件
- 日志记录: 详细记录每次添加的三元组
- 统计信息: 提供实时的图谱状态统计
- 错误恢复: 支持从日志文件恢复三元组

**关键方法**:
```python
def add_triples_with_logging(triples, source_url):
    """添加三元组并记录日志，支持自动保存"""

def _auto_save():
    """自动保存图谱和创建备份"""

def force_save():
    """强制保存图谱和统计信息"""

def get_statistics():
    """获取详细的图谱统计信息"""
```

### 2. EnhancedLLMExtractor (增强事实提取器)

**文件**: `graph_module/enhanced_fact_extractor.py`

**主要功能**:
- 实时保存: 每次提取的三元组立即保存到会话文件
- 会话管理: 创建带时间戳的会话文件
- 统计跟踪: 详细的关系类型和提取统计
- 错误处理: 完善的错误处理和重试机制

**关键方法**:
```python
def extract_triples_from_blocks(text_blocks, source_url):
    """提取三元组并实时保存到文件"""

def _save_triples_to_file(triples, source_url):
    """将三元组保存到当前会话文件"""

def save_session_summary():
    """保存会话总结和统计信息"""
```

### 3. 优化配置系统

**文件**: `config/optimized_config.py`

**配置参数**:
```python
# 实时保存配置
ENABLE_REAL_TIME_SAVE = True
AUTO_SAVE_FREQUENCY = 1  # 每处理多少个URL自动保存图谱
TRIPLE_SAVE_FORMAT = "jsonl"  # 三元组保存格式

# 备份配置
ENABLE_AUTO_BACKUP = True
BACKUP_FREQUENCY = 10  # 每处理多少个URL创建备份
MAX_BACKUP_COUNT = 5   # 最大备份文件数量

# 性能优化配置
BATCH_SIZE = 50        # 批处理大小
MEMORY_THRESHOLD = 0.8  # 内存使用阈值
PROGRESS_REPORT_FREQUENCY = 1  # 进度报告频率
```

## 文件结构

### 数据保存目录
```
data/
├── knowledge_graph.gpickle          # 主知识图谱文件
├── triples_log.jsonl                # 三元组添加日志
├── progress_report.json             # 处理进度报告
├── extracted_triples/               # 三元组会话文件目录
│   ├── triples_session_YYYYMMDD_HHMMSS.jsonl
│   └── ...
├── backups/                         # 图谱备份目录
│   ├── graph_backup_YYYYMMDD_HHMMSS_Nurls.gpickle
│   └── ...
└── session_logs/                    # 会话日志目录
    ├── session_summary_YYYYMMDD_HHMMSS.json
    └── ...
```

### 日志文件格式

**三元组日志** (`triples_log.jsonl`):
```json
{
  "timestamp": "2025-07-22T11:20:54.659540",
  "source_url": "https://example.com/apple",
  "triples_count": 6,
  "triples": [
    {"subject": "苹果公司", "relation": "位于", "object": "加利福尼亚州库比蒂诺"},
    {"subject": "苹果公司", "relation": "成立于", "object": "1976年4月1日"}
  ]
}
```

**会话文件** (`extracted_triples/triples_session_*.jsonl`):
```json
{
  "timestamp": "2025-07-22T11:20:54.659540",
  "source_url": "https://example.com/apple",
  "extracted_triples": [
    {"subject": "苹果公司", "relation": "位于", "object": "加利福尼亚州库比蒂诺"}
  ],
  "extraction_metadata": {
    "model": "deepseek-chat-v3",
    "text_blocks_count": 1,
    "success": true
  }
}
```

## 使用方法

### 1. 基本使用

**优化的主流程脚本**:
```bash
python optimized_main.py --build-graph --max-urls 100
```

**参数说明**:
- `--build-graph`: 启动知识图谱构建
- `--max-urls N`: 限制处理的URL数量
- `--save-frequency N`: 设置保存频率
- `--no-auto-save`: 禁用自动保存
- `--status`: 显示当前状态

### 2. 从备份恢复

```bash
python optimized_main.py --resume-from data/backups/graph_backup_20250722_112055_3urls.gpickle
```

### 3. 状态检查

```bash
python optimized_main.py --status
```

### 4. 演示脚本

**模拟演示**（不需要LLM API）:
```bash
python mock_demo_real_time_saving.py
```

**完整演示**（需要LLM API）:
```bash
python demo_real_time_saving.py
```

## 主要优势

### 1. 数据安全性
- **实时保存**: 提取的三元组立即保存，防止数据丢失
- **自动备份**: 定期创建备份文件，支持多版本保存
- **详细日志**: 完整记录处理过程，支持问题追踪和恢复

### 2. 处理连续性
- **增量处理**: 支持中断后从上次位置继续处理
- **错误恢复**: 从日志和备份文件恢复数据
- **状态监控**: 实时显示处理进度和统计信息

### 3. 性能优化
- **批量处理**: 支持批量处理和优化的保存策略
- **内存管理**: 监控内存使用，适时触发保存
- **并发支持**: 支持多进程处理和数据同步

### 4. 灵活配置
- **可配置参数**: 保存频率、备份策略等均可配置
- **多种模式**: 支持实时模式、批处理模式等
- **扩展性**: 易于扩展和定制化

## 性能指标

基于演示结果的性能指标：

- **处理速度**: 平均6.3个三元组/URL
- **保存效率**: 每个URL处理后立即保存（<0.02秒）
- **存储效率**: 压缩的图谱文件，合理的日志格式
- **错误率**: 完善的错误处理，高可靠性

## 监控和维护

### 1. 文件监控
定期检查以下文件的大小和完整性：
- 主图谱文件 (`knowledge_graph.gpickle`)
- 三元组日志 (`triples_log.jsonl`)
- 备份文件目录 (`backups/`)

### 2. 性能监控
- 内存使用量
- 磁盘空间使用
- 处理速度和错误率

### 3. 清理维护
- 定期清理过期的会话文件
- 管理备份文件数量
- 压缩或归档历史日志

## 故障恢复

### 1. 从最新备份恢复
```bash
python optimized_main.py --resume-from data/backups/latest_backup.gpickle
```

### 2. 从日志重建
```python
graph_manager = EnhancedGraphManager()
graph_manager.recover_from_log()
```

### 3. 检查数据完整性
```bash
python optimized_main.py --status
```

## 扩展功能

### 1. 分布式处理
- 支持多进程并行处理
- 数据同步和冲突解决
- 负载均衡和任务分配

### 2. 云存储集成
- 自动上传备份到云存储
- 跨平台数据同步
- 灾难恢复机制

### 3. 可视化监控
- Web界面的实时监控
- 图谱可视化和统计图表
- 告警和通知机制

## 总结

本实时保存优化系统为知识图谱构建提供了完整的数据保护和处理连续性解决方案。通过实时保存、自动备份、详细日志和错误恢复机制，确保了大规模知识图谱构建过程的稳定性和可靠性。

系统具有良好的扩展性和可配置性，可以根据具体需求调整保存策略和性能参数，适应不同规模和复杂度的知识图谱构建任务。
