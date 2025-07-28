# 实时保存功能使用说明

## 概述

在原有 `main.py` 的基础上，新增了实时保存三元组和知识图谱的功能。该功能确保在处理大量数据时，能够及时保存进度，防止数据丢失，并且只保留一个备份文件和一个正在写入的文件。

## 主要特性

### 1. 实时保存机制
- **自动保存**: 每处理指定数量的文件后自动保存图谱
- **三元组日志**: 实时记录所有提取的三元组到日志文件
- **进度报告**: 自动生成处理进度报告
- **单一备份**: 只保留一个备份文件，自动覆盖旧备份

### 2. 文件管理
- **主图谱文件**: `knowledge_graph.gpickle` - 正在写入的主文件
- **备份文件**: `knowledge_graph_backup.gpickle` - 单一备份文件
- **三元组日志**: `triples_log.jsonl` - 按时间顺序记录所有三元组
- **进度报告**: `progress_report.json` - 当前处理状态和统计信息

### 3. 新增功能类 `RealTimeGraphManager`
继承自原有的 `GraphManager`，新增以下方法：
- `add_triples_with_logging()`: 添加三元组并记录日志
- `force_save()`: 强制保存所有数据
- `get_statistics()`: 获取详细统计信息
- `_auto_save()`: 自动保存机制
- `_create_single_backup()`: 创建单一备份文件

## 使用方法

### 1. 基本用法
```bash
# 运行爬虫并构建图谱（使用默认保存频率）
python main.py --run-scraper --build-graph

# 指定保存频率（每处理5个文件保存一次）
python main.py --build-graph --save-frequency 5

# 完整流程
python main.py --run-scraper --build-graph --generate-questions --save-frequency 3
```

### 2. 新增命令行参数
- `--save-frequency N`: 设置自动保存频率（默认每处理1个文件保存一次）

### 3. 实时监控
程序运行时会显示详细的保存状态：
```
✅ 实时保存图谱管理器初始化完成
   主图谱文件: knowledge_graph.gpickle
   备份文件: knowledge_graph_backup.gpickle
   三元组日志: triples_log.jsonl
   自动保存: 启用
   保存频率: 每处理1个文件保存一次

📝 已记录 5 个三元组到日志文件
💾 自动保存图谱 (已处理2个文件)...
🗑️  删除旧备份文件
📦 已创建备份: knowledge_graph_backup.gpickle
💾 图谱已保存到 knowledge_graph.gpickle
📊 已保存进度报告
✅ 自动保存完成
```

## 文件结构说明

### 1. 三元组日志格式 (`triples_log.jsonl`)
```json
{
  "timestamp": "2025-07-22T10:30:15.123456",
  "source_url": "http://example.com/page1",
  "triples_count": 3,
  "triples": [
    {
      "subject": "张三",
      "relation": "工作于", 
      "object": "ABC公司",
      "subject_type": "Person",
      "object_type": "Organization",
      "source_url": "http://example.com/page1"
    }
  ]
}
```

### 2. 进度报告格式 (`progress_report.json`)
```json
{
  "timestamp": "2025-07-22T10:30:15.123456",
  "processed_files": 10,
  "total_nodes": 250,
  "total_edges": 180,
  "graph_file": "/path/to/knowledge_graph.gpickle",
  "backup_file": "/path/to/knowledge_graph_backup.gpickle",
  "triples_log": "/path/to/triples_log.jsonl",
  "auto_save_enabled": true,
  "save_frequency": 1
}
```

## 演示脚本

运行演示脚本查看实时保存功能：
```bash
python demo_realtime_saving.py
```

演示脚本会：
1. 创建实时保存图谱管理器
2. 模拟处理多个文件的三元组
3. 展示自动保存触发
4. 显示最终统计信息
5. 检查生成的所有文件

## 优势

### 1. 数据安全
- **防止数据丢失**: 实时保存确保即使程序意外中断，已处理的数据也不会丢失
- **增量备份**: 每次保存前创建备份，确保数据安全
- **详细日志**: 完整记录所有三元组提取过程

### 2. 存储优化
- **单一备份**: 只保留一个备份文件，避免磁盘空间浪费
- **自动清理**: 自动覆盖旧备份，保持存储整洁
- **压缩存储**: 使用pickle格式高效存储图谱数据

### 3. 进度监控
- **实时反馈**: 详细的控制台输出显示保存状态
- **统计报告**: 自动生成处理进度和图谱统计信息
- **可恢复性**: 可从任意保存点继续处理

## 与原版本的兼容性

- **完全兼容**: 保持原有所有功能不变
- **向下兼容**: 原有的命令行参数和使用方法完全保持
- **可选功能**: 实时保存功能可通过参数控制启用/禁用
- **无侵入性**: 不影响其他模块的使用

## 注意事项

1. **磁盘空间**: 确保有足够的磁盘空间存储图谱文件和日志
2. **保存频率**: 根据数据量调整保存频率，频繁保存可能影响性能
3. **文件权限**: 确保程序对数据目录有读写权限
4. **中断恢复**: 程序中断后，可以从最后一次保存的图谱继续处理

## 文件清理建议

如需清理实时保存产生的文件：
```bash
# 清理日志文件
rm data/triples_log.jsonl

# 清理备份文件  
rm data/knowledge_graph_backup.gpickle

# 清理进度报告
rm data/progress_report.json
```
