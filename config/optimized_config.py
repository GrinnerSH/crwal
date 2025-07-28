"""
优化配置模块 - 实时保存和性能参数
"""

import os
from config.config import *

# 实时保存配置
ENABLE_REAL_TIME_SAVE = True
AUTO_SAVE_FREQUENCY = 1  # 每处理多少个URL自动保存图谱
TRIPLE_SAVE_FORMAT = "jsonl"  # 三元组保存格式: json 或 jsonl

# 备份配置
ENABLE_AUTO_BACKUP = True
BACKUP_FREQUENCY = 10  # 每处理多少个URL创建备份
MAX_BACKUP_COUNT = 5   # 最大备份文件数量

# 性能优化配置
BATCH_SIZE = 50        # 批处理大小
MEMORY_THRESHOLD = 0.8  # 内存使用阈值（触发强制保存）
PROGRESS_REPORT_FREQUENCY = 1  # 每处理多少个URL显示进度

# 文件路径配置
EXTRACTED_TRIPLES_DIR = os.path.join(os.path.dirname(GRAPH_STORE_PATH), "extracted_triples")
TRIPLES_LOG_PATH = os.path.join(os.path.dirname(GRAPH_STORE_PATH), "triples_log.jsonl")
BACKUP_DIR = os.path.join(os.path.dirname(GRAPH_STORE_PATH), "backups")
SESSION_LOGS_DIR = os.path.join(os.path.dirname(GRAPH_STORE_PATH), "session_logs")

# 会话管理配置
SESSION_TIMEOUT = 3600  # 会话超时时间（秒）
SESSION_AUTO_CLEANUP = True  # 自动清理过期会话

# 错误处理配置
MAX_RETRY_ATTEMPTS = 3  # 最大重试次数
RETRY_DELAY = 1.0      # 重试延迟（秒）
SKIP_ON_ERROR = True   # 遇到错误时是否跳过继续处理

# 日志配置
DETAILED_LOGGING = True  # 启用详细日志
LOG_TRIPLE_EXTRACTION = True  # 记录三元组提取过程
LOG_GRAPH_UPDATES = True     # 记录图谱更新过程

# 统计配置
ENABLE_STATISTICS = True     # 启用统计功能
STATISTICS_REPORT_FREQUENCY = 10  # 每处理多少个URL生成统计报告

# 资源监控配置
MONITOR_MEMORY_USAGE = True  # 监控内存使用
MONITOR_DISK_SPACE = True    # 监控磁盘空间
MIN_DISK_SPACE_GB = 1.0      # 最小磁盘空间要求（GB）

# 确保目录存在
def ensure_directories():
    """确保所有必要的目录存在"""
    directories = [
        EXTRACTED_TRIPLES_DIR,
        BACKUP_DIR,
        SESSION_LOGS_DIR,
        os.path.dirname(TRIPLES_LOG_PATH),
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

# 初始化时创建目录
ensure_directories()
