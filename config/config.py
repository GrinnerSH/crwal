# import os

# # --- LLM API 配置 ---
# # 请将您的API密钥存储在环境变量中，以保证安全
# LLM_API_KEY = os.getenv("API_KEY", "your-api-key-value")
# LLM_API_BASE_URL = "https://api.360.cn/v1/chat/completions"
# LLM_MODEL_NAME = "deepseek-chat-v3"  # 根据用户文档指定
# LLM_TEMPERATURE = 0.7
# LLM_MAX_TOKENS = 16384
# LLM_USE_STREAMING = False  # 是否使用流式调用

# # --- 爬虫模块配置 ---
# # 获取项目根目录的绝对路径
# import os
# _PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace('config', '')

# # 种子URL文件路径
# SEED_URLS_FILE = os.path.join(_PROJECT_ROOT, "data", "seed_urls.txt")
# # 原始HTML存储目录
# RAW_HTML_DIR = os.path.join(_PROJECT_ROOT, "data", "raw_html")
# # 爬取最大深度
# MAX_DEPTH = 2
# # 最大爬取页面数量限制
# MAX_PAGES_TO_CRAWL = 10000
# # 两次请求之间的随机延迟范围 (秒)
# CRAWL_DELAY_RANGE = (2, 5)
# # 链接文本过滤停用词列表
# FILTER_WORDS = [
#     '宏大叙事', '不确定性', '争议', '参考文献', '外部链接', 
#     '参见', '注释', '脚注', '分类', '隐私政策', '免责声明',
#     '版权', '联系我们', '帮助', '用户手册', '使用条款',
#     '存档', '讨论', '编辑', '历史', '上传文件', '特殊页面'
# ]
# # User-Agent池，用于轮换
# USER_AGENTS = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
# ]

# # --- 图谱模块配置 ---
# # 实体与关系类型定义 (源自用户文档)
# ENTITY_TYPES = [
#     "Person", "Organization", "Location", "Event", "Product", "Concept", 
#     "Date", "Number", "Chemical", "Award", "Publication", "Technology"
# ]
# RELATION_TYPES = [
#     # 创作/归属
#     "创作者", "导演", "作者", "创立者", "开发者",
#     # 个人/家庭
#     "配偶", "父亲", "兄弟姐妹", "结婚对象",
#     # 职业/教育
#     "毕业于", "就职于", "成员",
#     # 时序
#     "发生年份", "出生年份", "成立年份", "发布时间",
#     # 地理/空间
#     "位于", "出生地", "总部位于",
#     # 构成/属性
#     "具有属性", "基于", "化学分子式", "定义",
#     # 比较
#     "早于", "相同",
#     # 因果/影响
#     "导致", "受影响于", "影响", "贡献",
#     # 奖项相关
#     "获得奖项", "提名奖项", "颁发奖项",
#     # 演出相关
#     "主演", "参演", "制片人",
#     # 研究/学术相关
#     "研究领域", "研究目标", "研究方法", "研究内容", "研究历史",
#     # 发展/历史
#     "发展阶段", "成立时间",
#     # 技术/产品
#     "发布模型"
# ]
# # 实体对齐相似度阈值（提高阈值以减少错误合并）
# SIMILARITY_THRESHOLD = 0.90
# # NetworkX图谱持久化路径
# GRAPH_STORE_PATH = os.path.join(_PROJECT_ROOT, "data", "knowledge_graph.gpickle")

# # --- Neo4j 配置 (可选) ---
# NEO4J_URI = "bolt://localhost:7687"
# NEO4J_USER = "neo4j"
# NEO4J_PASSWORD = "password" # 请使用强密码

# # --- 问题生成模块配置 ---
# # 生成问题的数量
# NUM_QUESTIONS_TO_GENERATE = 5000
# # 最终数据集输出路径
# OUTPUT_DATASET_PATH = os.path.join(_PROJECT_ROOT, "data", "output", "generated_qa_dataset.jsonl")


import os

# --- LLM API 配置 ---
# 请将您的API密钥存储在环境变量中，以保证安全
LLM_API_KEY = os.getenv("API_KEY", "your-api-key-value")
LLM_API_BASE_URL = "https://api.360.cn/v1/chat/completions"
LLM_MODEL_NAME = "deepseek-chat-v3"  # 根据用户文档指定
LLM_TEMPERATURE = 0.7
LLM_MAX_TOKENS = 16384
LLM_USE_STREAMING = False  # 是否使用流式调用

# --- 爬虫模块配置 ---
# 获取项目根目录的绝对路径
import os
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)).replace('config', '')

# 种子URL文件路径
SEED_URLS_FILE = os.path.join(_PROJECT_ROOT, "data", "seed_urls.txt")
# 原始HTML存储目录
RAW_HTML_DIR = os.path.join(_PROJECT_ROOT, "data", "raw_html")
# 爬取最大深度
MAX_DEPTH = 5
# 最大爬取页面数量限制
MAX_PAGES_TO_CRAWL = 50000
# 两次请求之间的随机延迟范围 (秒)
CRAWL_DELAY_RANGE = (2, 5)
# 链接文本过滤停用词列表
FILTER_WORDS = [
    '宏大叙事', '不确定性', '争议', '参考文献', '外部链接', 
    '参见', '注释', '脚注', '分类', '隐私政策', '免责声明',
    '版权', '联系我们', '帮助', '用户手册', '使用条款',
    '存档', '讨论', '编辑', '历史', '上传文件', '特殊页面'
]
# User-Agent池，用于轮换
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
]

# --- 图谱模块配置 ---
# 实体与关系类型定义 (源自用户文档)
ENTITY_TYPES = [
    "Person", "Organization", "Location", "Event", "Product", "Concept", 
    "Date", "Number", "Chemical", "Award", "Publication", "Technology"
]
RELATION_TYPES = [
    # 创作/归属
    "创作者", "导演", "作者", "创立者", "开发者",
    # 个人/家庭
    "配偶", "父亲", "兄弟姐妹", "结婚对象",
    # 职业/教育
    "毕业于", "就职于", "成员",
    # 时序
    "发生年份", "出生年份", "成立年份", "发布时间",
    # 地理/空间
    "位于", "出生地", "总部位于",
    # 构成/属性
    "具有属性", "基于", "化学分子式", "定义",
    # 比较
    "早于", "相同",
    # 因果/影响
    "导致", "受影响于", "影响", "贡献",
    # 奖项相关
    "获得奖项", "提名奖项", "颁发奖项",
    # 演出相关
    "主演", "参演", "制片人",
    # 研究/学术相关
    "研究领域", "研究目标", "研究方法", "研究内容", "研究历史",
    # 发展/历史
    "发展阶段", "成立时间",
    # 技术/产品
    "发布模型"
]
# 实体对齐相似度阈值（提高阈值以减少错误合并）
SIMILARITY_THRESHOLD = 0.90
# NetworkX图谱持久化路径
GRAPH_STORE_PATH = os.path.join(_PROJECT_ROOT, "data", "knowledge_graph.gpickle")

# --- Neo4j 配置 (可选) ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password" # 请使用强密码

# --- 问题生成模块配置 ---
# 生成问题的数量
NUM_QUESTIONS_TO_GENERATE = 5000
# 最终数据集输出路径
OUTPUT_DATASET_PATH = os.path.join(_PROJECT_ROOT, "data", "output", "generated_qa_dataset.jsonl")