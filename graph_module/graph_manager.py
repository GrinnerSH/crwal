# import os
# import pandas as pd
# import networkx as nx
# import recordlinkage
# from jarowinkler import jarowinkler_similarity
# from py2neo import Graph as Py2neoGraph, Node, Relationship

# from config import config

# class GraphManager:
#     def __init__(self, graph_path=None):
#         if graph_path and os.path.exists(graph_path):
#             self.load_graph(graph_path)
#         else:
#             self.G = nx.MultiDiGraph() # 支持平行边和有向边

#     def add_triples(self, triples):
#         """添加三元组列表到图中，并返回新增的实体节点"""
#         new_nodes = set()
#         for triple in triples:
#             subj, rel, obj = triple['subject'], triple['relation'], triple['object']
            
#             # 为节点添加类型属性，如果LLM能提供的话
#             subj_type = triple.get('subject_type', 'Unknown')
#             obj_type = triple.get('object_type', 'Unknown')

#             if subj not in self.G:
#                 self.G.add_node(subj, type=subj_type, names={subj})
#                 new_nodes.add(subj)
#             if obj not in self.G:
#                 self.G.add_node(obj, type=obj_type, names={obj})
#                 new_nodes.add(obj)
            
#             # 边上存储关系类型和源URL
#             self.G.add_edge(subj, obj, key=rel, relation=rel, source_url=triple.get('source_url'))
#         return list(new_nodes)

#     def resolve_entities_incremental(self, new_nodes):
#         """对新增节点进行增量式实体对齐"""
#         if not new_nodes:
#             return

#         existing_nodes = list(self.G.nodes)
#         all_nodes = list(set(existing_nodes + new_nodes))
        
#         # 创建一个Pandas DataFrame用于recordlinkage
#         import pandas as pd
#         df = pd.DataFrame({'name': all_nodes, 'type': [self.G.nodes[n].get('type', 'Unknown') for n in all_nodes]})
#         df.index.name = 'id'

#         # 1. 阻塞：只比较同类型的实体
#         indexer = recordlinkage.Index()
#         indexer.block('type')
#         candidate_links = indexer.index(df)

#         print(f"Generated {len(candidate_links)} candidate pairs for entity resolution.")

#         # 2. 比较：使用Jaro-Winkler相似度
#         compare = recordlinkage.Compare()
#         compare.string('name', 'name', method='jarowinkler', threshold=config.SIMILARITY_THRESHOLD, label='name_sim')
#         features = compare.compute(candidate_links, df)
        
#         matches = features[features.sum(axis=1) > 0].index
#         print(f"Found {len(matches)} matching pairs above threshold.")

#         # 3. 聚类与合并 - 增加额外的过滤逻辑
#         if not matches.empty:
#             match_graph = nx.Graph(list(matches))
#             for component in nx.connected_components(match_graph):
#                 nodes_to_merge_indices = list(component)
#                 nodes_to_merge = [df.iloc[i]['name'] for i in nodes_to_merge_indices]
                
#                 # 增加严格的过滤条件
#                 if self._should_merge_entities(nodes_to_merge):
#                     # 选择最长的名字作为规范名
#                     canonical_name = max(nodes_to_merge, key=len)
                    
#                     print(f"Merging {nodes_to_merge} into {canonical_name}")
                    
#                     # 在主图中执行合并
#                     # nx.contracted_nodes不直接支持MultiDiGraph，需要手动合并
#                     self._merge_nodes(nodes_to_merge, canonical_name)
#                 else:
#                     print(f"Skipping merge for {nodes_to_merge} - failed validation checks")

#     def _should_merge_entities(self, entities):
#         """判断实体是否应该被合并的严格检查"""
#         if len(entities) < 2:
#             return False
        
#         # 检查关键词冲突 - 防止性别、类型等关键差异被忽略
#         conflict_keywords = [
#             ('男', '女'), ('男性', '女性'), ('男主角', '女主角'),
#             ('最佳', '最差'), ('获奖', '提名'), 
#             ('电影', '电视剧'), ('导演', '演员'), ('制片人', '编剧')
#         ]
        
#         # 语义冲突检测 - 防止概念与具体实例/人物的错误合并
#         semantic_conflicts = [
#             # 技术概念 vs 人物称谓
#             ('深度学习', '深度学习之父'),
#             ('人工智能', '人工智能之父'),
#             ('机器学习', '机器学习专家'),
#             # 概念 vs 发展/历史
#             ('深度学习', '深度学习发展'),
#             ('人工智能', '人工智能发展'),
#             ('人工智能', '人工智能概念'),  # 添加这个
#             ('机器学习', '机器学习历史'),
#             ('神经网络', '神经网络历史'),  # 添加这个
#             # 技术 vs 概念
#             ('技术', '概念'),
#             ('理论', '实践'),
#             ('方法', '结果'),
#             # 机构 vs 人物
#             ('大学', '教授'),
#             ('公司', '博士'),
#         ]
        
#         # 检查是否是不同类型的实体（概念/人物/事件等）
#         entity_type_indicators = {
#             'person': ['之父', '之母', '专家', '学者', '教授', '博士', '先生', '女士', '院士'],
#             'concept': ['概念', '理论', '思想', '原理', '定义'],
#             'process': ['发展', '历史', '演进', '过程', '阶段', '趋势'],
#             'technology': ['技术', '方法', '算法', '模型', '系统'],
#             'organization': ['公司', '企业', '机构', '组织', '团队', '实验室'],
#             'event': ['会议', '大会', '论坛', '比赛', '竞赛', '活动']
#         }
        
#         # 检查日期精度冲突 - 防止不同精度的日期被合并
#         date_patterns = ['年', '月', '日', '号']
        
#         for entity1 in entities:
#             for entity2 in entities:
#                 if entity1 != entity2:
#                     # 检查是否包含冲突关键词
#                     for kw1, kw2 in conflict_keywords:
#                         if (kw1 in entity1 and kw2 in entity2) or (kw2 in entity1 and kw1 in entity2):
#                             print(f"Conflict detected between '{entity1}' and '{entity2}': {kw1}/{kw2}")
#                             return False
                    
#                     # 检查语义冲突
#                     for concept, related in semantic_conflicts:
#                         if (concept in entity1 and related in entity2) or (related in entity1 and concept in entity2):
#                             print(f"Semantic conflict detected between '{entity1}' and '{entity2}': concept/related entity")
#                             return False
                    
#                     # 检查实体类型冲突
#                     entity1_types = set()
#                     entity2_types = set()
                    
#                     for entity_type, indicators in entity_type_indicators.items():
#                         for indicator in indicators:
#                             if indicator in entity1:
#                                 entity1_types.add(entity_type)
#                             if indicator in entity2:
#                                 entity2_types.add(entity_type)
                    
#                     # 如果两个实体被分类为不同类型，则不合并
#                     if entity1_types and entity2_types and entity1_types.isdisjoint(entity2_types):
#                         print(f"Entity type conflict between '{entity1}' ({entity1_types}) and '{entity2}' ({entity2_types})")
#                         return False
                    
#                     # 检查日期精度冲突（如"1929年" vs "1929年5月16日"）
#                     entity1_has_date = any(dp in entity1 for dp in date_patterns)
#                     entity2_has_date = any(dp in entity2 for dp in date_patterns)
                    
#                     if entity1_has_date and entity2_has_date:
#                         # 都包含日期元素，检查精度是否不同
#                         entity1_precision = 0
#                         entity2_precision = 0
                        
#                         if '年' in entity1: entity1_precision += 1
#                         if '月' in entity1: entity1_precision += 1
#                         if '日' in entity1 or '号' in entity1: entity1_precision += 1
                        
#                         if '年' in entity2: entity2_precision += 1
#                         if '月' in entity2: entity2_precision += 1
#                         if '日' in entity2 or '号' in entity2: entity2_precision += 1
                        
#                         if entity1_precision != entity2_precision:
#                             print(f"Date precision conflict between '{entity1}' and '{entity2}': {entity1_precision} vs {entity2_precision}")
#                             return False
                    
#                     # 长度差异过大的不合并（可能是缩写vs全称之外的情况）
#                     len_ratio = min(len(entity1), len(entity2)) / max(len(entity1), len(entity2))
#                     if len_ratio < 0.5:  # 长度比例小于0.5
#                         print(f"Length ratio too small between '{entity1}' and '{entity2}': {len_ratio}")
#                         return False
        
#         return True

#     def _merge_nodes(self, nodes_to_merge, canonical_name):
#         """手动合并MultiDiGraph中的节点"""
#         if canonical_name not in nodes_to_merge:
#             # 如果规范名本身不在待合并列表中，先添加它的属性
#             self.G.add_node(canonical_name, **self.G.nodes[nodes_to_merge[0]])
        
#         all_names = set()
#         for node_name in nodes_to_merge:
#             if node_name in self.G:
#                 all_names.update(self.G.nodes[node_name].get('names', {node_name}))

#         self.G.nodes[canonical_name]['names'] = all_names

#         for node_name in nodes_to_merge:
#             if node_name == canonical_name or node_name not in self.G:
#                 continue

#             # 重定向入边
#             for u, _, key, data in self.G.in_edges(node_name, keys=True, data=True):
#                 self.G.add_edge(u, canonical_name, key=key, **data)
            
#             # 重定向出边
#             for _, v, key, data in self.G.out_edges(node_name, keys=True, data=True):
#                 self.G.add_edge(canonical_name, v, key=key, **data)
            
#             self.G.remove_node(node_name)


#     def save_graph(self, filepath=config.GRAPH_STORE_PATH):
#         """将NetworkX图保存到文件"""
#         import pickle
#         try:
#             # 使用更新的方法保存图
#             with open(filepath, 'wb') as f:
#                 pickle.dump(self.G, f)
#             print(f"Graph saved to {filepath}")
#         except Exception as e:
#             print(f"Error saving graph: {e}")

#     def load_graph(self, filepath=config.GRAPH_STORE_PATH):
#         """从文件加载NetworkX图"""
#         import pickle
#         try:
#             # 使用更新的方法加载图
#             with open(filepath, 'rb') as f:
#                 self.G = pickle.load(f)
#             print(f"Graph loaded from {filepath}")
#         except FileNotFoundError:
#             print(f"Graph file {filepath} not found, starting with empty graph")
#             self.G = nx.MultiDiGraph()
#         except Exception as e:
#             print(f"Error loading graph: {e}, starting with empty graph")
#             self.G = nx.MultiDiGraph()

#     def save_to_neo4j(self):
#         """将图谱数据持久化到Neo4j数据库"""
#         try:
#             db = Py2neoGraph(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD))
#             # 清空数据库以进行全新导入
#             db.delete_all()
            
#             tx = db.begin()
#             # 创建节点
#             for node, data in self.G.nodes(data=True):
#                 node_type = data.get('type', 'Entity')
#                 properties = {'name': node, 'names': list(data.get('names', {node}))}
#                 n = Node(node_type, **properties)
#                 tx.create(n)
#             tx.commit()

#             # 创建关系
#             tx = db.begin()
#             for u, v, data in self.G.edges(data=True):
#                 relation_type = data['relation'].upper()
#                 start_node = db.nodes.match(name=u).first()
#                 end_node = db.nodes.match(name=v).first()
#                 if start_node and end_node:
#                     rel = Relationship(start_node, relation_type, end_node, source_url=data.get('source_url'))
#                     tx.create(rel)
#             tx.commit()
#             print("Graph successfully saved to Neo4j.")
#         except Exception as e:
#             print(f"Failed to save to Neo4j: {e}")
#!/usr/bin/env python3
"""
增强的知识图谱管理器 - 支持实时保存
"""

import os
import json
import pickle
import pandas as pd
import networkx as nx
import recordlinkage
from datetime import datetime
from jarowinkler import jarowinkler_similarity
from py2neo import Graph as Py2neoGraph, Node, Relationship

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

class GraphManager:
    """增强的知识图谱管理器，支持实时保存和增量更新"""
    
    def __init__(self, graph_path=None, auto_save=True, save_frequency=1):
        """
        初始化图谱管理器
        
        Args:
            graph_path: 图谱文件路径
            auto_save: 是否启用自动保存
            save_frequency: 保存频率（处理多少个URL后保存一次）
        """
        self.auto_save = auto_save
        self.save_frequency = save_frequency
        self.processed_urls = 0
        
        # 设置文件路径
        self.graph_path = graph_path or config.GRAPH_STORE_PATH
        self.triples_log_path = os.path.join(
            os.path.dirname(self.graph_path), 
            "triples_log.jsonl"
        )
        self.backup_dir = os.path.join(
            os.path.dirname(self.graph_path),
            "backups"
        )
        
        # 创建备份目录
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # 加载或创建图谱
        if graph_path and os.path.exists(graph_path):
            self.load_graph(graph_path)
        else:
            self.G = nx.MultiDiGraph()
        
        print(f"✅ 图谱管理器初始化完成")
        print(f"   图谱文件: {self.graph_path}")
        print(f"   三元组日志: {self.triples_log_path}")
        print(f"   自动保存: {'启用' if auto_save else '禁用'}")
        if auto_save:
            print(f"   保存频率: 每处理{save_frequency}个URL保存一次")

    def add_triples_with_logging(self, triples, source_url):
        """
        添加三元组并记录日志
        
        Args:
            triples: 三元组列表
            source_url: 源URL
        """
        if not triples:
            return []
        
        # 记录到日志文件
        self._log_triples(triples, source_url)
        
        # 添加到图谱
        new_nodes = self.add_triples(triples)
        
        # 执行实体对齐
        if new_nodes:
            self.resolve_entities_incremental(new_nodes)
        
        # 更新处理计数
        self.processed_urls += 1
        
        # 检查是否需要自动保存
        if self.auto_save and self.processed_urls % self.save_frequency == 0:
            self._auto_save()
        
        return new_nodes
    
    def _log_triples(self, triples, source_url):
        """将三元组记录到日志文件"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "source_url": source_url,
            "triples_count": len(triples),
            "triples": triples
        }
        
        try:
            with open(self.triples_log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
            print(f"📝 已记录 {len(triples)} 个三元组到日志文件")
            
        except Exception as e:
            print(f"⚠️  记录三元组日志失败: {e}")
    
    def _auto_save(self):
        """自动保存图谱"""
        print(f"💾 自动保存图谱 (已处理{self.processed_urls}个URL)...")
        
        # 创建备份
        self._create_backup()
        
        # 保存主图谱
        self.save_graph()
        
        print(f"✅ 自动保存完成")
    
    def _create_backup(self):
        """创建图谱备份"""
        if not os.path.exists(self.graph_path):
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"graph_backup_{timestamp}_{self.processed_urls}urls.gpickle"
        backup_path = os.path.join(self.backup_dir, backup_filename)
        
        try:
            # 复制当前图谱文件
            import shutil
            shutil.copy2(self.graph_path, backup_path)
            print(f"📦 已创建备份: {backup_filename}")
            
            # 清理旧备份（保留最近10个）
            self._cleanup_old_backups()
            
        except Exception as e:
            print(f"⚠️  创建备份失败: {e}")
    
    def _cleanup_old_backups(self, keep_count=10):
        """清理旧的备份文件"""
        try:
            import glob
            backup_files = glob.glob(os.path.join(self.backup_dir, "graph_backup_*.gpickle"))
            backup_files.sort(key=os.path.getmtime, reverse=True)
            
            # 删除多余的备份
            for old_backup in backup_files[keep_count:]:
                os.remove(old_backup)
                print(f"🗑️  删除旧备份: {os.path.basename(old_backup)}")
                
        except Exception as e:
            print(f"⚠️  清理备份失败: {e}")
    
    def force_save(self):
        """强制保存图谱和日志"""
        print(f"💾 强制保存图谱...")
        self._create_backup()
        self.save_graph()
        self._save_progress_report()
        print(f"✅ 强制保存完成")
    
    def _save_progress_report(self):
        """保存处理进度报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "processed_urls": self.processed_urls,
            "total_nodes": len(self.G.nodes),
            "total_edges": len(self.G.edges),
            "graph_file": self.graph_path,
            "triples_log": self.triples_log_path
        }
        
        report_path = os.path.join(
            os.path.dirname(self.graph_path),
            "progress_report.json"
        )
        
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"📊 已保存进度报告: {os.path.basename(report_path)}")
        except Exception as e:
            print(f"⚠️  保存进度报告失败: {e}")
    
    def get_statistics(self):
        """获取图谱统计信息"""
        backup_count = 0
        if os.path.exists(self.backup_dir):
            backup_files = [f for f in os.listdir(self.backup_dir) if f.startswith("graph_backup_")]
            backup_count = len(backup_files)
        
        last_save_time = "未保存"
        if os.path.exists(self.graph_path):
            mod_time = datetime.fromtimestamp(os.path.getmtime(self.graph_path))
            last_save_time = mod_time.strftime('%Y-%m-%d %H:%M:%S')
        
        stats = {
            "urls_processed": self.processed_urls,
            "total_nodes": len(self.G.nodes),
            "total_edges": len(self.G.edges),
            "graph_file_exists": os.path.exists(self.graph_path),
            "triples_log_exists": os.path.exists(self.triples_log_path),
            "backup_count": backup_count,
            "last_save_time": last_save_time,
            "auto_save_enabled": self.auto_save,
            "save_frequency": self.save_frequency
        }
        return stats
    
    def recover_from_log(self, start_timestamp=None):
        """从日志文件恢复三元组"""
        if not os.path.exists(self.triples_log_path):
            print(f"❌ 日志文件不存在: {self.triples_log_path}")
            return
        
        print(f"🔄 从日志文件恢复三元组...")
        recovered_count = 0
        
        try:
            with open(self.triples_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    entry = json.loads(line.strip())
                    
                    # 如果指定了开始时间，跳过之前的记录
                    if start_timestamp and entry['timestamp'] < start_timestamp:
                        continue
                    
                    # 添加三元组到图谱
                    triples = entry['triples']
                    for triple in triples:
                        triple['source_url'] = entry['source_url']
                    
                    new_nodes = self.add_triples(triples)
                    if new_nodes:
                        self.resolve_entities_incremental(new_nodes)
                    
                    recovered_count += len(triples)
            
            print(f"✅ 已从日志恢复 {recovered_count} 个三元组")
            
        except Exception as e:
            print(f"❌ 从日志恢复失败: {e}")

    # 以下方法继承自原始GraphManager
    def add_triples(self, triples):
        """添加三元组列表到图中，并返回新增的实体节点"""
        new_nodes = set()
        for triple in triples:
            subj, rel, obj = triple['subject'], triple['relation'], triple['object']
            
            # 为节点添加类型属性，如果LLM能提供的话
            subj_type = triple.get('subject_type', 'Unknown')
            obj_type = triple.get('object_type', 'Unknown')

            if subj not in self.G:
                self.G.add_node(subj, type=subj_type, names={subj})
                new_nodes.add(subj)
            if obj not in self.G:
                self.G.add_node(obj, type=obj_type, names={obj})
                new_nodes.add(obj)
            
            # 边上存储关系类型和源URL
            self.G.add_edge(subj, obj, key=rel, relation=rel, source_url=triple.get('source_url'))
        return list(new_nodes)

    def resolve_entities_incremental(self, new_nodes):
        """对新增节点进行增量式实体对齐"""
        if not new_nodes:
            return

        existing_nodes = list(self.G.nodes)
        all_nodes = list(set(existing_nodes + new_nodes))
        
        # 创建一个Pandas DataFrame用于recordlinkage
        df = pd.DataFrame({'name': all_nodes, 'type': [self.G.nodes[n].get('type', 'Unknown') for n in all_nodes]})
        df.index.name = 'id'

        # 1. 阻塞：只比较同类型的实体
        indexer = recordlinkage.Index()
        indexer.block('type')
        candidate_links = indexer.index(df)

        print(f"Generated {len(candidate_links)} candidate pairs for entity resolution.")

        # 2. 比较：使用Jaro-Winkler相似度
        compare = recordlinkage.Compare()
        compare.string('name', 'name', method='jarowinkler', threshold=config.SIMILARITY_THRESHOLD, label='name_sim')
        features = compare.compute(candidate_links, df)
        
        matches = features[features.sum(axis=1) > 0].index
        print(f"Found {len(matches)} matching pairs above threshold.")

        # 3. 聚类与合并 - 增加额外的过滤逻辑
        if not matches.empty:
            match_graph = nx.Graph(list(matches))
            for component in nx.connected_components(match_graph):
                nodes_to_merge_indices = list(component)
                nodes_to_merge = [df.iloc[i]['name'] for i in nodes_to_merge_indices]
                
                # 增加严格的过滤条件
                if self._should_merge_entities(nodes_to_merge):
                    # 选择最长的名字作为规范名
                    canonical_name = max(nodes_to_merge, key=len)
                    
                    print(f"Merging {nodes_to_merge} into {canonical_name}")
                    
                    # 在主图中执行合并
                    self._merge_nodes(nodes_to_merge, canonical_name)
                else:
                    print(f"Skipping merge for {nodes_to_merge} - failed validation checks")

    def _should_merge_entities(self, entities):
        """判断实体是否应该被合并的严格检查"""
        if len(entities) < 2:
            return False
        
        # ===== 新增检查1: 数字/年份精确匹配检查 =====
        import re
        # 识别数字或年份的正则表达式
        number_pattern = re.compile(r'^[零一二三四五六七八九十百千万\d]+(年)?$')
        chinese_year_pattern = re.compile(r'^[零一二三四五六七八九十百千万]+年?$')
        
        # 检查是否所有实体都是数字/年份格式
        all_are_numbers = all(number_pattern.match(entity) or chinese_year_pattern.match(entity) for entity in entities)
        
        if all_are_numbers:
            # 如果都是数字/年份，要求完全相等才能合并
            first_entity = entities[0]
            if not all(entity == first_entity for entity in entities):
                print(f"Number/Year mismatch: {entities} - different numbers cannot be merged")
                return False
        
        # ===== 新增检查2: 名称包含关系严格检查 =====
        for i, entity1 in enumerate(entities):
            for j, entity2 in enumerate(entities):
                if i >= j:  # 避免重复检查
                    continue
                
                # 检查是否存在包含关系
                if entity1 in entity2 or entity2 in entity1:
                    shorter = entity1 if len(entity1) < len(entity2) else entity2
                    longer = entity2 if len(entity1) < len(entity2) else entity1
                    
                    # 计算长度差异
                    length_diff = len(longer) - len(shorter)
                    
                    # 如果长度差异较小（1-3个字符），需要检查额外部分
                    if 1 <= length_diff <= 3:
                        extra_part = longer.replace(shorter, '', 1)  # 获取额外部分
                        
                        # 定义可能表示不同实体的后缀/修饰词
                        distinct_suffixes = ['星', '华', '明', '强', '伟', '峰', '霞', '娟', '丽', '红', 
                                           '院', '所', '会', '司', '厂', '店', '馆', '楼', '室', '部', '科', '组', '队']
                        
                        # 如果额外部分是明显的区分词，则不应合并
                        if any(suffix in extra_part for suffix in distinct_suffixes):
                            print(f"Name containment with distinct suffix: '{shorter}' vs '{longer}' - extra part '{extra_part}' indicates different entities")
                            return False
                        
                        # 特别检查：如果额外部分是常见的人名字符，也不应合并
                        common_name_chars = set('华明强伟峰霞娟丽红梅兰芳雯雅婷洁静敏颖慧佳美君雄飞翔宇辉鹏涛磊刚勇军波涛')
                        if any(char in common_name_chars for char in extra_part):
                            print(f"Name containment with name character: '{shorter}' vs '{longer}' - extra part '{extra_part}' likely indicates different person")
                            return False
        
        # ===== 新增检查3: 强化实体类型冲突检测 =====
        # 检查关键词冲突 - 防止性别、类型等关键差异被忽略
        conflict_keywords = [
            ('男', '女'), ('男性', '女性'), ('男主角', '女主角'),
            ('最佳', '最差'), ('获奖', '提名'), 
            ('电影', '电视剧'), ('导演', '演员'), ('制片人', '编剧')
        ]
        
        # 语义冲突检测 - 防止概念与具体实例/人物的错误合并
        semantic_conflicts = [
            # 技术概念 vs 人物称谓
            ('深度学习', '深度学习之父'),
            ('人工智能', '人工智能之父'),
            ('机器学习', '机器学习专家'),
            # 概念 vs 发展/历史
            ('深度学习', '深度学习发展'),
            ('人工智能', '人工智能发展'),
            ('人工智能', '人工智能概念'),
            ('机器学习', '机器学习历史'),
            ('神经网络', '神经网络历史'),
            # 技术 vs 概念
            ('技术', '概念'),
            ('理论', '实践'),
            ('方法', '结果'),
            # 机构 vs 人物
            ('大学', '教授'),
            ('公司', '博士'),
        ]
        
        # 扩展的实体类型指示器 - 增加更多人物和机构的识别词
        entity_type_indicators = {
            'person': ['之父', '之母', '专家', '学者', '教授', '博士', '先生', '女士', '院士', 
                      '员', '家', '师', '长', '主任', '经理', '总裁', '董事', '主席'],  # 新增: 员、家、师等
            'concept': ['概念', '理论', '思想', '原理', '定义'],
            'process': ['发展', '历史', '演进', '过程', '阶段', '趋势'],
            'technology': ['技术', '方法', '算法', '模型', '系统'],
            'organization': ['公司', '企业', '机构', '组织', '团队', '实验室', 
                           '院', '所', '会', '司', '厂', '店', '馆', '部', '科', '组', '队'],  # 新增: 院、所、会、司等
            'event': ['会议', '大会', '论坛', '比赛', '竞赛', '活动']
        }
        
        # 检查日期精度冲突 - 防止不同精度的日期被合并
        date_patterns = ['年', '月', '日', '号']
        
        for entity1 in entities:
            for entity2 in entities:
                if entity1 != entity2:
                    # 检查是否包含冲突关键词
                    for kw1, kw2 in conflict_keywords:
                        if (kw1 in entity1 and kw2 in entity2) or (kw2 in entity1 and kw1 in entity2):
                            print(f"Conflict detected between '{entity1}' and '{entity2}': {kw1}/{kw2}")
                            return False
                    
                    # 检查语义冲突
                    for concept, related in semantic_conflicts:
                        if (concept in entity1 and related in entity2) or (related in entity1 and concept in entity2):
                            print(f"Semantic conflict detected between '{entity1}' and '{entity2}': concept/related entity")
                            return False
                    
                    # 强化的实体类型冲突检查
                    entity1_types = set()
                    entity2_types = set()
                    
                    for entity_type, indicators in entity_type_indicators.items():
                        for indicator in indicators:
                            if indicator in entity1:
                                entity1_types.add(entity_type)
                            if indicator in entity2:
                                entity2_types.add(entity_type)
                    
                    # 如果两个实体被分类为不同类型，则不合并
                    if entity1_types and entity2_types and entity1_types.isdisjoint(entity2_types):
                        print(f"Entity type conflict between '{entity1}' ({entity1_types}) and '{entity2}' ({entity2_types})")
                        return False
                    
                    # 特别针对人物vs机构的检查 - 解决"研究员"vs"研究院"问题
                    person_suffixes = ['员', '家', '师', '长', '主任', '经理', '总裁', '董事', '主席', '人']
                    org_suffixes = ['院', '所', '会', '司', '厂', '店', '馆', '部', '科', '组', '队']
                    
                    entity1_is_person = any(entity1.endswith(suffix) for suffix in person_suffixes)
                    entity1_is_org = any(entity1.endswith(suffix) for suffix in org_suffixes)
                    entity2_is_person = any(entity2.endswith(suffix) for suffix in person_suffixes)
                    entity2_is_org = any(entity2.endswith(suffix) for suffix in org_suffixes)
                    
                    # 如果一个明显是人物，另一个明显是机构，则禁止合并
                    if (entity1_is_person and entity2_is_org) or (entity1_is_org and entity2_is_person):
                        print(f"Person/Organization conflict between '{entity1}' and '{entity2}'")
                        return False
                    
                    # 检查日期精度冲突（如"1929年" vs "1929年5月16日"）
                    entity1_has_date = any(dp in entity1 for dp in date_patterns)
                    entity2_has_date = any(dp in entity2 for dp in date_patterns)
                    
                    if entity1_has_date and entity2_has_date:
                        # 都包含日期元素，检查精度是否不同
                        entity1_precision = 0
                        entity2_precision = 0
                        
                        if '年' in entity1: entity1_precision += 1
                        if '月' in entity1: entity1_precision += 1
                        if '日' in entity1 or '号' in entity1: entity1_precision += 1
                        
                        if '年' in entity2: entity2_precision += 1
                        if '月' in entity2: entity2_precision += 1
                        if '日' in entity2 or '号' in entity2: entity2_precision += 1
                        
                        if entity1_precision != entity2_precision:
                            print(f"Date precision conflict between '{entity1}' and '{entity2}': {entity1_precision} vs {entity2_precision}")
                            return False
                    
                    # 长度差异过大的不合并（可能是缩写vs全称之外的情况）
                    len_ratio = min(len(entity1), len(entity2)) / max(len(entity1), len(entity2))
                    if len_ratio < 0.5:  # 长度比例小于0.5
                        print(f"Length ratio too small between '{entity1}' and '{entity2}': {len_ratio}")
                        return False
        
        return True

    def _merge_nodes(self, nodes_to_merge, canonical_name):
        """手动合并MultiDiGraph中的节点"""
        if canonical_name not in nodes_to_merge:
            # 如果规范名本身不在待合并列表中，先添加它的属性
            self.G.add_node(canonical_name, **self.G.nodes[nodes_to_merge[0]])
        
        all_names = set()
        for node_name in nodes_to_merge:
            if node_name in self.G:
                all_names.update(self.G.nodes[node_name].get('names', {node_name}))

        self.G.nodes[canonical_name]['names'] = all_names

        for node_name in nodes_to_merge:
            if node_name == canonical_name or node_name not in self.G:
                continue

            # 重定向入边
            for u, _, key, data in self.G.in_edges(node_name, keys=True, data=True):
                self.G.add_edge(u, canonical_name, key=key, **data)
            
            # 重定向出边
            for _, v, key, data in self.G.out_edges(node_name, keys=True, data=True):
                self.G.add_edge(canonical_name, v, key=key, **data)
            
            self.G.remove_node(node_name)

    def save_graph(self, filepath=None):
        """将NetworkX图保存到文件"""
        filepath = filepath or self.graph_path
        try:
            # 使用更新的方法保存图
            with open(filepath, 'wb') as f:
                pickle.dump(self.G, f)
            print(f"Graph saved to {filepath}")
        except Exception as e:
            print(f"Error saving graph: {e}")

    def load_graph(self, filepath=None):
        """从文件加载NetworkX图"""
        filepath = filepath or self.graph_path
        try:
            with open(filepath, 'rb') as f:
                self.G = pickle.load(f)
            print(f"Graph loaded from {filepath}")
            return self.G  # 返回加载的图谱
        except Exception as e:
            print(f"Error loading graph: {e}")
            self.G = nx.MultiDiGraph()
            return self.G  # 返回空图谱
