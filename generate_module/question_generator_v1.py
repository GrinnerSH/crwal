import json
import requests
import random
import os
from datetime import datetime
from config import config
from graph_module.fact_extractor import LLMExtractor

class QuestionGenerator:
    def __init__(self):
        self.llm_extractor = LLMExtractor()

    def _batch_llm_call(self, prompt, max_retries=2):
        """
        批量调用LLM，支持不同的API响应格式，带重试机制
        """
        for retry in range(max_retries + 1):
            try:
                payload = {
                    "model": config.LLM_MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.8,
                    "max_tokens": min(config.LLM_MAX_TOKENS, 4000)
                }
                headers = {'Authorization': f'Bearer {config.LLM_API_KEY}', 'Content-Type': 'application/json'}
                
                # 根据prompt长度调整超时时间
                prompt_length = len(prompt)
                if prompt_length > 10000:
                    timeout = 60
                elif prompt_length > 5000:
                    timeout = 45
                else:
                    timeout = 30
                
                response = requests.post(config.LLM_API_BASE_URL, headers=headers, data=json.dumps(payload), timeout=timeout)
                
                if response.status_code != 200:
                    error_msg = f"API调用失败，状态码: {response.status_code}"
                    print(f"  {error_msg}")
                    if retry < max_retries:
                        print(f"  重试 {retry + 1}/{max_retries}...")
                        continue
                    return f"API调用失败：HTTP {response.status_code}"
                
                response_data = response.json()
                
                # 尝试不同的响应格式
                if 'choices' in response_data and len(response_data['choices']) > 0:
                    if 'message' in response_data['choices'][0]:
                        content = response_data['choices'][0]['message']['content']
                        if content:
                            return content.strip()
                    elif 'text' in response_data['choices'][0]:
                        content = response_data['choices'][0]['text']
                        if content:
                            return content.strip()
                elif 'data' in response_data:
                    return str(response_data['data']).strip()
                elif 'content' in response_data:
                    return str(response_data['content']).strip()
                
                error_msg = f"未知的API响应格式: {response_data}"
                print(f"  {error_msg}")
                if retry < max_retries:
                    print(f"  重试 {retry + 1}/{max_retries}...")
                    continue
                return "API调用失败：未知响应格式"
                    
            except requests.exceptions.Timeout:
                error_msg = "API调用超时"
                print(f"  {error_msg}")
                if retry < max_retries:
                    print(f"  重试 {retry + 1}/{max_retries}...")
                    continue
                return "API调用失败：请求超时"
            except requests.exceptions.RequestException as e:
                error_msg = f"网络请求异常: {e}"
                print(f"  {error_msg}")
                if retry < max_retries:
                    print(f"  重试 {retry + 1}/{max_retries}...")
                    continue
                return f"API调用失败：网络错误 - {str(e)}"
            except json.JSONDecodeError as e:
                error_msg = f"JSON解析异常: {e}"
                print(f"  {error_msg}")
                if retry < max_retries:
                    print(f"  重试 {retry + 1}/{max_retries}...")
                    continue
                return "API调用失败：响应格式错误"
            except Exception as e:
                error_msg = f"LLM API调用失败: {e}"
                print(f"  {error_msg}")
                if retry < max_retries:
                    print(f"  重试 {retry + 1}/{max_retries}...")
                    continue
                return f"API调用异常: {str(e)}"
                
        return "API调用失败：重试次数耗尽"

    def _parse_llm_response(self, response_str, expected_count, prefix="句子", separator=None):
        """
        解析LLM返回的带编号的响应字符串 - 改进版本支持多种格式和自定义分隔符。
        """
        results = []
        try:
            lines = response_str.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                content_part = None
                # 提取冒号或点号后面的主要内容
                if f'{prefix} ' in line and (':' in line or '：' in line):
                    content_part = line.split(':', 1)[-1].strip() if ':' in line else line.split('：', 1)[-1].strip()
                elif line and line[0].isdigit() and '. ' in line:
                    content_part = line.split('. ', 1)[1].strip()
                elif line.startswith(f'{prefix}'):
                    content_part = line.replace(f'{prefix}', '').lstrip('0123456789: ：.').strip()
                
                if content_part:
                    content_part = content_part.strip('[]"\'')
                    if separator:
                        parts = content_part.split(separator)
                        if len(parts) >= 2:
                            results.append([parts[0].strip(), parts[1].strip()])
                        else:
                            results.append([content_part, "Error: 格式不匹配"])
                    else:
                        results.append(content_part)

            # 填充缺失的结果
            while len(results) < expected_count:
                error_result = ["Error: 解析失败"] * 2 if separator else "Error: 解析失败"
                results.append(error_result)
                
            return results[:expected_count]

        except Exception as e:
            print(f"解析LLM响应时出错: {e}")
            print(f"原始响应内容: {repr(response_str)}")
            error_result = ["Error: 解析异常"] * 2 if separator else "Error: 解析异常"
            return [error_result] * expected_count

    def generate_questions_cascade_new_strategy(self, graph, total_questions=50, batch_size=10):
        """
        全新的三阶段深度问题生成策略：
        1. 多源线索聚合 - 先确定答案，再寻找多条独立推理路径作为线索
        2. 深度模糊与故事编织 - 对线索进行原子化模糊，然后编织成连贯故事
        3. 问题生成与校验 - 生成问题并进行答案逻辑校验
        
        Args:
            graph: 知识图谱对象
            total_questions: 总的问题生成数量
            batch_size: 批次大小
        """
        print(f"🚀 启动全新的三阶段深度问题生成策略")
        print(f"目标问题数量：{total_questions}，批次大小：{batch_size}")
        print(f"策略流程：多源线索聚合 → 深度模糊与故事编织 → 问题生成与校验")
        
        # 第一阶段：多源线索聚合
        print(f"\n=== 第一阶段：多源线索聚合 ===")
        aggregated_samples = self._multi_source_clue_aggregation(graph, total_questions)
        
        if not aggregated_samples:
            print("❌ 线索聚合失败，无法继续生成问题")
            return []
        
        print(f"✅ 线索聚合完成，生成了 {len(aggregated_samples)} 个答案-线索组合")
        
        # 第二阶段和第三阶段：分批处理
        total_batches = (len(aggregated_samples) + batch_size - 1) // batch_size
        print(f"\n将 {len(aggregated_samples)} 个样本分为 {total_batches} 批次处理")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(aggregated_samples))
            batch_samples = aggregated_samples[start_idx:end_idx]
            
            print(f"\n=== 处理批次 {batch_idx + 1}/{total_batches} (样本 {start_idx+1}-{end_idx}) ===")
            
            # 第二阶段：深度模糊与故事编织
            print(f"第二阶段：深度模糊与故事编织 (批次 {batch_idx + 1})")
            self._deep_obfuscation_and_narrative_weaving_batch(batch_samples)
            self._save_stage_results(batch_samples, "stage2_deep_obfuscation", batch_idx + 1)
            
            # 第三阶段：问题生成与校验
            print(f"第三阶段：问题生成与校验 (批次 {batch_idx + 1})")
            self._question_generation_and_validation_batch(batch_samples)
            self._save_stage_results(batch_samples, "stage3_question_validation", batch_idx + 1)
        
        print(f"\n🎉 全新三阶段问题生成完成，共处理 {len(aggregated_samples)} 个样本")
        return aggregated_samples

    def generate_questions_cascade(self, path_samples, batch_size=10, questions_per_path=2):
        """
        [已废弃] 原有的四阶段问题生成级联，保留作为备用方案
        请使用新的 generate_questions_cascade_new_strategy 方法
        """
        print(f"⚠️  您正在使用已废弃的旧版问题生成方法")
        print(f"💡 建议使用新的三阶段策略：generate_questions_cascade_new_strategy")
        print(f"开始处理 {len(path_samples)} 个路径样本的问题生成")
        print(f"优化策略：整合链路 -> 选择目标 -> 【情境模糊与重构】 -> 问题构建")
        print(f"批次大小：{batch_size}，每路径生成问题数：{questions_per_path}")
        
        # 第一步：整合推理链路并生成多个问题样本
        expanded_samples = []
        for original_sample in path_samples:
            path = original_sample['path']
            if len(path) < 2:  # 路径太短，跳过
                continue
                
            # 从一条路径生成多个问题样本
            path_expanded_samples = self._expand_path_to_multiple_questions(
                original_sample, questions_per_path
            )
            expanded_samples.extend(path_expanded_samples)
        
        print(f"路径扩展完成：从 {len(path_samples)} 条路径生成了 {len(expanded_samples)} 个问题样本")
        
        # 第二步：分批处理扩展后的样本
        if expanded_samples:
            total_batches = (len(expanded_samples) + batch_size - 1) // batch_size
            print(f"将 {len(expanded_samples)} 个样本分为 {total_batches} 批次处理")
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(expanded_samples))
                batch_samples = expanded_samples[start_idx:end_idx]
                
                print(f"\n=== 处理批次 {batch_idx + 1}/{total_batches} (样本 {start_idx+1}-{end_idx}) ===")
                
                # 阶段一：线索整合与陈述生成 (保持不变)
                print(f"阶段一：整合线索信息 (批次 {batch_idx + 1})")
                self._process_clue_integration_batch(batch_samples)
                self._save_stage_results(batch_samples, "stage1_clue_integration", batch_idx + 1)
                
                # 阶段二：【新增】情境模糊与重构
                print(f"阶段二：情境模糊与重构 (批次 {batch_idx + 1})")
                self._process_obfuscation_and_weaving_batch(batch_samples)
                self._save_stage_results(batch_samples, "stage2_obfuscation_weaving", batch_idx + 1)
                
                # 阶段三：最终问题构建
                print(f"阶段三：基于模糊情境构建问题 (批次 {batch_idx + 1})")
                self._process_final_question_batch(batch_samples)
                self._save_stage_results(batch_samples, "stage3_final_questions", batch_idx + 1)
        
        print(f"问题生成完成，共处理 {len(expanded_samples)} 个样本")
        return expanded_samples

    def _multi_source_clue_aggregation(self, graph, target_count):
        """
        第一阶段：多源线索聚合
        先确定答案实体，然后从知识图谱中为这个答案寻找多条相互独立的推理路径作为线索
        
        Args:
            graph: 知识图谱对象
            target_count: 目标生成的问题数量
            
        Returns:
            list: 包含答案和多源线索的样本列表
        """
        print(f"开始多源线索聚合，目标数量：{target_count}")
        
        # 获取图中的所有节点
        all_nodes = list(graph.nodes())
        if len(all_nodes) < 10:
            print(f"❌ 图节点数量过少 ({len(all_nodes)})，无法进行有效的线索聚合")
            return []
        
        aggregated_samples = []
        attempts = 0
        max_attempts = target_count * 10  # 允许更多尝试次数
        
        print(f"图中共有 {len(all_nodes)} 个节点，开始聚合线索...")
        
        while len(aggregated_samples) < target_count and attempts < max_attempts:
            attempts += 1
            
            # 1. 随机选择目标实体作为答案
            answer_entity = random.choice(all_nodes)
            
            # 2. 为这个答案实体寻找多条独立的推理路径
            independent_paths = self._discover_independent_clue_paths(graph, answer_entity)
            
            if len(independent_paths) < 2:  # 至少需要2条独立路径
                continue
            
            # 3. 构建线索包
            clue_package = self._build_clue_package(answer_entity, independent_paths, graph)
            
            if clue_package:
                aggregated_samples.append(clue_package)
                if len(aggregated_samples) % 10 == 0:
                    print(f"  已聚合 {len(aggregated_samples)}/{target_count} 个样本")
        
        print(f"多源线索聚合完成：成功生成 {len(aggregated_samples)} 个样本 (尝试 {attempts} 次)")
        return aggregated_samples
    
    def _discover_independent_clue_paths(self, graph, answer_entity, max_paths=4, max_path_length=3):
        """
        为指定的答案实体发现多条相互独立的推理路径
        
        Args:
            graph: 知识图谱对象
            answer_entity: 目标答案实体
            max_paths: 最大路径数量
            max_path_length: 单条路径的最大长度
            
        Returns:
            list: 独立的推理路径列表
        """
        independent_paths = []
        used_entities = set([answer_entity])  # 已使用的实体，避免路径重叠
        
        # 获取答案实体的前驱节点（指向它的节点）
        predecessors = list(graph.predecessors(answer_entity))
        
        if len(predecessors) < 2:
            # 如果前驱太少，尝试从答案实体出发的路径
            successors = list(graph.successors(answer_entity))
            if len(successors) >= 2:
                # 构建从答案实体出发的路径
                for successor in successors[:max_paths]:
                    if successor not in used_entities:
                        path = self._extend_path_from_node(graph, answer_entity, successor, max_path_length)
                        if path and len(path) >= 2:
                            independent_paths.append(path)
                            used_entities.update(path)
                            if len(independent_paths) >= max_paths:
                                break
            return independent_paths
        
        # 从前驱节点开始构建指向答案的路径
        random.shuffle(predecessors)  # 随机化顺序
        
        for pred_node in predecessors:
            if pred_node in used_entities:
                continue
                
            # 构建从某个起点到答案实体的路径
            clue_path = self._build_clue_path_to_answer(graph, pred_node, answer_entity, max_path_length)
            
            if clue_path and len(clue_path) >= 2:
                # 检查路径是否与已有路径独立（没有共同的中间节点）
                path_entities = set(clue_path[:-1])  # 除了答案实体外的所有节点
                if not path_entities.intersection(used_entities):
                    independent_paths.append(clue_path)
                    used_entities.update(path_entities)
                    
                    if len(independent_paths) >= max_paths:
                        break
        
        return independent_paths
    
    def _extend_path_from_node(self, graph, start_node, next_node, max_length):
        """从指定节点开始扩展路径"""
        path = [start_node, next_node]
        current_node = next_node
        
        for _ in range(max_length - 2):
            successors = list(graph.successors(current_node))
            if not successors:
                break
            
            # 避免回到路径中已有的节点
            available_successors = [n for n in successors if n not in path]
            if not available_successors:
                break
            
            next_node = random.choice(available_successors)
            path.append(next_node)
            current_node = next_node
        
        return path
    
    def _build_clue_path_to_answer(self, graph, start_node, answer_entity, max_length):
        """构建从起点到答案实体的线索路径"""
        try:
            # 检查是否存在从start_node到answer_entity的路径
            if not graph.has_node(start_node) or not graph.has_node(answer_entity):
                return None
            
            # 尝试找到最短路径
            if hasattr(graph, 'has_path') and graph.has_path(start_node, answer_entity):
                import networkx as nx
                shortest_path = nx.shortest_path(graph, start_node, answer_entity)
                if len(shortest_path) <= max_length:
                    return shortest_path
            
            # 如果没有直接路径或路径太长，尝试构建间接路径
            path = [start_node]
            current_node = start_node
            
            for _ in range(max_length - 1):
                successors = list(graph.successors(current_node))
                if answer_entity in successors:
                    # 找到了到答案的直接连接
                    path.append(answer_entity)
                    return path
                
                if not successors:
                    break
                
                # 选择一个后继节点继续
                available_successors = [n for n in successors if n not in path]
                if not available_successors:
                    break
                
                current_node = random.choice(available_successors)
                path.append(current_node)
            
            return None
            
        except Exception as e:
            print(f"构建线索路径时出错: {e}")
            return None
    
    def _build_clue_package(self, answer_entity, independent_paths, graph=None):
        """
        构建线索包：将多条独立路径整合为一个问题生成样本
        
        Args:
            answer_entity: 答案实体
            independent_paths: 多条独立的推理路径
            graph: 知识图谱对象（用于提取真实关系）
            
        Returns:
            dict: 包含答案和线索信息的样本
        """
        if not independent_paths:
            return None
        
        # 提取所有线索实体（路径终点，除了答案实体）
        clue_entities = []
        clue_paths_info = []
        
        for i, path in enumerate(independent_paths):
            if len(path) >= 2:
                # 路径信息
                path_triples = self._convert_path_to_triples(path, graph)
                clue_paths_info.append({
                    'path_id': i + 1,
                    'path_nodes': path,
                    'path_triples': path_triples,
                    'clue_entity': path[-1] if path[-1] != answer_entity else path[-2]
                })
                
                # 线索实体（路径中除答案外的关键实体）
                if path[-1] != answer_entity:
                    clue_entities.append(path[-1])
                elif len(path) >= 2:
                    clue_entities.append(path[-2])
        
        if not clue_entities:
            return None
        
        # 生成样本ID
        sample_id = f"multi_clue_{hash(answer_entity)}_{len(independent_paths)}"
        
        clue_package = {
            'sample_id': sample_id,
            'strategy': 'multi_source_clue_aggregation',
            'answer_entity': answer_entity,
            'independent_paths': independent_paths,
            'clue_entities': clue_entities,
            'clue_paths_info': clue_paths_info,
            'num_clue_paths': len(independent_paths),
            'generation_timestamp': datetime.now().isoformat(),
            'status': 'aggregated'
        }
        
        return clue_package
    
    def _convert_path_to_triples(self, path, graph=None):
        """将节点路径转换为三元组格式"""
        if len(path) < 2:
            return []
        
        triples = []
        for i in range(len(path) - 1):
            subject = path[i]
            obj = path[i + 1]
            
            # 如果提供了图对象，尝试获取真实的关系
            if graph and hasattr(graph, 'get_edge_data'):
                edge_data = graph.get_edge_data(subject, obj)
                if edge_data:
                    # 从边数据中选择一个关系
                    if isinstance(edge_data, dict):
                        relations = list(edge_data.keys())
                        relation = relations[0] if relations else '相关'
                    else:
                        relation = '相关'
                else:
                    relation = '相关'
            else:
                relation = '相关'  # 默认关系
            
            triple = {
                'subject': subject,
                'relation': relation,
                'object': obj
            }
            triples.append(triple)
        
        return triples

    def _expand_path_to_multiple_questions(self, original_sample, questions_per_path):
        """
        从一条推理路径生成多个问题样本，每个样本选择不同的目标实体
        """
        path = original_sample['path']
        expanded_samples = []
        
        # 收集路径中的所有实体（去重）
        all_entities = []
        all_relations = []
        
        for triple in path:
            all_entities.extend([triple['subject'], triple['object']])
            all_relations.append({
                'relation': triple['relation'],
                'subject': triple['subject'],
                'object': triple['object']
            })
        
        # 去重但保持顺序
        unique_entities = list(dict.fromkeys(all_entities))
        
        # 如果实体数量不足，直接返回基础样本
        if len(unique_entities) < 2:
            return [original_sample]
        
        # 随机选择目标实体（确保生成足够的样本）
        target_entities = []
        if len(unique_entities) >= questions_per_path:
            target_entities = random.sample(unique_entities, questions_per_path)
        else:
            # 如果实体不够，进行重复采样
            target_entities = random.choices(unique_entities, k=questions_per_path)
        
        # 为每个目标实体生成一个样本
        for i, target_entity in enumerate(target_entities):
            # 寻找与目标实体相关的关系
            target_relations = []
            for rel_info in all_relations:
                if rel_info['subject'] == target_entity or rel_info['object'] == target_entity:
                    if rel_info['object'] == target_entity:
                        target_relations.append(rel_info['relation'])
            
            # 如果没有找到相关关系，使用默认关系
            if not target_relations:
                target_relations = ['相关属性']
            
            # 构建线索信息（排除目标实体的三元组）
            clue_triples = []
            target_triple = None
            
            for triple in path:
                if triple['object'] == target_entity:
                    target_triple = triple
                else:
                    clue_triples.append(triple)
            
            # 如果没有找到目标三元组，使用路径中的最后一个
            if target_triple is None and path:
                target_triple = path[-1]
                target_entity = target_triple['object']
                clue_triples = path[:-1]
            
            # 创建扩展样本
            expanded_sample = {
                'sample_id': f"{original_sample.get('sample_id', 'unknown')}_{i+1}",
                'original_path': path,
                'target_entity': target_entity,
                'target_relation': target_triple['relation'] if target_triple else target_relations[0],
                'clue_triples': clue_triples,
                'clue_entities': [entity for entity in unique_entities if entity != target_entity],
                
                # 保留原始信息
                'original_reasoning_path': [
                    {'step': j + 1, 'subject': triple['subject'], 'relation': triple['relation'], 'object': triple['object']} 
                    for j, triple in enumerate(path)
                ],
                'reasoning_chain_raw': " -> ".join([f"({t['subject']},{t['relation']},{t['object']})" for t in path]),
                
                # 新增字段
                'question_generation_strategy': 'random_target_selection',
                'target_selection_method': f'random_from_{len(unique_entities)}_entities',
                'clue_count': len(clue_triples)
            }
            
            expanded_samples.append(expanded_sample)
        
        return expanded_samples

    def _process_clue_integration_batch(self, batch_samples):
        """
        【新阶段一】：线索整合与陈述生成 - 将线索三元组整合为连贯的背景陈述
        """
        prompt = """你是一个信息整合专家。请将以下每组事实三元组整合为一段连贯的背景陈述，要求：

1. 将所有给定的事实信息自然地组织在一起
2. 语言流畅、逻辑清晰，避免简单的堆砌
3. 保持客观中性，不做额外推理或暗示
4. 确保所有关键实体和关系都得到体现
5. 不要提及或暗示任何未在线索中出现的信息

现在，请处理以下线索组合：
"""
        
        for i, sample in enumerate(batch_samples):
            clue_triples = sample['clue_triples']
            if clue_triples:
                clue_descriptions = []
                for triple in clue_triples:
                    clue_descriptions.append(f"({triple['subject']}, {triple['relation']}, {triple['object']})")
                clue_str = " + ".join(clue_descriptions)
                prompt += f"\n线索组合 {i+1}: {clue_str}"
            else:
                prompt += f"\n线索组合 {i+1}: (暂无具体线索)"
        
        prompt += "\n\n请严格按照以下格式返回，每个陈述占一行:\n陈述 1: [整合后的背景陈述]\n陈述 2: [整合后的背景陈述]\n..."

        results_str = self._batch_llm_call(prompt)
        if results_str and not results_str.startswith("API调用失败"):
            parsed_statements = self._parse_llm_response(results_str, len(batch_samples), "陈述")
            success_count = 0
            for i, sample in enumerate(batch_samples):
                if i < len(parsed_statements) and not parsed_statements[i].startswith("Error"):
                    sample['integrated_clue_statement'] = parsed_statements[i]
                    success_count += 1
                else:
                    # 回退：基于线索实体的简化描述
                    clue_entities = sample.get('clue_entities', [])
                    if clue_entities:
                        sample['integrated_clue_statement'] = f"已知实体{clue_entities[0]}与其他相关实体存在多种关联关系"
                    else:
                        sample['integrated_clue_statement'] = "已知存在一系列相关的实体关系"
            
            print(f"  阶段一完成，线索整合成功处理 {success_count}/{len(batch_samples)} 个样本")
        else:
            print(f"  阶段一API调用失败: {results_str}")
            # 全部回退处理
            for sample in batch_samples:
                clue_entities = sample.get('clue_entities', [])
                if clue_entities:
                    sample['integrated_clue_statement'] = f"已知实体{clue_entities[0]}与其他相关实体存在多种关联关系"
                else:
                    sample['integrated_clue_statement'] = "已知存在一系列相关的实体关系"

    def _deep_obfuscation_and_narrative_weaving_batch(self, batch_samples):
        """
        第二阶段：深度模糊与故事编织（全新实现）
        
        这个阶段将：
        1. 对每个线索实体进行"原子化模糊"处理
        2. 将所有模糊化的描述编织成一个连贯的悬疑故事
        3. 确保故事指向共同目标但不泄露答案
        """
        print(f"  开始深度模糊与故事编织处理...")
        
        # 第一步：对所有线索实体进行原子化模糊
        self._atomic_obfuscation_batch(batch_samples)
        
        # 第二步：将模糊描述编织成故事
        self._narrative_weaving_batch(batch_samples)
        
        print(f"  深度模糊与故事编织完成")
    
    def _atomic_obfuscation_batch(self, batch_samples):
        """
        原子化模糊：对每个线索实体进行独立的、上下文无关的模糊化处理
        """
        print(f"    执行原子化模糊处理...")
        
        # 收集所有需要模糊化的实体
        all_entities_to_obfuscate = []
        entity_to_samples = {}  # 实体到样本的映射
        
        for sample in batch_samples:
            clue_entities = sample.get('clue_entities', [])
            for entity in clue_entities:
                if entity not in all_entities_to_obfuscate:
                    all_entities_to_obfuscate.append(entity)
                
                if entity not in entity_to_samples:
                    entity_to_samples[entity] = []
                entity_to_samples[entity].append(sample)
        
        if not all_entities_to_obfuscate:
            print(f"    警告：没有找到需要模糊化的实体")
            for sample in batch_samples:
                sample['obfuscated_entities'] = {}
            return
        
        # 批量对实体进行模糊化
        prompt = """你是一位信息抽象专家。请为以下每个实体生成一个模糊但具有唯一指向性的描述。

要求：
1. 描述中不能出现该实体的具体名称
2. 描述要足够模糊，但又要能唯一指向该实体
3. 使用通用的、描述性的语言
4. 每个描述控制在15字以内

示例：
- 陈晓卿 -> 一位著名的美食纪录片导演
- 中国传媒大学 -> 一所以信息传播闻名的高等学府
- 李立宏 -> 一位声音浑厚的国家级配音演员

现在请处理以下实体：
"""
        
        for i, entity in enumerate(all_entities_to_obfuscate):
            prompt += f"\n实体 {i+1}: {entity}"
        
        prompt += "\n\n请严格按照以下格式返回，每个描述占一行:\n描述 1: [模糊化描述]\n描述 2: [模糊化描述]\n..."
        
        results_str = self._batch_llm_call(prompt)
        
        # 解析结果并分配给样本
        obfuscated_descriptions = {}
        
        if results_str and not results_str.startswith("API调用失败"):
            parsed_descriptions = self._parse_llm_response(results_str, len(all_entities_to_obfuscate), "描述")
            
            for i, entity in enumerate(all_entities_to_obfuscate):
                if i < len(parsed_descriptions) and not parsed_descriptions[i].startswith("Error"):
                    obfuscated_descriptions[entity] = parsed_descriptions[i]
                else:
                    # 回退：简单的通用描述
                    obfuscated_descriptions[entity] = f"一个与{entity[:2]}相关的实体"
        else:
            print(f"    原子化模糊API调用失败，使用回退策略")
            for entity in all_entities_to_obfuscate:
                obfuscated_descriptions[entity] = f"一个相关实体"
        
        # 将模糊化结果分配给各个样本
        for sample in batch_samples:
            sample['obfuscated_entities'] = {}
            clue_entities = sample.get('clue_entities', [])
            for entity in clue_entities:
                if entity in obfuscated_descriptions:
                    sample['obfuscated_entities'][entity] = obfuscated_descriptions[entity]
        
        success_count = len([s for s in batch_samples if s.get('obfuscated_entities')])
        print(f"    原子化模糊完成：{success_count}/{len(batch_samples)} 个样本")
    
    def _narrative_weaving_batch(self, batch_samples):
        """
        故事编织：将模糊化的描述编织成连贯的悬疑故事
        """
        print(f"    执行故事编织...")
        
        valid_samples = []
        for sample in batch_samples:
            obfuscated_entities = sample.get('obfuscated_entities', {})
            if obfuscated_entities:
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    警告：没有有效的模糊化实体可供故事编织")
            for sample in batch_samples:
                sample['narrative_story'] = "存在一些相关的实体和关系"
            return
        
        # 为每个样本单独编织故事
        for sample in valid_samples:
            self._weave_single_narrative(sample)
        
        success_count = len([s for s in valid_samples if s.get('narrative_story')])
        print(f"    故事编织完成：{success_count}/{len(valid_samples)} 个样本")
    
    def _weave_single_narrative(self, sample):
        """
        为单个样本编织故事
        """
        obfuscated_entities = sample.get('obfuscated_entities', {})
        answer_entity = sample.get('answer_entity', '')
        
        if not obfuscated_entities:
            sample['narrative_story'] = "存在一些相关的实体和关系"
            return
        
        # 构建故事编织的prompt
        prompt = f"""你是一位逻辑严谨的谜题构建专家。你的任务是根据下面提供的一系列关于**同一个未知目标**的模糊描述，将它们整合并改写成一段信息陈述。这段陈述将作为一道谜题的题干。

请遵循以下核心准则：
1.  **客观整合**：将所有模糊描述作为独立的线索，客观地整合进一段话中。专注于事实的呈现，而不是创造一个故事。所有线索*最终***都描述的是**同一个主体**（可能是一个人、一个地方、一个概念等）。你必须将它们整合起来，共同指向这个唯一的、未知的目标。
2.  **避免文学化**：不要使用比喻、拟人、夸张等文学修辞手法。语言风格应保持中立、简洁、直接，类似于百科全书或事实报告。
3.  **建立逻辑关联**：确保各个线索之间存在内在的逻辑联系，共同指向一个最终答案，但不要直接揭示答案。
4.  **信息完整**：确保所有提供的模糊描述都以某种形式出现在最终的陈述中。
5.  **聚焦问题**：最终生成的文本应该是一段清晰的、用于引出问题的背景信息陈述，而不是一个故事。

**示例输入：**
-   模糊描述1: 一位著名的美食纪录片导演
-   模糊描述2: 一所以信息传播闻名的高等学府
-   模糊描述3: 一位声音浑厚的国家级配音演员

**符合要求的输出示例：**
“一位著名的美食纪录片导演，毕业于一所以信息传播闻名的高等学府。他导演的一部知名纪录片，其旁白由一位声音浑厚的国家级配音演员完成。”

现在，请根据以下提供的模糊描述，为我构建信息陈述：

"""
        
        descriptions = list(obfuscated_entities.values())
        for i, desc in enumerate(descriptions):
            prompt += f"{i+1}. {desc}\n"
        
        
        result = self._batch_llm_call(prompt)
        
        if result and not result.startswith("API调用失败"):
            sample['narrative_story'] = result.strip()
        else:
            # 回退：简单连接所有描述
            story = "，".join(descriptions)
            sample['narrative_story'] = f"这里涉及{story}等相关信息。"

    def _question_generation_and_validation_batch(self, batch_samples):
        """
        第三阶段：问题生成与校验（全新实现）
        
        这个阶段将：
        1. 基于故事生成问题
        2. 对生成的问题进行答案逻辑校验
        3. 确保问题可以通过推理得到正确答案
        """
        print(f"  开始问题生成与校验...")
        
        # 第一步：生成问题
        self._generate_questions_from_narrative_batch(batch_samples)
        
        # 第二步：校验问题
        self._validate_questions_batch(batch_samples)
        
        print(f"  问题生成与校验完成")
    
    def _generate_questions_from_narrative_batch(self, batch_samples):
        """
        基于故事背景生成问题
        """
        print(f"    执行问题生成...")
        
        valid_samples = []
        for sample in batch_samples:
            narrative_story = sample.get('narrative_story', '')
            answer_entity = sample.get('answer_entity', '')
            if narrative_story and not narrative_story.startswith("Error") and answer_entity:
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    警告：没有有效的故事背景可供问题生成")
            for sample in batch_samples:
                sample['generated_question'] = "这个实体是什么？"
            return
        
        # 为每个样本生成问题
        for sample in valid_samples:
            self._generate_single_question(sample)
        
        success_count = len([s for s in valid_samples if s.get('generated_question')])
        print(f"    问题生成完成：{success_count}/{len(valid_samples)} 个样本")
    
    def _generate_single_question(self, sample):
        """
        为单个样本生成问题
        """
        narrative_story = sample.get('narrative_story', '')
        answer_entity = sample.get('answer_entity', '')
        
        prompt = f"""你是一位精准的提问专家。你的任务是根据下面提供的【背景陈述】和【预设答案】，提出一个能够唯一指向该答案的简洁问题。

核心要求：
1.  **答案导向**：生成的问题的唯一正确答案必须是给定的【预设答案】。
2.  **自然衔接**：问题需要与【背景陈述】的结尾紧密衔接，读起来通顺自然。
3.  **禁止泄露**：问题本身不能包含【预设答案】的任何直接线索或文字。
4.  **简洁清晰**：问题表述应简洁、清晰，符合人类的提问习惯，控制在20字以内。

---
【背景陈述】：
{narrative_story}

【预设答案】：
{answer_entity}
---

请严格根据以上要求，直接生成一个问题，不要包含任何额外说明或 "问题：" 前缀。
"""

        
        result = self._batch_llm_call(prompt)
        
        if result and not result.startswith("API调用失败"):
            # 清理结果，提取纯问题
            question = result.strip()
            # 移除可能的前缀
            if question.startswith("问题：") or question.startswith("问："):
                question = question.split("：", 1)[-1].strip()
            
            sample['generated_question'] = question
        else:
            # 回退：生成通用问题
            sample['generated_question'] = "这是什么？"
    
    def _validate_questions_batch(self, batch_samples):
        """
        批量校验问题的逻辑正确性
        """
        print(f"    执行问题校验...")
        
        valid_samples = []
        for sample in batch_samples:
            if sample.get('generated_question') and sample.get('answer_entity'):
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    警告：没有有效的问题可供校验")
            return
        
        # 批量校验
        validation_prompt = """你是一位逻辑推理专家。请判断以下每组【背景故事+问题】是否能够逻辑推理出对应的【答案】。

评判标准：
1. 背景故事中的信息是否足够推导出答案
2. 问题与答案是否逻辑一致
3. 推理链条是否完整

请对每个问题组合只回答"通过"或"不通过"：

"""
        
        for i, sample in enumerate(valid_samples):
            narrative_story = sample.get('narrative_story', '')
            question = sample.get('generated_question', '')
            answer = sample.get('answer_entity', '')
            
            validation_prompt += f"""
问题组合 {i+1}:
背景故事：{narrative_story}
问题：{question}
答案：{answer}
"""
        
        validation_prompt += "\n请严格按照以下格式返回，每个结果占一行:\n结果 1: [通过/不通过]\n结果 2: [通过/不通过]\n..."
        
        validation_result = self._batch_llm_call(validation_prompt)
        
        # 解析校验结果
        if validation_result and not validation_result.startswith("API调用失败"):
            parsed_results = self._parse_llm_response(validation_result, len(valid_samples), "结果")
            
            passed_count = 0
            for i, sample in enumerate(valid_samples):
                if i < len(parsed_results):
                    validation_status = parsed_results[i].lower()
                    sample['validation_status'] = "通过" if "通过" in validation_status else "不通过"
                    
                    if "通过" in validation_status:
                        passed_count += 1
                        sample['final_status'] = 'validated'
                    else:
                        sample['final_status'] = 'validation_failed'
                else:
                    sample['validation_status'] = "未校验"
                    sample['final_status'] = 'validation_failed'
            
            print(f"    校验完成：{passed_count}/{len(valid_samples)} 个问题通过校验")
        else:
            print(f"    校验API调用失败，标记所有问题为未校验状态")
            for sample in valid_samples:
                sample['validation_status'] = "API失败"
                sample['final_status'] = 'validation_failed'
        
        # 为不在valid_samples中的样本设置默认状态
        for sample in batch_samples:
            if 'validation_status' not in sample:
                sample['validation_status'] = "无效问题"
                sample['final_status'] = 'invalid'

    def _process_obfuscation_and_weaving_batch(self, batch_samples):
        """
        【新阶段二】：情境模糊与重构 - 将精确的事实陈述转化为模糊但有趣的叙述
        
        这个阶段是整个优化方案的核心，它将：
        1. 对实体进行泛化处理，隐藏具体的名称
        2. 将数值变得模糊
        3. 通过故事性叙述隐藏直接的关系
        4. 创建相对时间和逻辑连接
        5. 确保不泄露目标答案
        """
        prompt = """你是一位顶级的出题专家和故事编织者。你的任务是将以下【原始事实陈述】转化为一段引人入胜且充满悬念的【背景故事】。

转化要求：
1. **信息模糊化**：
   - **实体泛化**：将具体的人名、地名、组织名等替换为更通用的描述（例如，"诗人C" -> "一位诗人"；"平汉铁路" -> "一条重要的交通干线"）。
   - **数值模糊**：将精确的数字或年份替换为相对或模糊的描述（例如，"36平方千米" -> "超过35平方千米"；"1933年" -> "在某场混战爆发后的第三年"）。
   - **关系隐藏**：不要直接陈述实体间的关系，而是通过描述事件来暗示它们。
   **注意**，在这一步骤中，**不可以引入任何错误信息或虚构的事实。**

2. **情境重构**：
   - **建立相对时间**：创造基于事件的时间线索（例如，"5年后"、"两年后又恢复了"）。
   - **逻辑串联**：将离散的事实有机地串联成一个连贯、流畅的故事，而不是简单的罗列。
   - **保持核心逻辑**：故事必须保留原始事实中的核心因果和时序关系，确保问题最终可以被解答。

3. **禁止泄露答案**：
   - 在生成的【背景故事】中，绝对不能出现【目标实体】（即问题的答案）。

现在，请处理以下内容：
"""
        
        valid_samples = []
        for i, sample in enumerate(batch_samples):
            if 'integrated_clue_statement' in sample and not sample['integrated_clue_statement'].startswith("Error"):
                prompt += f"\n原始事实陈述 {len(valid_samples)+1}: {sample['integrated_clue_statement']}"
                prompt += f"\n目标实体 {len(valid_samples)+1}: {sample.get('target_entity', '')}\n"
                valid_samples.append(sample)
        
        if not valid_samples:
            print("  阶段二：没有有效的事实陈述可供模糊化处理")
            for sample in batch_samples:
                sample['obfuscated_story'] = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
            return

        prompt += "\n\n请严格按照以下格式返回，每个背景故事占一行:\n背景故事 1: [转化后的模糊化故事]\n背景故事 2: [转化后的模糊化故事]\n..."
        
        results_str = self._batch_llm_call(prompt)
        if results_str and not results_str.startswith("API调用失败"):
            parsed_stories = self._parse_llm_response(results_str, len(valid_samples), "背景故事")
            success_count = 0
            for i, sample in enumerate(valid_samples):
                if i < len(parsed_stories) and not parsed_stories[i].startswith("Error"):
                    sample['obfuscated_story'] = parsed_stories[i]
                    success_count += 1
                else:
                    # 回退到整合的线索陈述，但进行简单的泛化处理
                    orig_statement = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
                    target_entity = sample.get('target_entity', '')
                    # 简单泛化处理：替换目标实体为"某个实体"
                    if target_entity and target_entity in orig_statement:
                        simplified_story = orig_statement.replace(target_entity, "某个相关实体")
                    else:
                        simplified_story = orig_statement
                    sample['obfuscated_story'] = simplified_story
            
            # 处理无效样本
            for sample in batch_samples:
                if sample not in valid_samples:
                    sample['obfuscated_story'] = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
                    
            print(f"  阶段二完成，成功模糊化处理 {success_count}/{len(batch_samples)} 个样本")
        else:
            print(f"  阶段二API调用失败: {results_str}")
            # 回退处理：简单泛化
            for sample in batch_samples:
                orig_statement = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
                target_entity = sample.get('target_entity', '')
                # 简单泛化处理：替换目标实体为"某个实体"
                if target_entity and target_entity in orig_statement:
                    simplified_story = orig_statement.replace(target_entity, "某个相关实体")
                else:
                    simplified_story = orig_statement
                sample['obfuscated_story'] = simplified_story

    def _process_targeted_question_batch(self, batch_samples):
        """
        [已废弃] 旧的问题构建函数 - 保留作为参考
        """
        pass
        
    def _process_final_question_batch(self, batch_samples):
        """
        【新阶段三】：最终问题构建 - 基于模糊化故事和目标实体生成问题
        
        这个阶段是整个流程的收尾，它将：
        1. 基于模糊化的背景故事构建问题
        2. 确保问题自然地衔接故事结尾
        3. 问题的答案必须是目标实体，但不能透露任何直接线索
        """
        prompt = """你是一位提问专家。请根据下面提供的【背景故事】和【答案】，提出一个简洁、自然的问句。

要求：
1. 问题必须与【背景故事】的结尾紧密衔接，像是故事叙述的自然延续。
2. 问题的答案必须是给定的【答案】。
3. 问题本身不能包含答案的任何直接线索。
4. 问题表述应简洁、清晰，符合人类的提问习惯。

现在，请处理以下内容：
"""
        
        valid_samples = []
        for i, sample in enumerate(batch_samples):
            if 'obfuscated_story' in sample and not sample['obfuscated_story'].startswith("Error"):
                story = sample['obfuscated_story']
                target_entity = sample.get('target_entity', '')
                
                # 限制故事长度以避免prompt过长
                if len(story) > 300:
                    story = story[:300] + "..."
                
                prompt += f"\n背景故事 {len(valid_samples)+1}: {story}"
                prompt += f"\n答案 {len(valid_samples)+1}: {target_entity}\n"
                valid_samples.append(sample)
        
        if not valid_samples:
            print("  阶段三：没有有效的模糊故事可供问题生成")
            for sample in batch_samples:
                sample['final_question'] = f"基于以上信息，请问答案是什么？"
                sample['answer'] = sample.get('target_entity', '')
            return

        prompt += "\n请严格按照以下格式返回，每个问题占一行:\n问题 1: [最终的问题]\n问题 2: [最终的问题]\n..."
        
        results_str = self._batch_llm_call(prompt)
        if results_str and not results_str.startswith("API调用失败"):
            parsed_questions = self._parse_llm_response(results_str, len(valid_samples), "问题")
            success_count = 0
            for i, sample in enumerate(valid_samples):
                if i < len(parsed_questions) and not parsed_questions[i].startswith("Error"):
                    sample['final_question'] = parsed_questions[i]
                    success_count += 1
                else:
                    # 回退逻辑
                    target_relation = sample.get('target_relation', '相关信息')
                    sample['final_question'] = f"根据这段描述，你能推断出{target_relation}是什么吗？"
                
                # 设置答案
                sample['answer'] = sample.get('target_entity', '')
            
            # 处理无效样本
            for sample in batch_samples:
                if sample not in valid_samples:
                    target_relation = sample.get('target_relation', '相关信息')
                    sample['final_question'] = f"根据这段描述，你能推断出{target_relation}是什么吗？"
                    sample['answer'] = sample.get('target_entity', '')
                    
            print(f"  阶段三完成，成功生成 {success_count}/{len(batch_samples)} 个最终问题")
        else:
            print(f"  阶段三API调用失败: {results_str}")
            # 回退处理
            for sample in batch_samples:
                target_relation = sample.get('target_relation', '相关信息')
                sample['final_question'] = f"根据这段描述，你能推断出{target_relation}是什么吗？"
                sample['answer'] = sample.get('target_entity', '')

    def _save_stage_results(self, batch_samples, stage_name, batch_idx):
        """
        保存每个阶段的处理结果 - 新三阶段策略版本
        """
        import os
        from datetime import datetime
        
        # 创建阶段结果保存目录
        stage_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "stage_results")
        os.makedirs(stage_dir, exist_ok=True)
        
        # 创建阶段文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stage_file = os.path.join(stage_dir, f"{stage_name}_batch_{batch_idx:03d}_{timestamp}.jsonl")
        
        with open(stage_file, 'w', encoding='utf-8') as f:
            for i, sample in enumerate(batch_samples):
                # 判断是新策略还是旧策略的数据结构
                if sample.get('strategy') == 'multi_source_clue_aggregation':
                    # 新三阶段策略的数据结构
                    stage_result = {
                        'sample_id': sample.get('sample_id', f"batch_{batch_idx:03d}_sample_{i+1:02d}"),
                        'strategy': sample.get('strategy', 'multi_source_clue_aggregation'),
                        'answer_entity': sample.get('answer_entity', ''),
                        'independent_paths': sample.get('independent_paths', []),
                        'clue_entities': sample.get('clue_entities', []),
                        'num_clue_paths': sample.get('num_clue_paths', 0),
                        'generation_timestamp': sample.get('generation_timestamp', ''),
                        'status': sample.get('status', 'unknown')
                    }
                    
                    # 根据阶段添加相应的新策略字段
                    if stage_name == "stage2_deep_obfuscation":
                        stage_result.update({
                            'obfuscated_entities': sample.get('obfuscated_entities', {}),
                            'narrative_story': sample.get('narrative_story', ''),
                            'clue_paths_info': sample.get('clue_paths_info', [])
                        })
                    elif stage_name == "stage3_question_validation":
                        stage_result.update({
                            'obfuscated_entities': sample.get('obfuscated_entities', {}),
                            'narrative_story': sample.get('narrative_story', ''),
                            'generated_question': sample.get('generated_question', ''),
                            'validation_status': sample.get('validation_status', ''),
                            'final_status': sample.get('final_status', ''),
                            'clue_paths_info': sample.get('clue_paths_info', [])
                        })
                
                else:
                    # 旧策略的数据结构（向后兼容）
                    stage_result = {
                        'sample_id': f"batch_{batch_idx:03d}_sample_{i+1:02d}",
                        'original_reasoning_path': sample.get('original_reasoning_path', []),
                        'reasoning_chain_raw': sample.get('reasoning_chain_raw', ''),
                        'target_entity': sample.get('target_entity', ''),
                        'target_relation': sample.get('target_relation', ''),
                        'clue_triples': sample.get('clue_triples', []),
                        'clue_entities': sample.get('clue_entities', []),
                        # 添加旧版字段
                        'obfuscated_story': sample.get('obfuscated_story', ''),
                        'question_generation_strategy': sample.get('question_generation_strategy', 'unknown'),
                        'clue_count': sample.get('clue_count', 0)
                    }
                    
                    # 根据阶段添加相应的旧策略字段
                    if stage_name == "stage1_clue_integration":
                        stage_result.update({
                            'integrated_clue_statement': sample.get('integrated_clue_statement', '')
                        })
                    elif stage_name == "stage2_context_weaving" or stage_name == "stage2_obfuscation_weaving":
                        stage_result.update({
                            'integrated_clue_statement': sample.get('integrated_clue_statement', ''),
                            'contextual_narrative': sample.get('contextual_narrative', '')
                        })
                    elif stage_name == "stage3_targeted_questions":
                        stage_result.update({
                            'integrated_clue_statement': sample.get('integrated_clue_statement', ''),
                            'contextual_narrative': sample.get('contextual_narrative', ''),
                            'final_question': sample.get('final_question', ''),
                            'answer': sample.get('answer', '')
                        })
                
                f.write(json.dumps(stage_result, ensure_ascii=False) + '\n')
        
        print(f"  阶段结果已保存到: {stage_file}")

    def generate_comprehensive_qa_new_strategy(self, graph, total_questions=50, batch_size=10):
        """
        基于新三阶段策略生成全面的问答数据集
        
        Args:
            graph: 知识图谱对象  
            total_questions: 总问题数量
            batch_size: 批次大小
            
        Returns:
            dict: 包含所有生成结果的字典
        """
        print(f"\n" + "="*80)
        print(f"🚀 启动基于三阶段深度策略的全面问答数据集生成")
        print(f"="*80)
        
        # 执行新的三阶段问题生成
        all_samples = self.generate_questions_cascade_new_strategy(graph, total_questions, batch_size)
        
        if not all_samples:
            print("❌ 新策略问题生成失败，无法创建数据集")
            return {'success': False, 'samples': []}
        
        # 生成各种格式的问答数据集
        print(f"\n🎯 基于新策略生成问答数据集...")
        
        # 1. 生成验证通过的问题数据集
        validated_questions = self._create_validated_questions_dataset(all_samples)
        
        # 2. 生成简化格式问答数据集  
        simple_count = self._create_simple_qa_new_strategy(validated_questions)
        
        # 3. 生成详细格式问答数据集
        detailed_count = self._create_detailed_qa_new_strategy(all_samples)
        
        # 4. 生成数据集统计报告
        self._generate_new_strategy_summary(all_samples, validated_questions, simple_count, detailed_count)
        
        result = {
            'success': True,
            'total_samples': len(all_samples),
            'validated_questions': validated_questions,
            'validated_count': len(validated_questions),
            'simple_count': simple_count,
            'detailed_count': detailed_count,
            'validation_rate': len(validated_questions) / len(all_samples) * 100 if all_samples else 0
        }
        
        print(f"\n✅ 基于新三阶段策略的问答数据集生成完成!")
        return result
    
    def _create_validated_questions_dataset(self, samples):
        """创建验证通过的问题数据集"""
        validated_questions = []
        
        for sample in samples:
            # 只包含验证通过的问题
            if sample.get('final_status') == 'validated':
                qa_item = {
                    'id': sample.get('sample_id', ''),
                    'question': sample.get('generated_question', ''),
                    'answer': sample.get('answer_entity', ''),
                    'background_story': sample.get('narrative_story', ''),
                    'strategy': 'multi_source_clue_aggregation',
                    'clue_entities': sample.get('clue_entities', []),
                    'num_clue_paths': sample.get('num_clue_paths', 0),
                    'validation_status': sample.get('validation_status', ''),
                    'generation_metadata': {
                        'has_background': bool(sample.get('narrative_story')),
                        'question_length': len(sample.get('generated_question', '')),
                        'story_length': len(sample.get('narrative_story', '')),
                        'clue_count': len(sample.get('clue_entities', [])),
                        'timestamp': sample.get('generation_timestamp', '')
                    }
                }
                validated_questions.append(qa_item)
        
        # 保存验证通过的问题数据集
        output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "validated_qa_new_strategy.jsonl")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for item in validated_questions:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        print(f"验证通过的问题数据集已保存: {output_path}")
        print(f"验证通过问题数: {len(validated_questions)}")
        
        return validated_questions
    
    def _create_simple_qa_new_strategy(self, validated_questions):
        """创建简化格式的问答数据集（新策略版本）"""
        output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
        output_path = os.path.join(output_dir, "simple_qa_new_strategy.jsonl")
        
        simple_count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for item in validated_questions:
                simple_qa = {
                    'question': item['question'],
                    'answer': item['answer']
                }
                f.write(json.dumps(simple_qa, ensure_ascii=False) + '\n')
                simple_count += 1
        
        print(f"简化格式问答数据集已保存: {output_path}")
        print(f"简化格式问题数: {simple_count}")
        return simple_count
    
    def _create_detailed_qa_new_strategy(self, all_samples):
        """创建详细格式的问答数据集（包含所有信息）"""
        output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
        output_path = os.path.join(output_dir, "detailed_qa_new_strategy.jsonl")
        
        detailed_count = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for sample in all_samples:
                detailed_item = {
                    'sample_id': sample.get('sample_id', ''),
                    'strategy': sample.get('strategy', ''),
                    'answer_entity': sample.get('answer_entity', ''),
                    'independent_paths': sample.get('independent_paths', []),
                    'clue_entities': sample.get('clue_entities', []),
                    'obfuscated_entities': sample.get('obfuscated_entities', {}),
                    'narrative_story': sample.get('narrative_story', ''),
                    'generated_question': sample.get('generated_question', ''),
                    'validation_status': sample.get('validation_status', ''),
                    'final_status': sample.get('final_status', ''),
                    'generation_timestamp': sample.get('generation_timestamp', ''),
                    'debug_info': {
                        'num_clue_paths': sample.get('num_clue_paths', 0),
                        'clue_paths_info': sample.get('clue_paths_info', [])
                    }
                }
                f.write(json.dumps(detailed_item, ensure_ascii=False) + '\n')
                detailed_count += 1
        
        print(f"详细格式问答数据集已保存: {output_path}")
        print(f"详细格式条目数: {detailed_count}")
        return detailed_count
    
    def _generate_new_strategy_summary(self, all_samples, validated_questions, simple_count, detailed_count):
        """生成新策略的数据集统计报告"""
        print(f"\n" + "="*80)
        print(f"📋 三阶段深度问题生成策略数据集报告")
        print(f"="*80)
        
        # 基本统计
        total_samples = len(all_samples)
        validated_count = len(validated_questions)
        validation_rate = validated_count / total_samples * 100 if total_samples > 0 else 0
        
        # 策略分析
        status_distribution = {}
        for sample in all_samples:
            status = sample.get('final_status', 'unknown')
            status_distribution[status] = status_distribution.get(status, 0) + 1
        
        # 问题质量分析
        if validated_questions:
            avg_question_len = sum(len(q['question']) for q in validated_questions) / len(validated_questions)
            avg_story_len = sum(len(q['background_story']) for q in validated_questions) / len(validated_questions)
            avg_clue_count = sum(len(q['clue_entities']) for q in validated_questions) / len(validated_questions)
        else:
            avg_question_len = avg_story_len = avg_clue_count = 0
        
        print(f"📊 数据统计:")
        print(f"   • 总生成样本数: {total_samples}")
        print(f"   • 验证通过数: {validated_count}")
        print(f"   • 验证通过率: {validation_rate:.1f}%")
        print(f"   • 简化格式问题数: {simple_count}")
        print(f"   • 详细格式条目数: {detailed_count}")
        
        print(f"\n📈 质量指标:")
        print(f"   • 平均问题长度: {avg_question_len:.1f} 字符")
        print(f"   • 平均故事长度: {avg_story_len:.1f} 字符") 
        print(f"   • 平均线索数量: {avg_clue_count:.1f} 个")
        
        print(f"\n🎯 状态分布:")
        for status, count in status_distribution.items():
            percentage = count / total_samples * 100 if total_samples > 0 else 0
            print(f"   • {status}: {count} 个 ({percentage:.1f}%)")
        
        print(f"\n🔬 新策略特点:")
        print(f"   • 多源线索聚合：从单一路径转向多条独立线索")
        print(f"   • 原子化模糊：对每个实体进行独立的模糊化处理")
        print(f"   • 故事编织：将模糊线索编织成连贯的悬疑故事")
        print(f"   • 逻辑校验：确保问题可以通过推理得到答案")
        
        print(f"\n📁 生成文件:")
        print(f"   • validated_qa_new_strategy.jsonl: 验证通过的完整问题")
        print(f"   • simple_qa_new_strategy.jsonl: 纯问答格式")
        print(f"   • detailed_qa_new_strategy.jsonl: 包含调试信息的详细格式")
        
        return True

    def generate_questions_cascade_with_context(self, path_samples, batch_size=10, questions_per_path=2, generate_contextual=True):
        """
        执行完整的问题生成流程，并自动生成包含背景信息的问题数据集。
        
        Args:
            path_samples: 路径样本列表
            batch_size: 批次大小
            questions_per_path: 每条路径生成的问题数量
            generate_contextual: 是否自动生成背景信息格式的问题
        
        Returns:
            dict: 包含所有生成结果的字典
        """
        print(f"🚀 开始全新的问题生成流程（随机目标选择策略）")
        
        # 执行新的四阶段级联生成
        all_samples = self.generate_questions_cascade(path_samples, batch_size, questions_per_path)
        
        result = {
            'standard_samples': all_samples,
            'contextual_questions': None,
            'simple_count': 0
        }
        
        if generate_contextual and all_samples:
            print(f"\n🎯 开始生成背景信息格式的问题...")
            
            # 自动生成背景信息格式的问题
            contextual_results = self.generate_comprehensive_qa_datasets(all_samples)
            
            result.update({
                'contextual_questions': contextual_results['contextual_questions'],
                'simple_count': contextual_results['simple_count'],
                'total_samples': contextual_results['total_samples']
            })
            
            print(f"\n✅ 完整的问题生成流程已完成!")
            print(f"   • 标准格式问题: {len(all_samples)} 个")
            print(f"   • 背景式问题: {len(contextual_results['contextual_questions'])} 个")
            print(f"   • 简化格式问题: {contextual_results['simple_count']} 个")
        
        return result

    def generate_contextual_questions(self, samples, output_path=None):
        """
        生成包含背景信息的问题数据集，问题格式为 obfuscated_story + final_question
        优先使用模糊化后的故事作为背景信息，使问题更具挑战性
        """
        print(f"\n=== 开始生成包含模糊化背景的问题数据集 ===")
        
        if output_path is None:
            output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, "contextual_qa_dataset.jsonl")
        
        contextual_questions = []
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for idx, sample in enumerate(samples):
                # 优先使用模糊化的故事，回退到其他可用字段
                background_story = sample.get('obfuscated_story', '').strip()
                if not background_story:
                    background_story = sample.get('contextual_narrative', '').strip()
                if not background_story:
                    background_story = sample.get('integrated_clue_statement', '').strip()
                
                final_question = sample.get('final_question', '').strip()
                answer = sample.get('answer', '').strip()
                
                # 构建包含背景的完整问题
                if background_story and final_question:
                    # 确保叙述和问题之间有适当的连接
                    if background_story.endswith(('。', '.', '！', '!', '？', '?')):
                        contextual_question = f"{background_story} {final_question}"
                    else:
                        contextual_question = f"{background_story}。{final_question}"
                elif final_question:
                    # 如果没有情境叙述，只使用问题
                    contextual_question = final_question
                else:
                    # 回退方案
                    contextual_question = f"根据相关信息，请问{sample.get('target_relation', '答案')}是什么？"
                
                # 创建包含背景的问题记录
                contextual_record = {
                    "id": f"contextual_mq_{idx:05d}",
                    "question_with_context": contextual_question,
                    "answer": answer,
                    "target_entity": sample.get('target_entity', ''),
                    "target_relation": sample.get('target_relation', ''),
                    
                    # 原始组成部分（供参考）
                    "original_components": {
                        "background_story": background_story,
                        "final_question": final_question,
                        "integrated_clue_statement": sample.get('integrated_clue_statement', ''),
                        "obfuscated_story": sample.get('obfuscated_story', '')
                    },
                    
                    # 推理信息（放在最后）
                    "reasoning_info": {
                        "original_reasoning_path": sample.get('original_reasoning_path', []),
                        "reasoning_chain_raw": sample.get('reasoning_chain_raw', ''),
                        "clue_triples": sample.get('clue_triples', []),
                        "clue_entities": sample.get('clue_entities', []),
                        "clue_count": sample.get('clue_count', 0)
                    },
                    
                    # 元数据
                    "metadata": {
                        "source_sample_id": sample.get('sample_id', f'generated_{idx}'),
                        "question_length": len(contextual_question),
                        "has_context": bool(background_story),
                        "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "generation_method": "obfuscation_and_weaving",
                        "question_generation_strategy": sample.get('question_generation_strategy', 'unknown'),
                        "target_selection_method": sample.get('target_selection_method', 'unknown'),
                        "is_obfuscated": bool(sample.get('obfuscated_story', ''))
                    }
                }
                
                f.write(json.dumps(contextual_record, ensure_ascii=False) + '\n')
                contextual_questions.append(contextual_record)
        
        print(f"基于随机目标选择的背景式问题数据集已生成: {output_path}")
        print(f"总问题数: {len(contextual_questions)}")
        
        # 质量统计
        print(f"\n=== 背景式问题质量统计 ===")
        
        # 统计有背景的问题比例
        with_context_count = sum(1 for q in contextual_questions if q['metadata']['has_context'])
        context_percentage = with_context_count / len(contextual_questions) * 100 if contextual_questions else 0
        
        # 统计问题长度
        lengths = [q['metadata']['question_length'] for q in contextual_questions]
        avg_length = sum(lengths) / len(lengths) if lengths else 0
        
        # 统计答案完整性
        complete_answers = sum(1 for q in contextual_questions if q['answer'] and q['answer'] != '')
        answer_percentage = complete_answers / len(contextual_questions) * 100 if contextual_questions else 0
        
        # 统计线索多样性
        clue_counts = [q['reasoning_info']['clue_count'] for q in contextual_questions]
        avg_clue_count = sum(clue_counts) / len(clue_counts) if clue_counts else 0
        
        print(f"  包含背景信息: {with_context_count}/{len(contextual_questions)} ({context_percentage:.1f}%)")
        print(f"  平均问题长度: {avg_length:.1f} 字符")
        print(f"  答案完整性: {complete_answers}/{len(contextual_questions)} ({answer_percentage:.1f}%)")
        print(f"  平均线索数量: {avg_clue_count:.1f} 个")
        
        # 展示样本
        print(f"\n=== 基于随机目标选择的问题样本展示 ===")
        for i in range(min(3, len(contextual_questions))):
            question = contextual_questions[i]
            print(f"\n--- 样本 {i+1} ---")
            print(f"ID: {question['id']}")
            print(f"目标实体: {question.get('target_entity', 'N/A')}")
            print(f"目标关系: {question.get('target_relation', 'N/A')}")
            print(f"线索数量: {question['reasoning_info']['clue_count']}")
            print(f"包含背景: {'是' if question['metadata']['has_context'] else '否'}")
            print(f"问题: {question['question_with_context'][:150]}{'...' if len(question['question_with_context']) > 150 else ''}")
            print(f"答案: {question['answer']}")
        
        return contextual_questions
    
    def create_simple_qa_format(self, contextual_questions=None, output_path=None):
        """
        创建简化的问答格式文件，只包含问题和答案
        """
        print(f"\n=== 创建简化问答格式 ===")
        
        if output_path is None:
            output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, "simple_qa_format.jsonl")
        
        # 如果没有提供contextual_questions，从文件读取
        if contextual_questions is None:
            contextual_file = os.path.join(os.path.dirname(output_path), "contextual_qa_dataset.jsonl")
            if not os.path.exists(contextual_file):
                print(f"错误：找不到背景式问题文件 {contextual_file}")
                return 0
            
            contextual_questions = []
            with open(contextual_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        contextual_questions.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        continue
        
        simple_count = 0
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for data in contextual_questions:
                # 创建简化格式
                simple_record = {
                    "question": data['question_with_context'],
                    "answer": data['answer']
                }
                
                f.write(json.dumps(simple_record, ensure_ascii=False) + '\n')
                simple_count += 1
        
        print(f"简化问答格式已生成: {output_path}")
        print(f"问题数: {simple_count}")
        
        return simple_count
    
    def generate_comprehensive_qa_datasets(self, samples):
        """
        生成全面的问答数据集，包括所有格式
        """
        print(f"\n" + "="*70)
        print(f"🚀 开始生成全面的问答数据集（随机目标选择策略）")
        print(f"="*70)
        
        # 1. 生成背景式问题数据集
        contextual_questions = self.generate_contextual_questions(samples)
        
        # 2. 生成简化格式
        simple_count = self.create_simple_qa_format(contextual_questions)
        
        # 3. 生成数据集总结报告
        self._generate_comprehensive_summary(len(samples), len(contextual_questions), simple_count)
        
        return {
            'contextual_questions': contextual_questions,
            'simple_count': simple_count,
            'total_samples': len(samples)
        }
    
    def _generate_comprehensive_summary(self, original_samples, contextual_count, simple_count):
        """生成全面的数据集总结报告"""
        print(f"\n" + "="*70)
        print(f"📋 基于随机目标选择的问题生成器数据集报告")
        print(f"="*70)
        
        # 检查生成的文件
        output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
        files_to_check = [
            ("背景式问题数据集", os.path.join(output_dir, "contextual_qa_dataset.jsonl")),
            ("简化问答格式", os.path.join(output_dir, "simple_qa_format.jsonl"))
        ]
        
        for name, filepath in files_to_check:
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath) / 1024  # KB
                print(f"✅ {name}: {file_size:.1f} KB")
                print(f"   📁 文件路径: {filepath}")
            else:
                print(f"❌ {name}: 文件不存在")
        
        print(f"\n📊 数据统计:")
        print(f"   • 原始样本数: {original_samples}")
        print(f"   • 背景式问题数: {contextual_count}")
        print(f"   • 简化格式问题数: {simple_count}")
        print(f"   • 转换成功率: {(contextual_count/original_samples*100):.1f}%" if original_samples > 0 else "   • 转换成功率: N/A")
        
        print(f"\n🎯 基于随机目标选择的问题特点:")
        print(f"   • 从完整推理链路随机选择目标实体")
        print(f"   • 每条路径可生成多个不同角度的问题")
        print(f"   • 线索信息更加丰富和多样化")
        print(f"   • 问题难度和类型更加均衡")
        
        print(f"\n🔄 生成方法演进:")
        print(f"   • 第一代: 完整路径 → 完整陈述 → 复杂陈述 → 基于陈述提问")
        print(f"   • 第二代: 线索路径 → 线索陈述 → 线索编织 → 基于线索+目标关系提问")
        print(f"   • 第三代: 完整链路整合 → 随机目标选择 → 线索编织 → 目标导向问题构建")
        
        print(f"\n📝 文件用途说明:")
        print(f"   • contextual_qa_dataset.jsonl: 基于随机目标选择的完整背景式问题")
        print(f"   • simple_qa_format.jsonl: 纯问答格式，适合直接测试")
        
        print(f"\n✨ 使用建议:")
        print(f"   • 多样性测试: 利用随机目标选择的多样性")
        print(f"   • 难度评估: 比较不同生成策略的问题难度")
        print(f"   • 系统优化: 可根据需要调整questions_per_path参数")
        
        return True

    def validate_generated_samples(self, samples):
        """
        验证生成的样本质量 - 更新验证逻辑以适应新的字段结构
        """
        valid_count = 0
        clue_not_contain_answer_count = 0
        target_diversity_count = 0
        
        for sample in samples:
            required_fields = ['integrated_clue_statement', 'contextual_narrative', 'final_question', 'answer']
            if all(field in sample and sample[field] and "Error" not in str(sample[field]) for field in required_fields):
                
                # 检查1：确保线索不包含答案
                clue_statement = sample['integrated_clue_statement'].lower()
                answer = sample['answer'].lower()
                clue_check_passed = answer not in clue_statement
                if clue_check_passed:
                    clue_not_contain_answer_count += 1
                
                # 检查2：确保目标实体的多样性（不总是路径末端）
                target_entity = sample.get('target_entity', '')
                original_path = sample.get('original_path', [])
                if original_path:
                    last_entity = original_path[-1].get('object', '') if original_path else ''
                    target_diversity_check = target_entity != last_entity
                    if target_diversity_check:
                        target_diversity_count += 1
                
                # 综合检查通过
                if clue_check_passed:
                    valid_count += 1
                else:
                    print(f"样本质量检查失败（线索包含答案）: {sample.get('target_entity', 'Unknown')}")
            else:
                print(f"样本质量检查失败（字段缺失）: {sample.get('target_entity', 'Unknown')}")
        
        print(f"\n=== 样本质量统计 ===")
        print(f"总体质量: {valid_count}/{len(samples)} ({(valid_count/len(samples)*100):.1f}%) 通过检查")
        print(f"线索纯净度: {clue_not_contain_answer_count}/{len(samples)} ({(clue_not_contain_answer_count/len(samples)*100):.1f}%) 线索不包含答案")
        print(f"目标多样性: {target_diversity_count}/{len(samples)} ({(target_diversity_count/len(samples)*100):.1f}%) 目标不是路径末端")
        
        return valid_count / len(samples) if samples else 0

    def run_new_strategy(self, graph, total_questions=50, batch_size=10):
        """
        🚀 便捷入口：运行全新的三阶段深度问题生成策略
        
        这是一个一键式的方法，会执行完整的三阶段流程并生成所有格式的数据集。
        
        Args:
            graph: 知识图谱对象
            total_questions: 目标生成的问题数量 (默认50)
            batch_size: 批次大小 (默认10)
            
        Returns:
            dict: 包含生成结果的完整报告
            
        Usage:
            generator = QuestionGenerator()
            result = generator.run_new_strategy(knowledge_graph, 100, 15)
        """
        print(f"\n" + "🔥"*25 + " 全新三阶段深度问题生成策略 " + "🔥"*25)
        print(f"📋 参数配置:")
        print(f"   • 目标问题数量: {total_questions}")
        print(f"   • 批次大小: {batch_size}")
        print(f"   • 策略: 多源线索聚合 → 深度模糊与故事编织 → 问题生成与校验")
        print(f"" + "="*80)
        
        try:
            # 执行完整的问答数据集生成流程
            result = self.generate_comprehensive_qa_new_strategy(graph, total_questions, batch_size)
            
            if result['success']:
                print(f"\n🎉 任务完成! 生成报告:")
                print(f"   ✅ 总样本数: {result['total_samples']}")
                print(f"   ✅ 验证通过数: {result['validated_count']}")
                print(f"   ✅ 验证通过率: {result['validation_rate']:.1f}%")
                print(f"   ✅ 简化格式问题: {result['simple_count']} 个")
                print(f"   ✅ 详细格式条目: {result['detailed_count']} 个")
                
                print(f"\n💡 使用建议:")
                if result['validation_rate'] < 60:
                    print(f"   ⚠️  验证通过率较低，建议调整图结构或增加批次大小")
                elif result['validation_rate'] > 80:
                    print(f"   🌟 验证通过率很高，问题质量优秀!")
                else:
                    print(f"   👍 验证通过率良好，建议继续优化")
                
                print(f"\n📂 输出文件位置:")
                output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
                print(f"   📄 verified_qa_new_strategy.jsonl - 验证通过的完整问题")
                print(f"   📄 simple_qa_new_strategy.jsonl - 纯问答格式")
                print(f"   📄 detailed_qa_new_strategy.jsonl - 详细调试信息")
                print(f"   📁 目录: {output_dir}")
            else:
                print(f"\n❌ 任务失败，请检查图结构或参数配置")
            
            return result
            
        except Exception as e:
            error_result = {
                'success': False,
                'error': str(e),
                'total_samples': 0,
                'validated_count': 0,
                'validation_rate': 0
            }
            print(f"\n❌ 执行过程中出现错误: {e}")
            return error_result
