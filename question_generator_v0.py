import json
import requests
import random
import os
import time
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
            if not response_str or response_str.startswith("API调用失败"):
                print(f"    ⚠️  LLM响应无效，返回空结果")
                return results
            
            # 清理响应内容
            response_str = response_str.strip()
            
            # 尝试多种解析模式
            patterns_to_try = [
                # 模式1: "编号: 内容" 格式 (如 "描述 1: xxx", "陈述 2: yyy")
                (rf'{prefix}\s*(\d+)\s*[:：]\s*(.+)', 2),
                # 模式2: 纯编号格式 (如 "1. xxx", "2. yyy")
                (r'(\d+)[\.\)]\s*(.+)', 2),
                # 模式3: 自定义分隔符格式
                (rf'(.+?){separator}(.+)' if separator else None, 2),
                # 模式4: 按行分割，不考虑编号
                (r'(.+)', 1)
            ]
            
            lines = response_str.split('\n')
            
            for pattern, group_count in patterns_to_try:
                if pattern is None:
                    continue
                    
                import re
                temp_results = []
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                        
                    match = re.search(pattern, line)
                    if match:
                        if group_count == 2:
                            content = match.group(2).strip()
                        else:
                            content = match.group(1).strip()
                        
                        if content and len(content) > 5:  # 过滤太短的内容
                            temp_results.append(content)
                
                # 如果这种模式找到了结果，使用它
                if temp_results:
                    results = temp_results
                    break
            
            # 如果仍然没有结果，尝试简单按行分割
            if not results:
                for line in lines:
                    line = line.strip()
                    if line and len(line) > 10:
                        results.append(line)
            
            # 限制结果数量
            if expected_count > 0:
                results = results[:expected_count]
            
            print(f"    ✅ 成功解析 {len(results)}/{expected_count} 个{prefix}")
            return results

        except Exception as e:
            print(f"    ❌ 解析LLM响应时出错: {e}")
            return results

    def generate_questions(self, graph, num_questions=50, strategy='combined', long_path_ratio=0.5, batch_size=10):
        """
        整合的问题生成主入口 - 支持链式推理和实体中心两种策略
        
        Args:
            graph: 知识图谱对象
            num_questions: 要生成的问题总数
            strategy: 'entity_centric', 'chain_of_thought', 或 'combined'
            long_path_ratio: 在 'combined' 策略下，长路径问题的比例
            batch_size: 批次大小
            
        Returns:
            dict: 包含生成结果的完整报告
        """
        print(f"\n" + "🔥"*25 + " 整合问题生成策略 " + "🔥"*25)
        print(f"📋 参数配置:")
        print(f"   • 目标问题数量: {num_questions}")
        print(f"   • 生成策略: {strategy}")
        print(f"   • 长路径比例: {long_path_ratio}")
        print(f"   • 批次大小: {batch_size}")
        print(f"" + "="*80)
        
        try:
            # 计算两种类型问题的数量
            num_long_path = 0
            if strategy in ['chain_of_thought', 'combined']:
                num_long_path = int(num_questions * long_path_ratio) if strategy == 'combined' else num_questions

            num_entity_centric = num_questions - num_long_path
            
            print(f"📊 问题分配:")
            print(f"   • 链式推理问题: {num_long_path} 个")
            print(f"   • 实体中心问题: {num_entity_centric} 个")
            
            all_samples = []
            
            # 1. 生成链式推理问题 (基于长路径)
            if num_long_path > 0:
                print(f"\n🔗 开始生成链式推理问题...")
                chain_samples = self._generate_chain_of_thought_questions(graph, num_long_path, batch_size)
                all_samples.extend(chain_samples)
                print(f"✅ 链式推理问题生成完成: {len(chain_samples)} 个")

            # 2. 生成实体中心问题 (复用现有逻辑)
            if num_entity_centric > 0:
                print(f"\n🏢 开始生成实体中心问题...")
                entity_samples = self._generate_entity_centric_questions(graph, num_entity_centric, batch_size)
                all_samples.extend(entity_samples)
                print(f"✅ 实体中心问题生成完成: {len(entity_samples)} 个")

            # 3. 生成最终数据集
            print(f"\n📊 生成综合数据集...")
            final_result = self._create_integrated_dataset(all_samples)
            
            print(f"\n🎉 整合问题生成完成!")
            print(f"   • 总样本数: {len(all_samples)}")
            print(f"   • 验证通过数: {final_result.get('validated_count', 0)}")
            print(f"   • 验证通过率: {final_result.get('validation_rate', 0):.1f}%")
            
            return final_result
            
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

    def _generate_chain_of_thought_questions(self, graph, num_questions, batch_size):
        """
        生成基于长路径的链式推理问题
        
        Args:
            graph: 知识图谱对象
            num_questions: 要生成的问题数量
            batch_size: 批次大小
            
        Returns:
            list: 生成的链式推理问题样本列表
        """
        print(f"🔗 开始链式推理问题生成，目标数量: {num_questions}")
        
        # 获取图中的所有节点
        all_nodes = list(graph.nodes())
        if len(all_nodes) < 10:
            print(f"❌ 图节点数量过少 ({len(all_nodes)})，无法生成链式推理问题")
            return []
        
        # 缓存图结构
        self._cache_graph_structure(graph)
        
        chain_samples = []
        attempts = 0
        max_attempts = num_questions * 15  # 允许更多尝试次数，因为长路径较难找到
        
        print(f"图中共有 {len(all_nodes)} 个节点，开始搜索长路径...")
        
        while len(chain_samples) < num_questions and attempts < max_attempts:
            attempts += 1
            
            # 1. 随机选择起始节点
            start_node = random.choice(all_nodes)
            
            # 2. 寻找从该节点出发的长路径
            long_paths = self._find_long_paths(start_node, min_length=4, max_paths=3, timeout=10)
            
            if not long_paths:
                continue
            
            # 3. 为每条长路径生成问题
            for path in long_paths:
                if len(chain_samples) >= num_questions:
                    break
                    
                question_sample = self._create_chain_question_sample(path, graph)
                if question_sample:
                    chain_samples.append(question_sample)
                    
            if len(chain_samples) % 10 == 0 and len(chain_samples) > 0:
                print(f"  已生成 {len(chain_samples)}/{num_questions} 个链式推理问题")
        
        print(f"链式推理问题生成完成：成功生成 {len(chain_samples)} 个样本 (尝试 {attempts} 次)")
        
        # 分批处理链式推理问题的后续步骤
        if chain_samples:
            self._process_chain_samples_in_batches(chain_samples, batch_size)
        
        return chain_samples

    def _find_long_paths(self, start_node, min_length=4, max_paths=5, timeout=10):
        """
        寻找从某个实体出发，路径长度大于等于min_length的长路径。
        采用"带深度限制的随机游走"策略避免路径爆炸和性能问题。

        Args:
            start_node: 起始实体
            min_length: 路径的最小长度 (节点数量，所以需要大于3才能有3跳)
            max_paths: 最多寻找多少条路径
            timeout: 搜索超时时间（秒）
            
        Returns:
            list: 包含多条长路径的列表，每条路径格式为 [node1, node2, node3, ...]
        """
        all_paths = []
        start_time = time.time()
        attempts = 0
        max_attempts = max_paths * 20  # 限制尝试次数
        
        while len(all_paths) < max_paths and time.time() - start_time < timeout and attempts < max_attempts:
            attempts += 1
            path = self._perform_random_walk(start_node, min_length)
            
            if path and len(path) >= min_length:
                # 检查路径是否与已有路径足够不同
                if self._is_path_unique(path, all_paths):
                    all_paths.append(path)
        
        return all_paths

    def _perform_random_walk(self, start_node, min_length, max_length=8):
        """
        从指定节点开始执行随机游走，生成长路径
        
        Args:
            start_node: 起始节点
            min_length: 最小路径长度
            max_length: 最大路径长度
            
        Returns:
            list: 生成的路径节点列表
        """
        path = [start_node]
        current_node = start_node
        visited_recently = set([start_node])  # 防止短期内回头
        
        for step in range(max_length - 1):
            # 获取当前节点的邻居
            neighbors = self.graph_cache.get(current_node, [])
            
            # 过滤掉最近访问的节点
            available_neighbors = [n for n in neighbors if n not in visited_recently]
            
            if not available_neighbors:
                # 如果没有可用邻居，试试包含最近访问的节点
                available_neighbors = [n for n in neighbors if n not in path[-2:]] if len(path) >= 2 else neighbors
            
            if not available_neighbors:
                break
            
            # 随机选择下一个节点
            next_node = random.choice(available_neighbors)
            path.append(next_node)
            current_node = next_node
            
            # 更新最近访问记录
            visited_recently.add(next_node)
            if len(visited_recently) > 3:  # 只保留最近3个节点的记录
                visited_recently.pop()
        
        return path if len(path) >= min_length else None

    def _is_path_unique(self, new_path, existing_paths, similarity_threshold=0.7):
        """
        检查新路径是否与已有路径足够不同
        
        Args:
            new_path: 新生成的路径
            existing_paths: 已有的路径列表
            similarity_threshold: 相似度阈值
            
        Returns:
            bool: 如果路径足够独特返回True
        """
        if not existing_paths:
            return True
        
        new_path_set = set(new_path)
        
        for existing_path in existing_paths:
            existing_path_set = set(existing_path)
            
            # 计算节点重叠度
            intersection = new_path_set.intersection(existing_path_set)
            union = new_path_set.union(existing_path_set)
            
            if len(union) == 0:
                continue
                
            similarity = len(intersection) / len(union)
            
            if similarity > similarity_threshold:
                return False
        
        return True

    def _create_chain_question_sample(self, path, graph):
        """
        根据长路径创建链式推理问题样本
        
        Args:
            path: 长路径节点列表
            graph: 知识图谱对象
            
        Returns:
            dict: 问题样本
        """
        if len(path) < 4:
            return None
        
        # 生成问题和答案
        question = self._generate_question_from_long_path(path)
        
        if not question:
            return None
        
        # 构建样本
        sample_id = f"chain_{hash(tuple(path))}_{len(path)}"
        
        sample = {
            'sample_id': sample_id,
            'strategy': 'chain_of_thought',
            'question_type': 'multi_hop_reasoning',
            'path': path,
            'path_length': len(path),
            'start_entity': path[0],
            'end_entity': path[-1],
            'question': question,
            'answer': path[-1],  # 答案是路径的终点
            'intermediate_entities': path[1:-1],
            'reasoning_steps': self._extract_reasoning_steps(path, graph),
            'generation_timestamp': datetime.now().isoformat(),
            'status': 'generated'
        }
        
        return sample

    def _generate_question_from_long_path(self, path):
        """
        根据一条长路径，生成一个需要链式推理的问题（带背景故事）。

        Args:
            path: 一条长路径，格式为 [node1, node2, node3, ...]
            
        Returns:
            str: 生成的完整问题字符串（包含背景和问题）
        """
        if len(path) < 4:
            return None
        
        start_entity = path[0]
        end_entity = path[-1]
        intermediate_entities = path[1:-1]
        
        # 为链式路径生成背景故事
        background_story = self._generate_chain_background_story(path)
        
        # 生成与背景故事衔接的问题
        question_part = self._generate_chain_question_part(start_entity, intermediate_entities)
        
        # 组合完整问题（参考目标格式）
        full_question = f"**问题：**  \n{background_story}{question_part}  \n\n**解析：**  \n1. 根据路径关系，{start_entity}通过{len(intermediate_entities)}步关联。  \n2. 关键中间实体包括{intermediate_entities[0] if intermediate_entities else '相关实体'}等。  \n3. 需要整合路径中的所有关系信息来推理最终答案。"
        
        return full_question

    def _generate_chain_background_story(self, path):
        """
        为链式路径生成背景故事
        """
        start_entity = path[0]
        end_entity = path[-1]
        intermediate_entities = path[1:-1]
        
        # 调用LLM生成背景故事
        prompt = f"""你是一位问题编织专家。请根据下面的实体路径，创建一个问题背景故事，正常叙述，避免过于文学化。

要求：
1. **实体泛化**：将具体的实体名称替换为更通用的描述（例如，"张三" -> "一位作家"；"北京大学" -> "一所知名学府"）
2. **关系隐藏**：不要过于直接地说明实体间的关系，可以通过更换表述等方式表达，但是需要确保逻辑清晰以及关系正确
3. **禁止泄露答案**：绝对不能在问题背景故事中出现最终答案实体：{end_entity}
4. **保持逻辑**：问题背景故事必须保留路径中的核心逻辑关系，确保可以推理出答案

实体路径：{' -> '.join(path)}
最终答案（不可在故事中出现）：{end_entity}

请生成一个简洁的背景故事（100字以内）："""

        result = self._batch_llm_call(prompt)
        
        if result and not result.startswith("API调用失败"):
            return result.strip()
        else:
            # 回退：生成简单的背景故事
            return f"在某个研究领域，一个重要的起点与多个相关概念存在连接。这些概念之间形成了一个复杂的关联网络。"

    def _generate_chain_question_part(self, start_entity, intermediate_entities):
        """
        生成链式推理问题的提问部分
        """
        # 对起始实体进行泛化
        generalized_start = self._generalize_entity(start_entity)
        
        templates = [
            f"这个网络最终指向的核心概念是什么？",
            f"通过这些关联，最终会到达哪个目标？",
            f"这个关联链的终点是什么？",
            f"根据这些线索，最终的答案是什么？"
        ]
        
        return random.choice(templates)

    def _generalize_entity(self, entity):
        """
        对实体进行泛化处理
        """
        # 简单的实体泛化逻辑
        if any(char in entity for char in ['大学', '学院', '学校']):
            return "一所教育机构"
        elif any(char in entity for char in ['公司', '集团', '企业']):
            return "一个组织"
        elif any(char in entity for char in ['书', '著作', '作品']):
            return "一部作品"
        elif any(char in entity for char in ['人', '家', '者']):
            return "一位人物"
        else:
            return "一个相关概念"

    def _extract_reasoning_steps(self, path, graph):
        """
        从路径中提取推理步骤
        
        Args:
            path: 路径节点列表
            graph: 知识图谱对象
            
        Returns:
            list: 推理步骤列表
        """
        steps = []
        
        for i in range(len(path) - 1):
            current_node = path[i]
            next_node = path[i + 1]
            
            # 尝试获取真实的关系
            relation = '相关于'  # 默认关系
            if hasattr(graph, 'get_edge_data'):
                edge_data = graph.get_edge_data(current_node, next_node)
                if edge_data:
                    if isinstance(edge_data, dict) and edge_data:
                        relation = list(edge_data.keys())[0]
            
            step = {
                'step': i + 1,
                'from_entity': current_node,
                'relation': relation,
                'to_entity': next_node,
                'description': f"{current_node} {relation} {next_node}"
            }
            steps.append(step)
        
        return steps

    def _generate_entity_centric_questions(self, graph, num_questions, batch_size):
        """
        生成基于实体中心的问题（复用原有的多源线索聚合逻辑）
        
        Args:
            graph: 知识图谱对象
            num_questions: 要生成的问题数量
            batch_size: 批次大小
            
        Returns:
            list: 生成的实体中心问题样本列表
        """
        print(f"🏢 开始实体中心问题生成，目标数量: {num_questions}")
        
        # 使用原有的多源线索聚合逻辑
        entity_samples = self._multi_source_clue_aggregation(graph, num_questions)
        
        if not entity_samples:
            print("❌ 实体中心问题生成失败")
            return []
        
        print(f"✅ 实体中心线索聚合完成，生成了 {len(entity_samples)} 个答案-线索组合")
        
        # 分批处理实体中心问题
        if entity_samples:
            self._process_entity_samples_in_batches(entity_samples, batch_size)
        
        return entity_samples

    def _process_chain_samples_in_batches(self, chain_samples, batch_size):
        """
        分批处理链式推理问题样本
        """
        total_batches = (len(chain_samples) + batch_size - 1) // batch_size
        print(f"\n分批处理链式推理问题：{len(chain_samples)} 个样本分为 {total_batches} 批次")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(chain_samples))
            batch_samples = chain_samples[start_idx:end_idx]
            
            print(f"\n=== 处理链式推理批次 {batch_idx + 1}/{total_batches} ===")
            
            # 对链式推理问题进行验证和优化
            self._validate_chain_questions_batch(batch_samples)
            self._save_stage_results(batch_samples, "chain_reasoning_validation", batch_idx + 1)

    def _process_entity_samples_in_batches(self, entity_samples, batch_size):
        """
        分批处理实体中心问题样本
        """
        total_batches = (len(entity_samples) + batch_size - 1) // batch_size
        print(f"\n分批处理实体中心问题：{len(entity_samples)} 个样本分为 {total_batches} 批次")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(entity_samples))
            batch_samples = entity_samples[start_idx:end_idx]
            
            print(f"\n=== 处理实体中心批次 {batch_idx + 1}/{total_batches} ===")
            
            # 深度模糊与故事编织
            self._deep_obfuscation_and_narrative_weaving_batch(batch_samples)
            self._save_stage_results(batch_samples, "entity_obfuscation", batch_idx + 1)
            
            # 问题生成与校验
            self._question_generation_and_validation_batch(batch_samples)
            self._save_stage_results(batch_samples, "entity_validation", batch_idx + 1)

    def _validate_chain_questions_batch(self, batch_samples):
        """
        验证链式推理问题的逻辑性和可答性
        """
        print(f"验证 {len(batch_samples)} 个链式推理问题...")
        
        validated_count = 0
        for sample in batch_samples:
            # 基本检查
            if (sample.get('question') and 
                sample.get('answer') and 
                sample.get('path_length', 0) >= 4):
                
                # 检查问题是否包含答案
                question_lower = sample['question'].lower()
                answer_lower = str(sample['answer']).lower()
                
                if answer_lower not in question_lower:
                    sample['validation_status'] = '通过'
                    sample['final_status'] = 'validated'
                    validated_count += 1
                else:
                    sample['validation_status'] = '失败：问题包含答案'
                    sample['final_status'] = 'failed'
            else:
                sample['validation_status'] = '失败：缺少必要字段'
                sample['final_status'] = 'failed'
        
        print(f"链式推理问题验证完成：{validated_count}/{len(batch_samples)} 通过")

    def _create_integrated_dataset(self, all_samples):
        """
        创建整合的问答数据集
        
        Args:
            all_samples: 所有生成的样本列表
            
        Returns:
            dict: 包含统计信息的结果字典
        """
        print(f"📊 开始创建整合数据集...")
        
        # 分别统计两种类型的问题
        chain_samples = [s for s in all_samples if s.get('strategy') == 'chain_of_thought']
        entity_samples = [s for s in all_samples if s.get('strategy') == 'multi_source_clue_aggregation']
        
        # 验证通过的问题
        validated_samples = [s for s in all_samples if s.get('final_status') == 'validated']
        
        print(f"📈 数据统计:")
        print(f"   • 链式推理问题: {len(chain_samples)} 个")
        print(f"   • 实体中心问题: {len(entity_samples)} 个")
        print(f"   • 总样本数: {len(all_samples)} 个")
        print(f"   • 验证通过数: {len(validated_samples)} 个")
        
        # 生成数据集文件
        self._save_integrated_datasets(all_samples, validated_samples, chain_samples, entity_samples)
        
        validation_rate = len(validated_samples) / len(all_samples) * 100 if all_samples else 0
        
        result = {
            'success': True,
            'total_samples': len(all_samples),
            'chain_samples': len(chain_samples),
            'entity_samples': len(entity_samples),
            'validated_samples': validated_samples,
            'validated_count': len(validated_samples),
            'validation_rate': validation_rate,
            'simple_count': len(validated_samples)  # 简化格式问题数等于验证通过数
        }
        
        return result

    def _save_integrated_datasets(self, all_samples, validated_samples, chain_samples, entity_samples):
        """
        保存整合的数据集文件
        """
        output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. 保存完整的整合数据集
        full_output_path = os.path.join(output_dir, "integrated_qa_dataset.jsonl")
        with open(full_output_path, 'w', encoding='utf-8') as f:
            for sample in all_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        print(f"完整整合数据集已保存: {full_output_path}")
        
        # 2. 保存验证通过的问题
        validated_output_path = os.path.join(output_dir, "validated_integrated_qa.jsonl")
        with open(validated_output_path, 'w', encoding='utf-8') as f:
            for sample in validated_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        print(f"验证通过的问题已保存: {validated_output_path}")
        
        # 3. 保存简化格式
        simple_output_path = os.path.join(output_dir, "simple_integrated_qa.jsonl")
        with open(simple_output_path, 'w', encoding='utf-8') as f:
            for sample in validated_samples:
                simple_qa = {
                    'question': sample.get('question', sample.get('generated_question', '')),
                    'answer': sample.get('answer', sample.get('answer_entity', '')),
                    'strategy': sample.get('strategy', 'unknown'),
                    'question_type': sample.get('question_type', 'unknown')
                }
                f.write(json.dumps(simple_qa, ensure_ascii=False) + '\n')
        print(f"简化格式问答已保存: {simple_output_path}")
        
        # 4. 保存策略分类数据集
        chain_output_path = os.path.join(output_dir, "chain_reasoning_qa.jsonl")
        with open(chain_output_path, 'w', encoding='utf-8') as f:
            for sample in chain_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        print(f"链式推理问题已保存: {chain_output_path}")
        
        entity_output_path = os.path.join(output_dir, "entity_centric_qa.jsonl")
        with open(entity_output_path, 'w', encoding='utf-8') as f:
            for sample in entity_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        print(f"实体中心问题已保存: {entity_output_path}")

    def run_new_strategy(self, graph, total_questions=50, batch_size=10):
        """
        🚀 便捷入口：运行整合的问题生成策略 (链式推理 + 实体中心 = 1:1)
        
        Args:
            graph: 知识图谱对象
            total_questions: 目标生成的问题数量 (默认50)
            batch_size: 批次大小 (默认10)
            
        Returns:
            dict: 包含生成结果的完整报告
        """
        return self.generate_questions(
            graph=graph, 
            num_questions=total_questions, 
            strategy='combined', 
            long_path_ratio=0.5,  # 1:1 比例
            batch_size=batch_size
        )

    # 简化的必要支持方法
    def _cache_graph_structure(self, graph):
        """
        缓存图结构以便后续使用
        """
        if not hasattr(self, 'graph_cache'):
            self.graph_cache = {}
        
        # 缓存每个节点的邻居信息
        for node in graph.nodes():
            self.graph_cache[node] = list(graph.successors(node))

    def _multi_source_clue_aggregation(self, graph, target_count):
        """
        简化的多源线索聚合方法（保留核心逻辑）
        """
        print(f"开始实体中心线索聚合，目标数量：{target_count}")
        
        all_nodes = list(graph.nodes())
        if len(all_nodes) < 10:
            print(f"❌ 图节点数量过少 ({len(all_nodes)})，无法进行有效的线索聚合")
            return []
        
        # 为了与图对象交互，需要缓存图的邻接信息
        self._cache_graph_structure(graph)
        
        aggregated_samples = []
        attempts = 0
        max_attempts = target_count * 10
        
        while len(aggregated_samples) < target_count and attempts < max_attempts:
            attempts += 1
            
            # 随机选择目标实体作为答案
            answer_entity = random.choice(all_nodes)
            
            # 为这个答案实体寻找多条独立的推理路径
            independent_paths = self._discover_independent_clue_paths(graph, answer_entity)
            
            if len(independent_paths) < 2:
                continue
            
            # 构建线索包
            clue_package = self._build_clue_package(answer_entity, independent_paths, graph)
            
            if clue_package:
                aggregated_samples.append(clue_package)
                if len(aggregated_samples) % 10 == 0:
                    print(f"  已聚合 {len(aggregated_samples)}/{target_count} 个样本")
        
        print(f"实体中心线索聚合完成：成功生成 {len(aggregated_samples)} 个样本")
        return aggregated_samples

    def _discover_independent_clue_paths(self, graph, answer_entity, max_paths=4, max_path_length=3):
        """简化的独立线索路径发现"""
        independent_paths = []
        used_entities = set([answer_entity])
        
        predecessors = list(graph.predecessors(answer_entity))
        
        if len(predecessors) < 2:
            successors = list(graph.successors(answer_entity))
            for successor in successors[:max_paths]:
                if successor not in used_entities:
                    path = [answer_entity, successor]
                    independent_paths.append(path)
                    used_entities.add(successor)
                    if len(independent_paths) >= max_paths:
                        break
            return independent_paths
        
        random.shuffle(predecessors)
        
        for pred_node in predecessors:
            if pred_node in used_entities:
                continue
                
            clue_path = [pred_node, answer_entity]
            
            if clue_path and len(clue_path) >= 2:
                path_entities = set(clue_path[:-1])
                if not path_entities.intersection(used_entities):
                    independent_paths.append(clue_path)
                    used_entities.update(path_entities)
                    
                    if len(independent_paths) >= max_paths:
                        break
        
        return independent_paths

    def _build_clue_package(self, answer_entity, independent_paths, graph=None):
        """简化的线索包构建，增加关系信息提取"""
        if not independent_paths:
            return None
        
        clue_entities = []
        clue_paths_info = []
        relation_facts = []  # 新增：关系事实列表
        
        for i, path in enumerate(independent_paths):
            if len(path) >= 2:
                # 提取路径中的关系信息
                path_relations = self._extract_path_relations(path, graph)
                
                clue_paths_info.append({
                    'path_id': i + 1,
                    'path_nodes': path,
                    'path_relations': path_relations,  # 新增：路径关系
                    'clue_entity': path[-1] if path[-1] != answer_entity else path[-2]
                })
                
                # 收集关系事实
                relation_facts.extend(path_relations)
                
                if path[-1] != answer_entity:
                    clue_entities.append(path[-1])
                elif len(path) >= 2:
                    clue_entities.append(path[-2])
        
        if not clue_entities:
            return None
        
        sample_id = f"entity_clue_{hash(answer_entity)}_{len(independent_paths)}"
        
        clue_package = {
            'sample_id': sample_id,
            'strategy': 'multi_source_clue_aggregation',
            'answer_entity': answer_entity,
            'independent_paths': independent_paths,
            'clue_entities': clue_entities,
            'clue_paths_info': clue_paths_info,
            'relation_facts': relation_facts,  # 新增：关系事实
            'num_clue_paths': len(independent_paths),
            'generation_timestamp': datetime.now().isoformat(),
            'status': 'aggregated'
        }
        
        return clue_package

    def _extract_path_relations(self, path, graph):
        """
        提取路径中每一步的关系信息
        
        Args:
            path: 节点路径列表
            graph: 知识图谱对象
            
        Returns:
            list: 关系事实列表，格式为 [{'subject': ..., 'relation': ..., 'object': ...}, ...]
        """
        relations = []
        
        if not graph or len(path) < 2:
            return relations
        
        for i in range(len(path) - 1):
            subj = path[i]
            obj = path[i + 1]
            
            # 获取真实的关系
            relation = self._get_relation_between_entities(subj, obj, graph)
            
            if relation:
                relations.append({
                    'subject': subj,
                    'relation': relation,
                    'object': obj,
                    'step': i + 1
                })
        
        return relations
    
    def _get_relation_between_entities(self, entity1, entity2, graph):
        """
        获取两个实体间的关系
        
        Args:
            entity1: 第一个实体
            entity2: 第二个实体
            graph: 知识图谱对象
            
        Returns:
            str: 关系名称，如果没有找到则返回默认关系
        """
        if not graph or not hasattr(graph, 'get_edge_data'):
            return '相关于'
        
        # 尝试获取边数据
        edge_data = graph.get_edge_data(entity1, entity2)
        
        if edge_data:
            if isinstance(edge_data, dict):
                if edge_data:
                    # 对于MultiDiGraph，可能有多个关系，取第一个
                    first_relation = list(edge_data.keys())[0]
                    if isinstance(edge_data[first_relation], dict):
                        return edge_data[first_relation].get('relation', first_relation)
                    else:
                        return first_relation
        
        # 尝试反向关系
        edge_data = graph.get_edge_data(entity2, entity1)
        if edge_data:
            if isinstance(edge_data, dict) and edge_data:
                first_relation = list(edge_data.keys())[0]
                if isinstance(edge_data[first_relation], dict):
                    relation = edge_data[first_relation].get('relation', first_relation)
                    return f"被{relation}"  # 反向关系
                else:
                    return f"被{first_relation}"
        
        return '相关于'  # 默认关系

    def _deep_obfuscation_and_narrative_weaving_batch(self, batch_samples):
        """
        第二阶段：深度模糊与故事编织（基于v4版本的逻辑）
        
        这个阶段将：
        1. 对实体进行泛化处理，隐藏具体的名称
        2. 将精确的事实陈述转化为模糊但有趣的叙述
        3. 创建相对时间和逻辑连接
        4. 确保不泄露目标答案
        """
        print(f"  开始深度模糊与故事编织...")
        
        # 构建关系事实陈述
        self._build_integrated_clue_statements(batch_samples)
        
        # 执行情境模糊与重构
        self._process_obfuscation_and_weaving_batch(batch_samples)
        
        print(f"  深度模糊与故事编织完成")

    def _build_integrated_clue_statements(self, batch_samples):
        """
        为每个样本构建整合的线索陈述
        """
        print(f"    构建关系事实陈述...")
        
        for sample in batch_samples:
            relation_facts = sample.get('relation_facts', [])
            answer_entity = sample.get('answer_entity', '')
            
            if not relation_facts:
                sample['integrated_clue_statement'] = "相关实体间存在某种关联"
                continue
            
            # 构建事实陈述
            fact_statements = []
            for fact in relation_facts:
                statement = f"{fact['subject']} {fact['relation']} {fact['object']}"
                fact_statements.append(statement)
            
            # 整合所有事实
            integrated_statement = "。".join(fact_statements) + "。"
            sample['integrated_clue_statement'] = integrated_statement
            sample['target_entity'] = answer_entity

    def _process_obfuscation_and_weaving_batch(self, batch_samples):
        """
        情境模糊与重构 - 将精确的事实陈述转化为模糊但有趣的叙述（基于v4版本）
        """
        prompt = """你是一位顶级的出题专家和问题背景故事编织者。你的任务是将以下【原始事实陈述】转化为一段逻辑清晰的【问题背景故事】。

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
   - 在生成的【问题背景故事】中，绝对不能出现【目标实体】（即问题的答案）。

现在，请处理以下内容：
"""
        
        valid_samples = []
        for i, sample in enumerate(batch_samples):
            if 'integrated_clue_statement' in sample and not sample['integrated_clue_statement'].startswith("Error"):
                prompt += f"\n原始事实陈述 {len(valid_samples)+1}: {sample['integrated_clue_statement']}"
                prompt += f"\n目标实体 {len(valid_samples)+1}: {sample.get('target_entity', '')}\n"
                valid_samples.append(sample)
        
        if not valid_samples:
            print("    没有有效的事实陈述可供模糊化处理")
            for sample in batch_samples:
                sample['narrative_story'] = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
            return

        prompt += "\n\n请严格按照以下格式返回，每个问题背景故事占一行:\n问题背景故事 1: [转化后的模糊化故事]\n问题背景故事 2: [转化后的模糊化故事]\n..."
        
        results_str = self._batch_llm_call(prompt)
        if results_str and not results_str.startswith("API调用失败"):
            parsed_stories = self._parse_llm_response(results_str, len(valid_samples), "问题背景故事")
            success_count = 0
            for i, sample in enumerate(valid_samples):
                if i < len(parsed_stories) and not parsed_stories[i].startswith("Error"):
                    sample['narrative_story'] = parsed_stories[i]
                    sample['status'] = 'story_woven_with_relations'
                    
                    # 同时保存实体泛化信息
                    sample['obfuscated_entities'] = self._extract_entity_obfuscation(
                        sample['integrated_clue_statement'], 
                        parsed_stories[i], 
                        sample.get('clue_entities', [])
                    )
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
                    sample['narrative_story'] = simplified_story
                    sample['status'] = 'fallback_story'
            
            # 处理无效样本
            for sample in batch_samples:
                if sample not in valid_samples:
                    sample['narrative_story'] = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
                    sample['status'] = 'invalid_input'
                    
            print(f"    模糊化处理完成：{success_count}/{len(batch_samples)} 个样本")
        else:
            print(f"    模糊化API调用失败: {results_str}")
            # 回退处理：简单泛化
            for sample in batch_samples:
                orig_statement = sample.get('integrated_clue_statement', '相关实体间存在某种关联')
                target_entity = sample.get('target_entity', '')
                # 简单泛化处理：替换目标实体为"某个实体"
                if target_entity and target_entity in orig_statement:
                    simplified_story = orig_statement.replace(target_entity, "某个相关实体")
                else:
                    simplified_story = orig_statement
                sample['narrative_story'] = simplified_story
                sample['status'] = 'api_failed'

    def _extract_entity_obfuscation(self, original_statement, obfuscated_story, clue_entities):
        """
        从原始陈述和模糊化故事中提取实体泛化映射
        """
        obfuscated_entities = {}
        
        for entity in clue_entities:
            if entity in original_statement and entity not in obfuscated_story:
                # 实体被成功泛化，尝试找到对应的泛化描述
                generic_desc = self._generalize_entity(entity)
                obfuscated_entities[entity] = generic_desc
        
        return obfuscated_entities
    
    def _atomic_obfuscation_batch(self, batch_samples):
        """
        原子化模糊：对每个线索实体进行独立的、上下文无关的模糊化处理
        增强版本：包含关系上下文信息，确保不泄露实体名称
        """
        print(f"    执行原子化模糊处理...")
        
        # 收集所有需要模糊化的实体及其关系上下文
        entities_with_context = []
        entity_to_samples = {}  # 实体到样本的映射
        
        for sample in batch_samples:
            clue_entities = sample.get('clue_entities', [])
            relation_facts = sample.get('relation_facts', [])
            
            for entity in clue_entities:
                if entity not in entity_to_samples:
                    entity_to_samples[entity] = []
                    
                    # 为实体收集关系上下文
                    entity_context = self._extract_entity_context(entity, relation_facts)
                    entities_with_context.append({
                        'entity': entity,
                        'context': entity_context
                    })
                
                entity_to_samples[entity].append(sample)
        
        if not entities_with_context:
            print(f"    ⚠️  没有找到需要模糊化的实体")
            return
        
        # 批量对实体进行模糊化（基于v4版本的严格要求）
        prompt = """你是一位信息抽象专家。请为以下每个实体生成一个模糊但具有唯一指向性的描述。

严格要求：
1. **绝对禁止泄露**：描述中不能出现该实体的具体名称或任何直接标识符
2. **唯一指向性**：描述要足够具体，能唯一指向该实体，但又要保持模糊
3. **关系导向**：充分利用提供的关系上下文信息，确保描述准确且有区分度
4. **描述性语言**：使用通用的、描述性的语言，避免专有名词
5. **长度控制**：每个描述控制在15字以内

示例参考：
- 陈晓卿 -> 一位著名的美食纪录片导演
- 中国传媒大学 -> 一所以信息传播闻名的高等学府
- 李立宏 -> 一位声音浑厚的国家级配音演员

现在请处理以下实体（包含其关系上下文）：
"""
        
        for i, entity_info in enumerate(entities_with_context):
            entity = entity_info['entity']
            context = entity_info['context']
            
            prompt += f"\n实体 {i + 1}: {entity}\n"
            if context:
                prompt += f"   关系上下文: {'; '.join(context)}\n"
            else:
                prompt += f"   关系上下文: 无直接关系信息\n"
        
        prompt += "\n\n请严格按照以下格式返回，每个描述占一行:\n描述 1: [基于上下文的模糊化描述]\n描述 2: [基于上下文的模糊化描述]\n..."
        
        results_str = self._batch_llm_call(prompt)
        
        # 解析结果并分配给样本
        obfuscated_descriptions = {}
        
        if results_str and not results_str.startswith("API调用失败"):
            parsed_descriptions = self._parse_llm_response(results_str, len(entities_with_context), "描述")
            
            for i, description in enumerate(parsed_descriptions):
                if i < len(entities_with_context):
                    entity = entities_with_context[i]['entity']
                    # 严格检查是否泄露实体名称
                    if not self._contains_entity_leak(description, entity):
                        obfuscated_descriptions[entity] = description
                    else:
                        # 如果检测到泄露，使用更安全的通用描述
                        obfuscated_descriptions[entity] = self._get_generic_entity_desc(entity)
        else:
            print(f"    ❌ LLM调用失败，使用安全的通用描述")
            for entity_info in entities_with_context:
                entity = entity_info['entity']
                obfuscated_descriptions[entity] = self._get_generic_entity_desc(entity)
        
        # 将模糊化结果分配给各个样本
        for sample in batch_samples:
            sample_obfuscated = {}
            clue_entities = sample.get('clue_entities', [])
            
            for entity in clue_entities:
                if entity in obfuscated_descriptions:
                    sample_obfuscated[entity] = obfuscated_descriptions[entity]
            
            sample['obfuscated_entities'] = sample_obfuscated
        
        success_count = len([s for s in batch_samples if s.get('obfuscated_entities')])
        print(f"    原子化模糊完成：{success_count}/{len(batch_samples)} 个样本")

    def _extract_entity_context(self, entity, relation_facts):
        """
        为实体提取关系上下文信息
        
        Args:
            entity: 目标实体
            relation_facts: 关系事实列表
            
        Returns:
            list: 与该实体相关的关系描述
        """
        context = []
        
        for fact in relation_facts:
            if fact['subject'] == entity:
                context.append(f"{fact['relation']}{fact['object']}")
            elif fact['object'] == entity:
                context.append(f"被{fact['subject']}{fact['relation']}")
        
        return context[:3]  # 最多返回3个关系上下文
    
    def _contains_entity_leak(self, description, entity):
        """
        检查描述是否泄露了实体的真实名称
        
        Args:
            description: 模糊化描述
            entity: 原始实体名称
            
        Returns:
            bool: 如果检测到泄露返回True
        """
        if not description or not entity:
            return False
        
        description_lower = description.lower()
        entity_lower = str(entity).lower()
        
        # 检查是否包含完整的实体名称
        if entity_lower in description_lower:
            return True
        
        # 检查是否包含实体名称的主要部分（超过2个字符）
        if len(entity) > 2:
            for i in range(len(entity) - 2):
                substring = entity[i:i+3].lower()
                if substring in description_lower:
                    return True
        
        return False
    
    def _get_generic_entity_desc(self, entity):
        """
        为实体生成更自然的通用描述，避免"某实体"这样机械的表述
        
        Args:
            entity: 实体名称
            
        Returns:
            str: 自然的通用描述
        """
        # 简单的实体类型判断规则
        if any(keyword in str(entity) for keyword in ['公司', '集团', '有限', '股份', '科技', '工业', '商业']):
            return "一家知名企业"
        elif any(keyword in str(entity) for keyword in ['大学', '学院', '学校', '教育']):
            return "一所教育机构"
        elif any(keyword in str(entity) for keyword in ['医院', '诊所', '卫生']):
            return "一个医疗机构"
        elif any(keyword in str(entity) for keyword in ['银行', '保险', '证券', '基金', '金融']):
            return "一个金融机构"
        elif any(keyword in str(entity) for keyword in ['电影', '电视剧', '纪录片', '动画']):
            return "一部影视作品"
        elif any(keyword in str(entity) for keyword in ['歌曲', '专辑', '音乐']):
            return "一首音乐作品"
        elif any(keyword in str(entity) for keyword in ['小说', '散文', '诗歌', '著作', '作品']):
            return "一部文学作品"
        elif len(str(entity)) <= 4 and not any(char.isdigit() for char in str(entity)):
            # 短字符串且无数字，可能是人名
            return "一位人物"
        elif any(keyword in str(entity) for keyword in ['市', '省', '县', '区', '街道', '路']):
            return "一个地理位置"
        elif any(keyword in str(entity) for keyword in ['年', '月', '日', '时期', '朝代']):
            return "一个时间点"
        elif str(entity).isdigit() or any(char.isdigit() for char in str(entity)):
            return "一个数值"
        else:
            # 默认情况，尝试根据字符特征判断
            if len(str(entity)) <= 6:
                return "相关对象"
            else:
                return "相关内容"
    
    def _narrative_weaving_batch(self, batch_samples):
        """
        故事编织：将模糊化的描述编织成连贯的悬疑故事
        """
        print(f"    执行故事编织...")
        
        valid_samples = []
        for sample in batch_samples:
            if sample.get('obfuscated_entities'):
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    ⚠️  没有可用于故事编织的样本")
            return
        
        # 为每个样本单独编织故事
        for sample in valid_samples:
            self._weave_single_narrative(sample)
        
        success_count = len([s for s in valid_samples if s.get('narrative_story')])
        print(f"    故事编织完成：{success_count}/{len(valid_samples)} 个样本")
    
    def _weave_single_narrative(self, sample):
        """
        为单个样本编织故事（增强版本：基于真实关系路径）
        """
        obfuscated_entities = sample.get('obfuscated_entities', {})
        answer_entity = sample.get('answer_entity', '')
        relation_facts = sample.get('relation_facts', [])
        
        if not obfuscated_entities:
            return
        
        # 构建基于关系路径的故事编织prompt
        prompt = f"""你是一位逻辑严谨的谜题构建专家。你的任务是根据下面提供的【模糊实体描述】和【实体间的真实关系】，将它们整合成一段客观的信息陈述。

请遵循以下核心准则：
1. **基于事实关系**：严格按照提供的真实关系来描述实体间的联系，不要杜撰关系。
2. **客观整合**：将模糊描述和关系信息客观地整合，专注于事实呈现。
3. **逻辑连贯**：确保描述符合提供的关系路径，逻辑连贯。
4. **避免文学化**：保持中立、简洁、直接的语言风格。
5. **完整性**：确保所有模糊描述和关系都体现在最终陈述中。

---
【模糊实体描述】：
"""
        
        descriptions = list(obfuscated_entities.values())
        for i, desc in enumerate(descriptions):
            prompt += f"{i+1}. {desc}\n"
        
        # 添加真实关系信息
        prompt += f"\n【实体间的真实关系】：\n"
        if relation_facts:
            for i, fact in enumerate(relation_facts):
                # 使用模糊描述而不是真实实体名，如果没有模糊描述则使用智能化的通用表述
                subj_desc = obfuscated_entities.get(fact['subject'], self._get_generic_entity_desc(fact['subject']))
                obj_desc = obfuscated_entities.get(fact['object'], self._get_generic_entity_desc(fact['object']))
                prompt += f"{i+1}. {subj_desc} {fact['relation']} {obj_desc}\n"
        else:
            prompt += "无具体关系信息，请基于描述进行合理关联。\n"
        
        prompt += f"""
---

请基于以上信息生成一段客观的背景陈述，要求：
- 体现所有模糊实体描述
- 严格遵循提供的关系路径
- 语言客观、简洁
- 为后续问题做铺垫
- 语言流畅，不要出现“某实体”的表述，如果实体是人，则用“某人”、“该人”等代替，其他类型实体也需同样根据其性质表述。

直接输出陈述内容，不要包含任何前缀或说明。"""
        
        result = self._batch_llm_call(prompt)
        
        if result and not result.startswith("API调用失败"):
            # 检查生成的故事是否泄露答案
            if not self._contains_entity_leak(result, answer_entity):
                sample['narrative_story'] = result.strip()
                sample['status'] = 'story_woven_with_relations'
            else:
                # 如果泄露答案，使用更安全的回退方案
                safe_story = self._create_safe_fallback_story(descriptions, relation_facts)
                sample['narrative_story'] = safe_story
                sample['status'] = 'story_safe_fallback'
        else:
            # 回退到基于关系的安全拼接
            safe_story = self._create_safe_fallback_story(descriptions, relation_facts)
            sample['narrative_story'] = safe_story
            sample['status'] = 'story_fallback_with_relations'

    def _create_safe_fallback_story(self, descriptions, relation_facts):
        """
        创建安全的回退故事，确保不泄露答案
        """
        if not descriptions:
            return "存在一些相关的线索和关系"
        
        if relation_facts and len(descriptions) > 1:
            return f"根据相关线索，{descriptions[0]}通过{relation_facts[0]['relation']}等关系与{descriptions[1]}等内容相互关联，这些关系线索共同指向一个特定的目标。"
        else:
            return f"根据相关线索，涉及{', '.join(descriptions[:3])}等内容，这些线索共同指向一个特定的目标。"

    def _question_generation_and_validation_batch(self, batch_samples):
        """
        第三阶段：问题生成与校验（基于v4版本的高级LLM包装逻辑）
        
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
            if sample.get('narrative_story') and sample.get('answer_entity'):
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    ⚠️  没有可用于问题生成的样本")
            return
        
        # 为每个样本生成问题
        for sample in valid_samples:
            self._generate_single_question(sample)
        
        success_count = len([s for s in valid_samples if s.get('generated_question')])
        print(f"    问题生成完成：{success_count}/{len(valid_samples)} 个样本")
    
    def _generate_single_question(self, sample):
        """
        为单个样本生成完整的问题（基于v4版本的严格要求，参考目标格式）
        """
        narrative_story = sample.get('narrative_story', '')
        answer_entity = sample.get('answer_entity', '')
        clue_entities = sample.get('clue_entities', [])
        relation_facts = sample.get('relation_facts', [])
        
        prompt = f"""你是一位精准的提问专家。你的任务是根据下面提供的【背景陈述】和【预设答案】，创建一个完整的问题。

核心要求：
1. **答案导向**：生成的问题的唯一正确答案必须是给定的【预设答案】。
2. **自然衔接**：问题需要与【背景陈述】的结尾紧密衔接，读起来通顺自然。
3. **绝对禁止泄露**：问题本身不能包含【预设答案】的任何直接线索、文字或部分内容。
4. **简洁清晰**：问题表述应简洁、清晰，符合人类的提问习惯。
5. **逻辑完整**：确保根据背景信息能够逻辑推理出唯一答案。

输出格式要求：
请严格按照以下格式生成完整问题：

**问题：**  
[在这里写具体的问题内容，与问题背景故事自然衔接]

**解析：**  
1. [解析第一个关键信息点，说明如何从背景中获得线索]
2. [解析第二个关键信息点，说明推理逻辑]
3. [总结推理过程，说明如何得出最终答案]

---
【背景陈述】：
{narrative_story}

【预设答案】：
{answer_entity}

【线索实体】：
{', '.join(clue_entities)}
---

请严格根据以上要求和格式，生成完整的问题："""

        result = self._batch_llm_call(prompt)
        
        if result and not result.startswith("API调用失败"):
            # 检查问题是否泄露答案
            if not self._contains_entity_leak(result, answer_entity):
                sample['generated_question'] = result.strip()
                sample['answer'] = answer_entity
            else:
                # 如果泄露答案，生成更安全的问题
                safe_question = self._generate_safe_structured_question(narrative_story, answer_entity, clue_entities)
                sample['generated_question'] = safe_question
                sample['answer'] = answer_entity
        else:
            # 回退：生成安全的结构化问题
            safe_question = self._generate_safe_structured_question(narrative_story, answer_entity, clue_entities)
            sample['generated_question'] = safe_question
            sample['answer'] = answer_entity

    def _generate_safe_structured_question(self, narrative_story, answer_entity, clue_entities):
        """
        生成安全的结构化问题，确保不泄露答案（参考目标格式）
        """
        if not narrative_story:
            narrative_story = "在某个复杂的关联网络中，多个要素之间存在着密切的联系。"
        
        # 安全的问题模板
        question_templates = [
            "根据以上描述，这个网络最终指向的核心概念是什么？",
            "通过这些关联信息，最终的目标是什么？", 
            "基于这些线索，答案是什么？",
            "这个关联链的终点是什么？"
        ]
        
        main_question = random.choice(question_templates)
        
        # 生成解析部分
        analysis_parts = [
            f"1. 整合背景信息中的多个关键要素，发现它们形成一个关联网络。",
            f"2. 通过分析{len(clue_entities)}个线索实体之间的逻辑关系，建立推理链条。",
            f"3. 结合所有信息进行综合推理，确定最终答案。"
        ]
        
        # 构建完整的结构化问题
        structured_question = f"""**问题：**  
{narrative_story}{main_question}  

**解析：**  
{chr(10).join(analysis_parts)}"""
        
        return structured_question

    def _generate_safe_question(self, narrative_story):
        """
        生成安全的通用问题，确保不泄露答案
        """
        if not narrative_story:
            return "这是什么？"
        
        # 基于故事内容的安全问题模板
        safe_templates = [
            "这指向的是什么？",
            "这个目标是什么？",
            "这描述的是什么？",
            "根据以上信息，答案是什么？"
        ]
        
        return random.choice(safe_templates)
    
    def _validate_questions_batch(self, batch_samples):
        """
        批量校验问题的逻辑正确性
        """
        print(f"    执行问题校验...")
        
        valid_samples = []
        for sample in batch_samples:
            if (sample.get('generated_question') and 
                sample.get('answer') and 
                sample.get('narrative_story')):
                valid_samples.append(sample)
        
        if not valid_samples:
            print(f"    ⚠️  没有可用于校验的样本")
            return
        
        # 批量校验（基于v4版本的严格标准）
        validation_prompt = """你是一位逻辑推理专家。请判断以下每组【问题背景故事+问题】是否能够逻辑推理出对应的【答案】。

严格评判标准：
1. 问题背景故事中的信息是否足够推导出唯一答案
2. 问题与答案是否逻辑一致
3. 推理链条是否完整且无歧义
4. 问题是否存在答案泄露（包含答案的直接或间接线索）

请对每个问题组合只回答"通过"或"不通过"：

"""
        
        for i, sample in enumerate(valid_samples):
            story = sample.get('narrative_story', '')
            question = sample.get('generated_question', '')
            answer = sample.get('answer', '')
            
            validation_prompt += f"""
问题组合 {i + 1}:
问题背景故事：{story}
问题：{question}
答案：{answer}
"""
        
        validation_prompt += "\n请严格按照以下格式返回，每个结果占一行:\n结果 1: [通过/不通过]\n结果 2: [通过/不通过]\n..."
        
        validation_result = self._batch_llm_call(validation_prompt)
        
        # 解析校验结果（基于v4版本的严格处理）
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
                    # 额外的本地检查：确保问题不包含答案
                    question = sample.get('generated_question', '')
                    answer = sample.get('answer', '')
                    
                    if not self._contains_entity_leak(question, answer):
                        sample['validation_status'] = '通过（本地检查）'
                        sample['final_status'] = 'validated'
                        passed_count += 1
                    else:
                        sample['validation_status'] = '失败：问题泄露答案'
                        sample['final_status'] = 'validation_failed'
            
            print(f"    校验完成：{passed_count}/{len(valid_samples)} 个问题通过校验")
        else:
            print(f"    ⚠️  LLM校验失败，使用本地检查")
            passed_count = 0
            for sample in valid_samples:
                question = sample.get('generated_question', '')
                answer = sample.get('answer', '')
                
                if not self._contains_entity_leak(question, answer):
                    sample['validation_status'] = '通过（本地检查）'
                    sample['final_status'] = 'validated'
                    passed_count += 1
                else:
                    sample['validation_status'] = '失败：问题泄露答案'
                    sample['final_status'] = 'validation_failed'
            
            print(f"    本地校验完成：{passed_count}/{len(valid_samples)} 个问题通过")
        
        # 为不在valid_samples中的样本设置默认状态
        for sample in batch_samples:
            if sample not in valid_samples:
                sample['validation_status'] = '失败：缺少必要信息'
                sample['final_status'] = 'failed'
        
        validated_count = len([s for s in batch_samples if s.get('final_status') == 'validated'])
        print(f"    问题校验完成：{validated_count}/{len(batch_samples)} 通过")

    def _save_stage_results(self, batch_samples, stage_name, batch_idx):
        """保存阶段结果"""
        stage_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "stage_results")
        os.makedirs(stage_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stage_file = os.path.join(stage_dir, f"{stage_name}_batch_{batch_idx:03d}_{timestamp}.jsonl")
        
        with open(stage_file, 'w', encoding='utf-8') as f:
            for sample in batch_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        
        print(f"  阶段结果已保存到: {stage_file}")
