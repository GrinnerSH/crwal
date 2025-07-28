import random
import networkx as nx

class PathSampler:
    def __init__(self, graph):
        self.G = graph
        self.nodes = list(graph.nodes)

    def _path_to_triples(self, path):
        """
        将节点路径转换为三元组路径，确保关系信息完整传递
        """
        triples = []
        for i in range(len(path) - 1):
            u, v = path[i], path[i+1]
            # 在MultiDiGraph中，两个节点间可能有多个关系
            edge_data = self.G.get_edge_data(u, v)
            if edge_data:
                # 如果有多个关系，随机选择一个，但确保关系信息明确
                if isinstance(edge_data, dict):
                    available_relations = list(edge_data.keys())
                    if available_relations:
                        rel = random.choice(available_relations)
                    else:
                        rel = "相关"  # 默认关系
                else:
                    rel = "相关"  # 默认关系
                    
                # 确保创建完整的三元组结构
                triple = {
                    "subject": u, 
                    "relation": rel, 
                    "object": v,
                    "edge_data": edge_data  # 保留原始边数据用于调试
                }
                triples.append(triple)
            else:
                # 如果没有边数据，创建默认关系
                triple = {
                    "subject": u,
                    "relation": "相关",
                    "object": v,
                    "edge_data": None
                }
                triples.append(triple)
        return triples

    def sample_chain_reasoning(self, length=5, num_samples=15):
        """采样链式推理路径"""
        sampled_paths = []
        attempts = 0
        max_attempts = num_samples * 100

        while len(sampled_paths) < num_samples and attempts < max_attempts:
            attempts += 1
            start_node = random.choice(self.nodes)
            path = [start_node]
            current_node = start_node
            
            for _ in range(length):
                neighbors = list(self.G.successors(current_node))
                if not neighbors:
                    break
                next_node = random.choice(neighbors)
                path.append(next_node)
                current_node = next_node
            
            if len(path) == length + 1:
                # 随机选择问题类型以增加多样性
                question_types = ["identity", "属性", "关系", "特征", "性质", "定义", "分类", "来源", "用途", "作用"]
                selected_type = random.choice(question_types)
                
                sampled_paths.append({
                    "architecture": "Chain Reasoning",
                    "path": self._path_to_triples(path),
                    "target_entity": path[-1],
                    "question_payload_type": selected_type
                })
        return sampled_paths

    def sample_cross_referencing(self, num_clues=3, num_samples=10):
        """
        采样交叉定位路径：寻找汇聚于同一节点的多个独立线索
        这种模式适合生成需要综合多个信息源的复杂推理问题
        """
        sampled_paths = []
        attempts = 0
        max_attempts = num_samples * 200
        
        # 找到具有高入度的节点作为汇聚点
        in_degrees = dict(self.G.in_degree())
        high_degree_nodes = [node for node, degree in in_degrees.items() if degree >= num_clues]
        
        if not high_degree_nodes:
            print(f"警告：没有找到入度 >= {num_clues} 的节点，降低要求到入度 >= 2")
            high_degree_nodes = [node for node, degree in in_degrees.items() if degree >= 2]
            num_clues = min(num_clues, 2)
        
        while len(sampled_paths) < num_samples and attempts < max_attempts:
            attempts += 1
            
            # 随机选择一个高入度节点作为汇聚点
            if not high_degree_nodes:
                break
                
            convergence_node = random.choice(high_degree_nodes)
            predecessors = list(self.G.predecessors(convergence_node))
            
            if len(predecessors) < num_clues:
                continue
            
            # 随机选择多个前驱节点作为线索起点
            clue_starts = random.sample(predecessors, min(num_clues, len(predecessors)))
            
            # 为每个线索起点构建到汇聚点的路径
            clue_paths = []
            all_clues_valid = True
            
            for start_node in clue_starts:
                # 构建从起点到汇聚点的简单路径
                try:
                    if nx.has_path(self.G, start_node, convergence_node):
                        simple_path = nx.shortest_path(self.G, start_node, convergence_node)
                        if len(simple_path) >= 2:  # 至少有一跳
                            clue_paths.append(simple_path)
                        else:
                            all_clues_valid = False
                            break
                    else:
                        all_clues_valid = False
                        break
                except:
                    all_clues_valid = False
                    break
            
            if not all_clues_valid or len(clue_paths) < 2:
                continue
            
            # 将多个线索路径合并为一个交叉定位路径
            # 策略：随机选择其中一条作为主路径，其他作为辅助线索
            main_path = random.choice(clue_paths)
            auxiliary_clues = [path for path in clue_paths if path != main_path]
            
            # 构建综合的三元组路径
            combined_triples = self._path_to_triples(main_path)
            
            # 添加辅助线索的关键三元组
            for aux_path in auxiliary_clues[:2]:  # 最多添加2个辅助线索
                if len(aux_path) >= 2:
                    # 添加辅助路径的最后一跳（指向汇聚点）
                    aux_triple = self._path_to_triples(aux_path[-2:])
                    if aux_triple:
                        combined_triples.extend(aux_triple)
            
            if len(combined_triples) >= 2:
                # 随机选择问题类型
                question_types = ["交叉验证", "综合分析", "多源确认", "关联推理", "汇聚点识别"]
                selected_type = random.choice(question_types)
                
                sampled_paths.append({
                    "architecture": "Cross Referencing",
                    "path": combined_triples,
                    "target_entity": convergence_node,
                    "convergence_node": convergence_node,
                    "clue_sources": clue_starts,
                    "num_clues": len(clue_paths),
                    "question_payload_type": selected_type
                })
        
        print(f"交叉定位采样完成：成功生成 {len(sampled_paths)}/{num_samples} 个样本")
        return sampled_paths

    def sample_anchor_pivot(self, max_len=4, num_samples=10):
        """
        采样锚点与跳板路径：寻找通过共享属性进行跳跃的路径
        例如：A -> 共享属性 -> B，其中共享属性可能是年份、地点、类别等
        """
        sampled_paths = []
        attempts = 0
        max_attempts = num_samples * 300
        
        # 寻找可能的锚点节点（通常是时间、地点、数值等）
        potential_anchors = []
        
        # 识别潜在的锚点节点（具有多个入边和出边的节点）
        for node in self.nodes:
            in_degree = self.G.in_degree(node)
            out_degree = self.G.out_degree(node)
            
            # 锚点特征：有足够的连接，可能是共享属性
            if in_degree >= 2 and out_degree >= 2:
                potential_anchors.append(node)
            
            # 特殊识别：年份、地点、数值等典型锚点
            node_str = str(node).lower()
            if (any(keyword in node_str for keyword in ['年', '月', '日', '省', '市', '县', '区', '国', '州']) or
                node_str.isdigit() or 
                any(keyword in node_str for keyword in ['university', 'company', 'organization', 'group'])):
                if node not in potential_anchors:
                    potential_anchors.append(node)
        
        if not potential_anchors:
            print("警告：未找到合适的锚点节点，使用度数较高的节点")
            degrees = dict(self.G.degree())
            potential_anchors = [node for node, degree in sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:20]]
        
        while len(sampled_paths) < num_samples and attempts < max_attempts:
            attempts += 1
            
            if not potential_anchors:
                break
            
            # 随机选择锚点
            anchor_node = random.choice(potential_anchors)
            
            # 找到指向锚点的节点和从锚点出发的节点
            predecessors = list(self.G.predecessors(anchor_node))
            successors = list(self.G.successors(anchor_node))
            
            if len(predecessors) < 1 or len(successors) < 1:
                continue
            
            # 构建锚点跳板路径：A -> 锚点 -> B
            try:
                # 随机选择前驱和后继
                start_node = random.choice(predecessors)
                end_node = random.choice(successors)
                
                # 确保不是同一个节点
                if start_node == end_node:
                    continue
                
                # 构建完整路径：可能包括到达锚点前的路径
                path_to_anchor = [start_node, anchor_node]
                path_from_anchor = [anchor_node, end_node]
                
                # 尝试扩展路径到达锚点前的部分
                if random.random() < 0.6 and max_len > 2:  # 60%概率扩展
                    extended_start = self._extend_path_backward(start_node, max_len - 2)
                    if extended_start:
                        path_to_anchor = extended_start + [anchor_node]
                
                # 尝试扩展从锚点出发的部分
                if random.random() < 0.6 and len(path_to_anchor) < max_len:  # 60%概率扩展
                    remaining_len = max_len - len(path_to_anchor)
                    extended_end = self._extend_path_forward(end_node, remaining_len)
                    if extended_end:
                        path_from_anchor = [anchor_node] + extended_end
                
                # 合并路径（避免重复锚点）
                full_path = path_to_anchor + path_from_anchor[1:]
                
                if len(full_path) >= 3 and len(full_path) <= max_len:  # 确保路径长度合理
                    triples = self._path_to_triples(full_path)
                    
                    if len(triples) >= 2:
                        # 随机选择问题类型
                        question_types = ["跳板推理", "锚点定位", "属性桥接", "中介关系", "共享特征"]
                        selected_type = random.choice(question_types)
                        
                        sampled_paths.append({
                            "architecture": "Anchor Pivot",
                            "path": triples,
                            "target_entity": full_path[-1],
                            "anchor_node": anchor_node,
                            "pivot_type": self._classify_anchor_type(anchor_node),
                            "path_segments": {
                                "to_anchor": path_to_anchor,
                                "from_anchor": path_from_anchor
                            },
                            "question_payload_type": selected_type
                        })
                        
            except Exception as e:
                continue
        
        print(f"锚点跳板采样完成：成功生成 {len(sampled_paths)}/{num_samples} 个样本")
        return sampled_paths
    
    def _extend_path_backward(self, start_node, max_extension):
        """向前扩展路径（寻找指向start_node的路径）"""
        extended_path = []
        current_node = start_node
        
        for _ in range(max_extension):
            predecessors = list(self.G.predecessors(current_node))
            if not predecessors:
                break
            
            prev_node = random.choice(predecessors)
            extended_path.insert(0, prev_node)
            current_node = prev_node
            
            # 避免形成环路
            if prev_node == start_node:
                break
        
        return extended_path if extended_path else None
    
    def _extend_path_forward(self, start_node, max_extension):
        """向后扩展路径（从start_node出发）"""
        extended_path = [start_node]
        current_node = start_node
        
        for _ in range(max_extension):
            successors = list(self.G.successors(current_node))
            if not successors:
                break
            
            next_node = random.choice(successors)
            extended_path.append(next_node)
            current_node = next_node
            
            # 避免形成环路
            if next_node in extended_path[:-1]:
                break
        
        return extended_path[1:] if len(extended_path) > 1 else None
    
    def _classify_anchor_type(self, anchor_node):
        """分类锚点类型"""
        node_str = str(anchor_node).lower()
        
        # 时间类锚点
        if any(keyword in node_str for keyword in ['年', '月', '日', 'year', 'date']) or node_str.isdigit():
            return "temporal"
        
        # 地理类锚点
        elif any(keyword in node_str for keyword in ['省', '市', '县', '区', '国', '州', 'city', 'country', 'province']):
            return "geographical"
        
        # 机构类锚点
        elif any(keyword in node_str for keyword in ['university', 'company', 'organization', 'school', 'group', '大学', '公司', '机构']):
            return "institutional"
        
        # 属性类锚点
        elif any(keyword in node_str for keyword in ['type', 'category', '类', '种', '型']):
            return "categorical"
        
        # 默认类型
        else:
            return "general"
    
    def sample_multiple_strategies(self, total_samples=50, strategy_weights=None):
        """
        使用多种策略进行综合采样
        
        Args:
            total_samples: 总的采样数量
            strategy_weights: 各策略的权重字典，例如：
                             {"chain": 0.6, "cross": 0.25, "anchor": 0.15}
        """
        if strategy_weights is None:
            strategy_weights = {
                "chain": 0.6,      # 链式推理：60%
                "cross": 0.25,     # 交叉定位：25%  
                "anchor": 0.15     # 锚点跳板：15%
            }
        
        # 确保权重和为1
        total_weight = sum(strategy_weights.values())
        if total_weight != 1.0:
            strategy_weights = {k: v/total_weight for k, v in strategy_weights.items()}
        
        # 计算各策略的采样数量
        chain_samples = int(total_samples * strategy_weights.get("chain", 0))
        cross_samples = int(total_samples * strategy_weights.get("cross", 0))
        anchor_samples = total_samples - chain_samples - cross_samples  # 剩余的给锚点策略
        
        print(f"多重采样策略分配：")
        print(f"  链式推理: {chain_samples} 个样本")
        print(f"  交叉定位: {cross_samples} 个样本") 
        print(f"  锚点跳板: {anchor_samples} 个样本")
        print(f"  总计: {total_samples} 个样本")
        
        all_samples = []
        
        # 执行链式推理采样
        if chain_samples > 0:
            print(f"\n执行链式推理采样...")
            chain_paths = self.sample_chain_reasoning(
                length=random.randint(3, 6), 
                num_samples=chain_samples
            )
            all_samples.extend(chain_paths)
        
        # 执行交叉定位采样
        if cross_samples > 0:
            print(f"\n执行交叉定位采样...")
            cross_paths = self.sample_cross_referencing(
                num_clues=random.randint(2, 4),
                num_samples=cross_samples
            )
            all_samples.extend(cross_paths)
        
        # 执行锚点跳板采样
        if anchor_samples > 0:
            print(f"\n执行锚点跳板采样...")
            anchor_paths = self.sample_anchor_pivot(
                max_len=random.randint(3, 5),
                num_samples=anchor_samples
            )
            all_samples.extend(anchor_paths)
        
        # 随机打乱所有样本
        random.shuffle(all_samples)
        
        print(f"\n多重采样完成：总共生成 {len(all_samples)} 个样本")
        
        # 统计各种架构的分布
        architecture_stats = {}
        for sample in all_samples:
            arch = sample.get("architecture", "Unknown")
            architecture_stats[arch] = architecture_stats.get(arch, 0) + 1
        
        print(f"最终架构分布：")
        for arch, count in architecture_stats.items():
            percentage = (count / len(all_samples)) * 100 if all_samples else 0
            print(f"  {arch}: {count} 个 ({percentage:.1f}%)")
        
        return all_samples
    
    def sample_adaptive_strategy(self, total_samples=50, complexity_level="medium"):
        """
        自适应采样策略：根据图的特征和复杂度需求动态调整采样参数
        
        Args:
            total_samples: 总采样数量
            complexity_level: 复杂度级别 ("low", "medium", "high")
        """
        # 分析图的基本特征
        num_nodes = len(self.nodes)
        num_edges = self.G.number_of_edges()
        avg_degree = num_edges / num_nodes if num_nodes > 0 else 0
        
        print(f"图结构分析：")
        print(f"  节点数: {num_nodes}")
        print(f"  边数: {num_edges}")
        print(f"  平均度数: {avg_degree:.2f}")
        
        # 根据复杂度级别调整策略
        if complexity_level == "low":
            strategy_weights = {"chain": 0.8, "cross": 0.15, "anchor": 0.05}
            chain_length_range = (2, 4)
            cross_clues_range = (2, 3)
            anchor_length_range = (3, 4)
        elif complexity_level == "medium":
            strategy_weights = {"chain": 0.6, "cross": 0.25, "anchor": 0.15}
            chain_length_range = (3, 6)
            cross_clues_range = (2, 4)
            anchor_length_range = (3, 5)
        else:  # high
            strategy_weights = {"chain": 0.4, "cross": 0.35, "anchor": 0.25}
            chain_length_range = (4, 7)
            cross_clues_range = (3, 5)
            anchor_length_range = (4, 6)
        
        # 根据图密度调整策略权重
        if avg_degree > 10:  # 高密度图，更适合复杂策略
            strategy_weights["cross"] += 0.1
            strategy_weights["anchor"] += 0.1
            strategy_weights["chain"] -= 0.2
        elif avg_degree < 3:  # 低密度图，主要使用链式
            strategy_weights["chain"] += 0.2
            strategy_weights["cross"] -= 0.1
            strategy_weights["anchor"] -= 0.1
        
        print(f"复杂度级别: {complexity_level}")
        print(f"调整后的策略权重: {strategy_weights}")
        
        # 计算各策略采样数量
        chain_samples = int(total_samples * strategy_weights["chain"])
        cross_samples = int(total_samples * strategy_weights["cross"])
        anchor_samples = total_samples - chain_samples - cross_samples
        
        all_samples = []
        
        # 执行采样
        if chain_samples > 0:
            print(f"\n执行链式推理采样 ({chain_samples} 个)...")
            for _ in range(chain_samples):
                length = random.randint(*chain_length_range)
                paths = self.sample_chain_reasoning(length=length, num_samples=1)
                all_samples.extend(paths)
        
        if cross_samples > 0:
            print(f"\n执行交叉定位采样 ({cross_samples} 个)...")
            for _ in range(cross_samples):
                num_clues = random.randint(*cross_clues_range)
                paths = self.sample_cross_referencing(num_clues=num_clues, num_samples=1)
                all_samples.extend(paths)
        
        if anchor_samples > 0:
            print(f"\n执行锚点跳板采样 ({anchor_samples} 个)...")
            for _ in range(anchor_samples):
                max_len = random.randint(*anchor_length_range)
                paths = self.sample_anchor_pivot(max_len=max_len, num_samples=1)
                all_samples.extend(paths)
        
        # 随机打乱
        random.shuffle(all_samples)
        
        print(f"\n自适应采样完成：总共生成 {len(all_samples)} 个样本")
        return all_samples
