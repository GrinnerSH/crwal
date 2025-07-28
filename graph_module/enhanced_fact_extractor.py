# #!/usr/bin/env python3
# """
# 增强的事实提取器 - 支持实时保存三元组
# """

# import json
# import requests
# import time
# import os
# from datetime import datetime

# import sys
# import os
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from config import config

# class EnhancedLLMExtractor:
#     """增强的LLM事实提取器，支持实时保存三元组"""
    
#     def __init__(self, save_triples=True):
#         """
#         初始化提取器
        
#         Args:
#             save_triples: 是否实时保存三元组到文件
#         """
#         self.api_key = config.LLM_API_KEY
#         self.api_url = config.LLM_API_BASE_URL
#         self.save_triples = save_triples
        
#         # 用于追踪关系类型使用情况
#         self.relation_stats = {
#             'predefined': {},  # 预定义关系的使用次数
#             'custom': {},      # 自定义关系的使用次数
#             'total_count': 0   # 总三元组数量
#         }
        
#         # 设置三元组保存路径
#         if save_triples:
#             self.triples_save_dir = os.path.join(config.RAW_HTML_DIR, "..", "extracted_triples")
#             os.makedirs(self.triples_save_dir, exist_ok=True)
            
#             # 创建当前会话的三元组文件
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             self.current_session_file = os.path.join(
#                 self.triples_save_dir, 
#                 f"triples_session_{timestamp}.jsonl"
#             )
            
#             print(f"✅ 三元组实时保存已启用")
#             print(f"   保存目录: {self.triples_save_dir}")
#             print(f"   当前会话文件: {os.path.basename(self.current_session_file)}")
    
#     def extract_triples_from_blocks(self, text_blocks, source_url=None):
#         """
#         从文本块中提取三元组，并实时保存
        
#         Args:
#             text_blocks: 文本块列表
#             source_url: 源URL
            
#         Returns:
#             提取的三元组列表
#         """
#         if not text_blocks:
#             return []
        
#         print(f"🔍 开始提取三元组 (共{len(text_blocks)}个文本块)...")
        
#         # 构建提示
#         prompt = self._build_prompt(text_blocks)
        
#         # 调用LLM
#         response = self._call_llm(prompt)
        
#         if not response:
#             print("❌ LLM调用失败")
#             return []
        
#         # 解析响应
#         extracted_data = self._parse_response(response)
        
#         if not extracted_data:
#             print("❌ 响应解析失败")
#             return []
        
#         # 处理提取的三元组
#         all_triples = []
#         for item in extracted_data:
#             if isinstance(item, dict) and "triples" in item:
#                 block_triples = item["triples"]
#                 for triple in block_triples:
#                     # 添加源URL信息
#                     triple['source_url'] = source_url
#                     # 分析关系类型
#                     self._analyze_relation_type(triple.get('relation', ''))
#                     all_triples.append(triple)
        
#         print(f"✅ 成功提取 {len(all_triples)} 个三元组")
        
#         # 实时保存三元组
#         if self.save_triples and all_triples:
#             self._save_triples_to_file(all_triples, source_url)
        
#         return all_triples
    
#     def _save_triples_to_file(self, triples, source_url):
#         """将三元组实时保存到文件"""
#         save_entry = {
#             "timestamp": datetime.now().isoformat(),
#             "source_url": source_url,
#             "triples_count": len(triples),
#             "triples": triples,
#             "relation_stats": self.get_relation_statistics()
#         }
        
#         try:
#             # 保存到当前会话文件
#             with open(self.current_session_file, 'a', encoding='utf-8') as f:
#                 f.write(json.dumps(save_entry, ensure_ascii=False) + '\n')
            
#             # 同时保存到按URL命名的文件
#             if source_url:
#                 url_hash = str(hash(source_url))[-8:]  # 取URL哈希的后8位
#                 url_filename = f"triples_url_{url_hash}.json"
#                 url_filepath = os.path.join(self.triples_save_dir, url_filename)
                
#                 with open(url_filepath, 'w', encoding='utf-8') as f:
#                     json.dump(save_entry, f, ensure_ascii=False, indent=2)
            
#             print(f"💾 已保存 {len(triples)} 个三元组到文件")
            
#         except Exception as e:
#             print(f"⚠️  保存三元组失败: {e}")
    
#     def _build_prompt(self, text_blocks):
#         """构建用于批量抽取的完整提示"""
        
#         # 定义实体和关系模式
#         schema_definition = f"""
#         # 实体类型定义:
#         {json.dumps(config.ENTITY_TYPES, indent=2)}

#         # 关系类型定义 (建议使用，但不限制):
#         {json.dumps(config.RELATION_TYPES, indent=2)}
#         """

#         # 上下文学习示例
#         in_context_examples = """
#         # 示例:
#         ## 输入文本块:
#         1. "这部电影由克里斯托弗·诺兰执导,于2010年上映。"
#         2. "玛丽·居里出生于华沙,后来移居巴黎。她发现了镭元素。"
        
#         ## 输出JSON:
#         [
#             {
#                 "block_id": 1,
#                 "triples": [
#                     {"subject": "这部电影", "relation": "导演", "object": "克里斯托弗·诺兰"},
#                     {"subject": "这部电影", "relation": "发生年份", "object": "2010"}
#                 ]
#             },
#             {
#                 "block_id": 2,
#                 "triples": [
#                     {"subject": "玛丽·居里", "relation": "出生地", "object": "华沙"},
#                     {"subject": "玛丽·居里", "relation": "位于", "object": "巴黎"},
#                     {"subject": "玛丽·居里", "relation": "发现", "object": "镭元素"}
#                 ]
#             }
#         ]
#         """

#         # 构建文本块字符串
#         blocks_str = ""
#         for i, block in enumerate(text_blocks, 1):
#             blocks_str += f"{i}. \"{block['text']}\"\n"

#         prompt = f"""你是一个专业的知识图谱构建专家。请从给定的中文文本块中提取结构化的知识三元组。

#         {schema_definition}

#         ## 提取要求:
#         1. 仔细分析每个文本块，提取其中的实体和关系
#         2. 关系类型优先使用预定义列表中的类型，但如果文本中有更合适的关系表达，可以创建新的关系类型
#         3. 确保主语和宾语都是有意义的实体
#         4. 避免提取过于细碎或无意义的关系
#         5. 确保输出的JSON格式正确

#         {in_context_examples}

#         ## 待处理文本块:
#         {blocks_str}

#         请按照示例格式输出JSON，确保格式正确且完整："""

#         return prompt

#     def _call_llm(self, prompt):
#         """调用LLM API"""
#         headers = {
#             "Authorization": f"Bearer {self.api_key}",
#             "Content-Type": "application/json"
#         }
        
#         data = {
#             "model": config.LLM_MODEL_NAME,
#             "messages": [
#                 {"role": "user", "content": prompt}
#             ],
#             "temperature": config.LLM_TEMPERATURE,
#             "max_tokens": config.LLM_MAX_TOKENS,
#             "stream": config.LLM_USE_STREAMING
#         }
        
#         try:
#             response = requests.post(self.api_url, headers=headers, json=data, timeout=120)
#             response.raise_for_status()
            
#             result = response.json()
#             if 'choices' in result and len(result['choices']) > 0:
#                 content = result['choices'][0]['message']['content']
#                 print(f"Response preview: {content[:100]}...")
#                 return content
#             else:
#                 print(f"❌ 意外的API响应格式: {result}")
#                 return None
                
#         except requests.exceptions.RequestException as e:
#             print(f"❌ API请求错误: {e}")
#             return None
#         except Exception as e:
#             print(f"❌ 调用LLM时发生错误: {e}")
#             return None

#     def _parse_response(self, response_text):
#         """解析LLM响应，支持多种格式和错误修复"""
#         if not response_text:
#             return None
        
#         # 尝试多种解析策略
#         parsing_strategies = [
#             self._parse_direct_json,
#             self._parse_json_from_markdown,
#             self._parse_json_with_repair,
#             self._parse_truncated_json
#         ]
        
#         for strategy in parsing_strategies:
#             try:
#                 result = strategy(response_text)
#                 if result is not None:
#                     print(f"✅ 使用 {strategy.__name__} 成功解析响应")
#                     return result
#             except Exception as e:
#                 print(f"⚠️  {strategy.__name__} 解析失败: {e}")
#                 continue
        
#         print(f"❌ 所有解析策略都失败了")
#         return None

#     def _parse_direct_json(self, text):
#         """直接解析JSON"""
#         return json.loads(text.strip())

#     def _parse_json_from_markdown(self, text):
#         """从markdown代码块中提取JSON"""
#         import re
        
#         # 查找JSON代码块
#         json_pattern = r'```(?:json)?\s*(\[.*?\])\s*```'
#         match = re.search(json_pattern, text, re.DOTALL)
        
#         if match:
#             json_str = match.group(1)
#             return json.loads(json_str)
        
#         return None

#     def _parse_json_with_repair(self, text):
#         """尝试修复并解析JSON"""
#         # 查找看起来像JSON的部分
#         import re
        
#         # 查找以[开头的部分
#         start_idx = text.find('[')
#         if start_idx == -1:
#             return None
        
#         # 从开始位置截取
#         json_part = text[start_idx:]
        
#         # 尝试找到匹配的结束括号
#         bracket_count = 0
#         end_idx = -1
        
#         for i, char in enumerate(json_part):
#             if char == '[':
#                 bracket_count += 1
#             elif char == ']':
#                 bracket_count -= 1
#                 if bracket_count == 0:
#                     end_idx = i + 1
#                     break
        
#         if end_idx > 0:
#             json_str = json_part[:end_idx]
#             return json.loads(json_str)
        
#         return None

#     def _parse_truncated_json(self, text):
#         """处理截断的JSON响应"""
#         # 查找JSON开始
#         start_idx = text.find('[')
#         if start_idx == -1:
#             return None
        
#         json_part = text[start_idx:]
        
#         # 如果JSON看起来是截断的，尝试修复
#         if not json_part.rstrip().endswith(']'):
#             # 尝试添加缺失的结构
#             fixed_attempts = [
#                 json_part + ']',
#                 json_part + '}]',
#                 json_part + '"}]}]'
#             ]
            
#             for attempt in fixed_attempts:
#                 try:
#                     return json.loads(attempt)
#                 except:
#                     continue
        
#         # 如果没有修复，按原样解析
#         return json.loads(json_part)

#     def _analyze_relation_type(self, relation):
#         """分析关系类型并更新统计"""
#         if not relation:
#             return
        
#         self.relation_stats['total_count'] += 1
        
#         if relation in config.RELATION_TYPES:
#             # 预定义关系
#             self.relation_stats['predefined'][relation] = self.relation_stats['predefined'].get(relation, 0) + 1
#         else:
#             # 自定义关系
#             self.relation_stats['custom'][relation] = self.relation_stats['custom'].get(relation, 0) + 1

#     def get_relation_statistics(self):
#         """获取关系使用统计"""
#         total = self.relation_stats['total_count']
#         predefined_count = sum(self.relation_stats['predefined'].values())
#         custom_count = sum(self.relation_stats['custom'].values())
        
#         return {
#             "total_triples": total,
#             "predefined_relations": {
#                 "count": predefined_count,
#                 "percentage": (predefined_count / total * 100) if total > 0 else 0,
#                 "relations": self.relation_stats['predefined']
#             },
#             "custom_relations": {
#                 "count": custom_count,
#                 "percentage": (custom_count / total * 100) if total > 0 else 0,
#                 "relations": self.relation_stats['custom']
#             }
#         }

#     def print_relation_report(self):
#         """打印关系使用报告"""
#         stats = self.get_relation_statistics()
        
#         print("=" * 60)
#         print("                    关系类型使用报告")
#         print("=" * 60)
        
#         total = stats['total_triples']
#         predefined = stats['predefined_relations']
#         custom = stats['custom_relations']
        
#         print(f"总三元组数量: {total}")
        
#         print(f"📋 预定义关系使用情况:")
#         print(f"   数量: {predefined['count']} ({predefined['percentage']:.1f}%)")
#         if predefined['relations']:
#             print(f"   使用频率排序:")
#             sorted_pred = sorted(predefined['relations'].items(), key=lambda x: x[1], reverse=True)
#             for rel, count in sorted_pred:
#                 percentage = (count / total * 100) if total > 0 else 0
#                 print(f"     • {rel}: {count}次 ({percentage:.1f}%)")
        
#         print(f"🆕 自定义关系使用情况:")
#         print(f"   数量: {custom['count']} ({custom['percentage']:.1f}%)")
#         if custom['relations']:
#             print(f"   发现的新关系:")
#             sorted_custom = sorted(custom['relations'].items(), key=lambda x: x[1], reverse=True)
#             for rel, count in sorted_custom:
#                 percentage = (count / total * 100) if total > 0 else 0
#                 print(f"     • {rel}: {count}次 ({percentage:.1f}%)")
        
#         print("=" * 60)

#     def save_session_summary(self):
#         """保存当前会话的汇总信息"""
#         if not self.save_triples:
#             return
        
#         summary = {
#             "session_timestamp": datetime.now().isoformat(),
#             "total_triples_extracted": self.relation_stats['total_count'],
#             "relation_statistics": self.get_relation_statistics(),
#             "session_file": self.current_session_file
#         }
        
#         summary_file = os.path.join(self.triples_save_dir, "session_summary.json")
        
#         try:
#             with open(summary_file, 'w', encoding='utf-8') as f:
#                 json.dump(summary, f, ensure_ascii=False, indent=2)
#             print(f"📊 已保存会话汇总: {os.path.basename(summary_file)}")
#         except Exception as e:
#             print(f"⚠️  保存会话汇总失败: {e}")

#     def load_previous_session_stats(self, session_file):
#         """加载之前会话的统计信息"""
#         try:
#             with open(session_file, 'r', encoding='utf-8') as f:
#                 for line in f:
#                     entry = json.loads(line.strip())
#                     if 'relation_stats' in entry:
#                         prev_stats = entry['relation_stats']
                        
#                         # 合并统计信息
#                         for rel, count in prev_stats.get('predefined_relations', {}).get('relations', {}).items():
#                             self.relation_stats['predefined'][rel] = self.relation_stats['predefined'].get(rel, 0) + count
                        
#                         for rel, count in prev_stats.get('custom_relations', {}).get('relations', {}).items():
#                             self.relation_stats['custom'][rel] = self.relation_stats['custom'].get(rel, 0) + count
                        
#                         self.relation_stats['total_count'] += prev_stats.get('total_triples', 0)
            
#             print(f"✅ 已加载之前会话的统计信息")
            
#         except Exception as e:
#             print(f"⚠️  加载之前会话统计失败: {e}")
#!/usr/bin/env python3
"""
增强的事实提取器 - 支持实时保存三元组
"""

import json
import requests
import time
import os
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config

class EnhancedLLMExtractor:
    """增强的LLM事实提取器，支持实时保存三元组"""
    
    def __init__(self, save_triples=True):
        """
        初始化提取器
        
        Args:
            save_triples: 是否实时保存三元组到文件
        """
        self.api_key = config.LLM_API_KEY
        self.api_url = config.LLM_API_BASE_URL
        self.save_triples = save_triples
        
        # 用于追踪关系类型使用情况
        self.relation_stats = {
            'predefined': {},  # 预定义关系的使用次数
            'custom': {},      # 自定义关系的使用次数
            'total_count': 0   # 总三元组数量
        }
        
        # 设置三元组保存路径
        if save_triples:
            self.triples_save_dir = os.path.join(config.RAW_HTML_DIR, "..", "extracted_triples")
            os.makedirs(self.triples_save_dir, exist_ok=True)
            
            # 创建当前会话的三元组文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.current_session_file = os.path.join(
                self.triples_save_dir, 
                f"triples_session_{timestamp}.jsonl"
            )
            
            print(f"✅ 三元组实时保存已启用")
            print(f"   保存目录: {self.triples_save_dir}")
            print(f"   当前会话文件: {os.path.basename(self.current_session_file)}")
    
    def extract_triples_from_blocks(self, text_blocks, source_url=None):
        """
        从文本块中提取三元组，并实时保存
        
        Args:
            text_blocks: 文本块列表
            source_url: 源URL
            
        Returns:
            提取的三元组列表
        """
        if not text_blocks:
            return []
        
        print(f"🔍 开始提取三元组 (共{len(text_blocks)}个文本块)...")
        
        # 构建提示
        prompt = self._build_prompt(text_blocks)
        
        # 调用LLM
        response = self._call_llm(prompt)
        
        if not response:
            print("❌ LLM调用失败")
            return []
        
        # 解析响应
        extracted_data = self._parse_response(response)
        
        if not extracted_data:
            print("❌ 响应解析失败")
            return []
        
        # 处理提取的三元组
        all_triples = []
        for item in extracted_data:
            if isinstance(item, dict) and "triples" in item:
                block_triples = item["triples"]
                for triple in block_triples:
                    # 添加源URL信息
                    triple['source_url'] = source_url
                    # 分析关系类型
                    self._analyze_relation_type(triple.get('relation', ''))
                    all_triples.append(triple)
        
        print(f"✅ 成功提取 {len(all_triples)} 个三元组")
        
        # 实时保存三元组
        if self.save_triples and all_triples:
            self._save_triples_to_file(all_triples, source_url)
        
        return all_triples
    
    def _save_triples_to_file(self, triples, source_url):
        """将三元组实时保存到文件"""
        save_entry = {
            "timestamp": datetime.now().isoformat(),
            "source_url": source_url,
            "triples_count": len(triples),
            "triples": triples,
            "relation_stats": self.get_relation_statistics()
        }
        
        try:
            # 保存到当前会话文件
            with open(self.current_session_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(save_entry, ensure_ascii=False) + '\n')
            
            # 同时保存到按URL命名的文件
            if source_url:
                url_hash = str(hash(source_url))[-8:]  # 取URL哈希的后8位
                url_filename = f"triples_url_{url_hash}.json"
                url_filepath = os.path.join(self.triples_save_dir, url_filename)
                
                with open(url_filepath, 'w', encoding='utf-8') as f:
                    json.dump(save_entry, f, ensure_ascii=False, indent=2)
            
            print(f"💾 已保存 {len(triples)} 个三元组到文件")
            
        except Exception as e:
            print(f"⚠️  保存三元组失败: {e}")
    
    def _build_prompt(self, text_blocks):
        """构建用于批量抽取的完整提示"""
        
        # 定义实体和关系模式
        schema_definition = f"""
        # 实体类型定义:
        {json.dumps(config.ENTITY_TYPES, indent=2)}

        # 关系类型定义 (建议使用，但不限制):
        {json.dumps(config.RELATION_TYPES, indent=2)}
        """

        # 上下文学习示例
        in_context_examples = """
        # 示例:
        ## 输入文本块:
        1. "这部电影由克里斯托弗·诺兰执导,于2010年上映。"
        2. "玛丽·居里出生于华沙,后来移居巴黎。她发现了镭元素。"
        
        ## 输出JSON:
        [
            {
                "block_id": 1,
                "triples": [
                    {"subject": "这部电影", "relation": "导演", "object": "克里斯托弗·诺兰"},
                    {"subject": "这部电影", "relation": "发生年份", "object": "2010"}
                ]
            },
            {
                "block_id": 2,
                "triples": [
                    {"subject": "玛丽·居里", "relation": "出生地", "object": "华沙"},
                    {"subject": "玛丽·居里", "relation": "位于", "object": "巴黎"},
                    {"subject": "玛丽·居里", "relation": "发现", "object": "镭元素"}
                ]
            }
        ]
        """

        # 构建文本块字符串
        blocks_str = ""
        for i, block in enumerate(text_blocks, 1):
            blocks_str += f"{i}. \"{block['text']}\"\n"

        prompt = f"""你是一个专业的知识图谱构建专家。请从给定的中文文本块中提取结构化的知识三元组。

        {schema_definition}

        ## 提取要求:
        1. 仔细分析每个文本块，提取其中的实体和关系
        2. 关系类型优先使用预定义列表中的类型，但如果文本中有更合适的关系表达，可以创建新的关系类型
        3. 确保主语和宾语都是有意义的实体
        4. 避免提取过于细碎或无意义的关系
        5. 确保输出的JSON格式正确

        {in_context_examples}

        ## 待处理文本块:
        {blocks_str}

        请按照示例格式输出JSON，确保格式正确且完整："""

        return prompt

    def _call_llm(self, prompt):
        """调用LLM API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": config.LLM_MODEL_NAME,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": config.LLM_TEMPERATURE,
            "max_tokens": config.LLM_MAX_TOKENS,
            "stream": config.LLM_USE_STREAMING
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                print(f"Response preview: {content[:100]}...")
                return content
            else:
                print(f"❌ 意外的API响应格式: {result}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ API请求错误: {e}")
            return None
        except Exception as e:
            print(f"❌ 调用LLM时发生错误: {e}")
            return None

    def _parse_response(self, response_text):
        """解析LLM响应，支持多种格式和错误修复"""
        if not response_text:
            return None
        
        # 尝试多种解析策略
        parsing_strategies = [
            self._parse_direct_json,
            self._parse_json_from_markdown,
            self._parse_json_with_repair,
            self._parse_truncated_json
        ]
        
        for strategy in parsing_strategies:
            try:
                result = strategy(response_text)
                if result is not None:
                    print(f"✅ 使用 {strategy.__name__} 成功解析响应")
                    return result
            except Exception as e:
                print(f"⚠️  {strategy.__name__} 解析失败: {e}")
                continue
        
        print(f"❌ 所有解析策略都失败了")
        return None

    def _parse_direct_json(self, text):
        """直接解析JSON"""
        return json.loads(text.strip())

    def _parse_json_from_markdown(self, text):
        """从markdown代码块中提取JSON"""
        import re
        
        # 查找JSON代码块
        json_pattern = r'```(?:json)?\s*(\[.*?\])\s*```'
        match = re.search(json_pattern, text, re.DOTALL)
        
        if match:
            json_str = match.group(1)
            return json.loads(json_str)
        
        return None

    def _parse_json_with_repair(self, text):
        """尝试修复并解析JSON"""
        # 查找看起来像JSON的部分
        import re
        
        # 查找以[开头的部分
        start_idx = text.find('[')
        if start_idx == -1:
            return None
        
        # 从开始位置截取
        json_part = text[start_idx:]
        
        # 尝试找到匹配的结束括号
        bracket_count = 0
        end_idx = -1
        
        for i, char in enumerate(json_part):
            if char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end_idx = i + 1
                    break
        
        if end_idx > 0:
            json_str = json_part[:end_idx]
            return json.loads(json_str)
        
        return None

    def _parse_truncated_json(self, text):
        """处理截断的JSON响应"""
        # 查找JSON开始
        start_idx = text.find('[')
        if start_idx == -1:
            return None
        
        json_part = text[start_idx:]
        
        # 如果JSON看起来是截断的，尝试修复
        if not json_part.rstrip().endswith(']'):
            # 尝试添加缺失的结构
            fixed_attempts = [
                json_part + ']',
                json_part + '}]',
                json_part + '"}]}]'
            ]
            
            for attempt in fixed_attempts:
                try:
                    return json.loads(attempt)
                except:
                    continue
        
        # 如果没有修复，按原样解析
        return json.loads(json_part)

    def _analyze_relation_type(self, relation):
        """分析关系类型并更新统计"""
        if not relation:
            return
        
        self.relation_stats['total_count'] += 1
        
        if relation in config.RELATION_TYPES:
            # 预定义关系
            self.relation_stats['predefined'][relation] = self.relation_stats['predefined'].get(relation, 0) + 1
        else:
            # 自定义关系
            self.relation_stats['custom'][relation] = self.relation_stats['custom'].get(relation, 0) + 1

    def get_relation_statistics(self):
        """获取关系使用统计"""
        total = self.relation_stats['total_count']
        predefined_count = sum(self.relation_stats['predefined'].values())
        custom_count = sum(self.relation_stats['custom'].values())
        
        return {
            "total_triples": total,
            "predefined_relations": {
                "count": predefined_count,
                "percentage": (predefined_count / total * 100) if total > 0 else 0,
                "relations": self.relation_stats['predefined']
            },
            "custom_relations": {
                "count": custom_count,
                "percentage": (custom_count / total * 100) if total > 0 else 0,
                "relations": self.relation_stats['custom']
            }
        }

    def print_relation_report(self):
        """打印关系使用报告"""
        stats = self.get_relation_statistics()
        
        print("=" * 60)
        print("                    关系类型使用报告")
        print("=" * 60)
        
        total = stats['total_triples']
        predefined = stats['predefined_relations']
        custom = stats['custom_relations']
        
        print(f"总三元组数量: {total}")
        
        print(f"📋 预定义关系使用情况:")
        print(f"   数量: {predefined['count']} ({predefined['percentage']:.1f}%)")
        if predefined['relations']:
            print(f"   使用频率排序:")
            sorted_pred = sorted(predefined['relations'].items(), key=lambda x: x[1], reverse=True)
            for rel, count in sorted_pred:
                percentage = (count / total * 100) if total > 0 else 0
                print(f"     • {rel}: {count}次 ({percentage:.1f}%)")
        
        print(f"🆕 自定义关系使用情况:")
        print(f"   数量: {custom['count']} ({custom['percentage']:.1f}%)")
        if custom['relations']:
            print(f"   发现的新关系:")
            sorted_custom = sorted(custom['relations'].items(), key=lambda x: x[1], reverse=True)
            for rel, count in sorted_custom:
                percentage = (count / total * 100) if total > 0 else 0
                print(f"     • {rel}: {count}次 ({percentage:.1f}%)")
        
        print("=" * 60)

    def save_session_summary(self):
        """保存当前会话的汇总信息"""
        if not self.save_triples:
            return
        
        summary = {
            "session_timestamp": datetime.now().isoformat(),
            "total_triples_extracted": self.relation_stats['total_count'],
            "relation_statistics": self.get_relation_statistics(),
            "session_file": self.current_session_file
        }
        
        summary_file = os.path.join(self.triples_save_dir, "session_summary.json")
        
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            print(f"📊 已保存会话汇总: {os.path.basename(summary_file)}")
        except Exception as e:
            print(f"⚠️  保存会话汇总失败: {e}")

    def load_previous_session_stats(self, session_file):
        """加载之前会话的统计信息"""
        try:
            with open(session_file, 'r', encoding='utf-8') as f:
                for line in f:
                    entry = json.loads(line.strip())
                    if 'relation_stats' in entry:
                        prev_stats = entry['relation_stats']
                        
                        # 合并统计信息
                        for rel, count in prev_stats.get('predefined_relations', {}).get('relations', {}).items():
                            self.relation_stats['predefined'][rel] = self.relation_stats['predefined'].get(rel, 0) + count
                        
                        for rel, count in prev_stats.get('custom_relations', {}).get('relations', {}).items():
                            self.relation_stats['custom'][rel] = self.relation_stats['custom'].get(rel, 0) + count
                        
                        self.relation_stats['total_count'] += prev_stats.get('total_triples', 0)
            
            print(f"✅ 已加载之前会话的统计信息")
            
        except Exception as e:
            print(f"⚠️  加载之前会话统计失败: {e}")
