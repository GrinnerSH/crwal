# # import json
# # import requests
# # import time
# # from config import config

# # class LLMExtractor:
# #     def __init__(self):
# #         self.api_key = config.LLM_API_KEY
# #         self.api_url = config.LLM_API_BASE_URL
# #         # 用于追踪关系类型使用情况
# #         self.relation_stats = {
# #             'predefined': {},  # 预定义关系的使用次数
# #             'custom': {},      # 自定义关系的使用次数
# #             'total_count': 0   # 总三元组数量
# #         }

# #     def _build_prompt(self, text_blocks):
# #         """构建用于批量抽取的完整提示"""
        
# #         # 定义实体和关系模式
# #         schema_definition = f"""
# #         # 实体类型定义:
# #         {json.dumps(config.ENTITY_TYPES, indent=2)}

# #         # 关系类型定义:
# #         {json.dumps(config.RELATION_TYPES, indent=2)}
# #         """

# #         # 上下文学习示例
# #         in_context_examples = """
# #         # 示例:
# #         ## 输入文本块:
# #         1. "这部电影由克里斯托弗·诺兰执导,于2010年上映。"
# #         2. "玛丽·居里出生于华沙,后来移居巴黎。她发现了镭元素。"
        
# #         ## 输出JSON:
# #         [
# #             {
# #                 "block_id": 1,
# #                 "triples": [
# #                     {"subject": "这部电影", "relation": "导演", "object": "克里斯托弗·诺兰"},
# #                     {"subject": "这部电影", "relation": "发生年份", "object": "2010"}
# #                 ]
# #             },
# #             {
# #                 "block_id": 2,
# #                 "triples": [
# #                     {"subject": "玛丽·居里", "relation": "出生地", "object": "华沙"},
# #                     {"subject": "玛丽·居里", "relation": "位于", "object": "巴黎"},
# #                     {"subject": "玛丽·居里", "relation": "发现", "object": "镭元素"}
# #                 ]
# #             }
# #         ]
        
# #         注意：每个triple都必须有完整的subject, relation, object字段！
# #         关系可以使用预定义列表中的，也可以根据文本内容使用合适的中文关系词。
# #         """

# #         # 构建待处理的文本块列表
# #         formatted_blocks = ""
# #         for i, block in enumerate(text_blocks):
# #             # 清理文本中的JSON特殊字符，防止注入
# #             clean_text = json.dumps(block['text'], ensure_ascii=False)
# #             formatted_blocks += f'{i+1}. {clean_text}\n'

# #         # 组合成最终提示
# #         prompt = f"""
# #         你是一名知识抽取专家。请根据下面提供的模式定义和示例，分析接下来编号的文本块，并抽取出所有符合模式的事实三元组。

# #         **重要要求：**
# #         1. 请只返回一个合法的JSON数组，不要包含任何其他文字说明或markdown标记
# #         2. JSON数组中的每个元素必须包含完整的 block_id 和 triples 字段
# #         3. 每个triple必须包含完整的 subject, relation, object 三个字段
# #         4. **关系(relation)必须使用中文，优先使用预定义的关系类型列表中的关系，但如果文本中存在其他有意义的关系，也可以使用新的中文关系名称**
# #         5. 如果一个文本块中没有可抽出的三元组，则返回空的 triples 列表: []
# #         6. 确保JSON格式严格正确，所有字符串都用双引号包围
# #         7. 不要在JSON中嵌套额外的对象
# #         8. 注意区分不同类型的实体，如"奥斯卡最佳男主角奖"和"奥斯卡最佳女主角奖"是完全不同的实体
# #         9. 关系名称应该简洁明确，表达实体间的语义关系

# #         {schema_definition}
# #         ---
# #         {in_context_examples}
# #         ---
# #         # 现在，请处理以下真实的文本块:
# #         {formatted_blocks}
        
# #         直接返回JSON数组（不要包含```json或```标记）：
# #         """
# #         return prompt

# #     def _handle_streaming_response(self, response):
# #         """处理流式响应，组装完整的响应内容"""
# #         complete_content = ""
        
# #         try:
# #             for line in response.iter_lines(decode_unicode=True):
# #                 if line:
# #                     # 跳过空行和注释行
# #                     if line.startswith('data: '):
# #                         data_str = line[6:]  # 移除 'data: ' 前缀
                        
# #                         # 检查是否是结束标记
# #                         if data_str.strip() == '[DONE]':
# #                             break
                            
# #                         try:
# #                             data = json.loads(data_str)
# #                             # 提取内容
# #                             if 'choices' in data and len(data['choices']) > 0:
# #                                 choice = data['choices'][0]
# #                                 if 'delta' in choice and 'content' in choice['delta']:
# #                                     complete_content += choice['delta']['content']
# #                                 elif 'message' in choice and 'content' in choice['message']:
# #                                     complete_content += choice['message']['content']
# #                         except json.JSONDecodeError:
# #                             # 忽略无法解析的行
# #                             continue
            
# #             return complete_content
# #         except Exception as e:
# #             print(f"Error processing streaming response: {e}")
# #             return ""

# #     def _handle_non_streaming_response(self, response):
# #         """处理非流式响应"""
# #         try:
# #             response_data = response.json()
# #             return response_data['choices'][0]['message']['content']
# #         except (KeyError, IndexError, json.JSONDecodeError) as e:
# #             print(f"Error parsing non-streaming response: {e}")
# #             return ""

# #     def _extract_json_from_response(self, response_content):
# #         """从响应中提取JSON数据，使用多种策略"""
        
# #         # 预处理：移除Markdown代码块标记
# #         content = response_content.strip()
# #         if content.startswith('```json'):
# #             content = content[7:]  # 移除 '```json'
# #         if content.startswith('```'):
# #             content = content[3:]   # 移除 '```'
# #         if content.endswith('```'):
# #             content = content[:-3]  # 移除结尾的 '```'
# #         content = content.strip()
        
# #         # 策略1: 直接尝试解析整个响应
# #         try:
# #             return json.loads(content)
# #         except json.JSONDecodeError:
# #             pass
        
# #         # 策略2: 查找JSON数组标记
# #         start_idx = content.find('[')
# #         end_idx = content.rfind(']')
        
# #         if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
# #             json_str = content[start_idx:end_idx + 1]
# #             try:
# #                 return json.loads(json_str)
# #             except json.JSONDecodeError as e:
# #                 print(f"JSON parse error with strategy 2: {e}")
# #                 print(f"Attempted to parse: {json_str[:200]}...")
                
# #                 # 尝试清理JSON字符串
# #                 cleaned_json = self._clean_json_string(json_str)
# #                 try:
# #                     return json.loads(cleaned_json)
# #                 except json.JSONDecodeError as e2:
# #                     print(f"JSON parse error after cleaning: {e2}")
# #                     print(f"Cleaned JSON: {cleaned_json[:200]}...")
                    
# #                     # 尝试修复JSON结构错误
# #                     fixed_json = self._fix_json_structure(cleaned_json)
# #                     if fixed_json:
# #                         try:
# #                             return json.loads(fixed_json)
# #                         except json.JSONDecodeError as e3:
# #                             print(f"JSON parse error after structure fix: {e3}")
        
# #         # 策略2.5: 处理可能被截断的JSON（没有找到完整的]）
# #         elif start_idx != -1:
# #             # JSON开始了但没有正确结束，可能被截断
# #             print("Detected truncated JSON, attempting to fix...")
# #             json_str = content[start_idx:]
# #             fixed_json = self._fix_truncated_json(json_str)
# #             try:
# #                 return json.loads(fixed_json)
# #             except json.JSONDecodeError as e:
# #                 print(f"JSON parse error after truncation fix: {e}")
        
# #         # 策略3: 查找多个可能的JSON块
# #         import re
# #         json_pattern = r'\[.*?\]'
# #         matches = re.findall(json_pattern, content, re.DOTALL)
        
# #         for match in matches:
# #             try:
# #                 cleaned_match = self._clean_json_string(match)
# #                 return json.loads(cleaned_match)
# #             except json.JSONDecodeError:
# #                 continue
        
# #         # 策略4: 逐行查找JSON
# #         lines = content.split('\n')
# #         json_lines = []
# #         in_json = False
        
# #         for line in lines:
# #             line = line.strip()
# #             if line.startswith('['):
# #                 in_json = True
# #                 json_lines = [line]
# #             elif in_json:
# #                 json_lines.append(line)
# #                 if line.endswith(']'):
# #                     try:
# #                         json_str = '\n'.join(json_lines)
# #                         cleaned_json = self._clean_json_string(json_str)
# #                         return json.loads(cleaned_json)
# #                     except json.JSONDecodeError:
# #                         continue
        
# #         print("All JSON extraction strategies failed")
# #         print(f"Response content: {response_content}")
        
# #         # 最后的回退策略：返回空的结构化数据
# #         print("Using fallback: creating empty response structure")
# #         return []

# #     def _clean_json_string(self, json_str):
# #         """清理JSON字符串中的常见格式错误"""
# #         # 移除尾随逗号
# #         import re
        
# #         # 移除对象中的尾随逗号
# #         json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        
# #         # 移除数组中的尾随逗号
# #         json_str = re.sub(r',(\s*\])', r'\1', json_str)
        
# #         return json_str
    
# #     def _fix_truncated_json(self, json_str):
# #         """修复被截断的JSON"""
# #         import re
        
# #         # 移除可能的Markdown标记
# #         json_str = json_str.strip()
# #         if json_str.startswith('```'):
# #             json_str = json_str[3:]
# #         if json_str.endswith('```'):
# #             json_str = json_str[:-3]
# #         json_str = json_str.strip()
        
# #         # 尝试找到最后一个完整的三元组
# #         # 使用正则表达式找到所有完整的三元组
# #         triple_pattern = r'\{"subject":\s*"[^"]*",\s*"relation":\s*"[^"]*",\s*"object":\s*"[^"]*"\}'
# #         complete_triples = re.findall(triple_pattern, json_str)
        
# #         if complete_triples:
# #             print(f"Found {len(complete_triples)} complete triples")
            
# #             # 重新构建JSON
# #             triples_str = ',\n            '.join(complete_triples)
            
# #             # 提取block_id（如果存在）
# #             block_id_match = re.search(r'"block_id":\s*(\d+)', json_str)
# #             block_id = block_id_match.group(1) if block_id_match else "1"
            
# #             fixed_json = f'''[
# #     {{
# #         "block_id": {block_id},
# #         "triples": [
# #             {triples_str}
# #         ]
# #     }}
# # ]'''
# #             return fixed_json
        
# #         # 如果找不到完整的三元组，尝试逐行解析
# #         lines = json_str.split('\n')
# #         valid_triples = []
# #         current_triple = {}
        
# #         for line in lines:
# #             line = line.strip().rstrip(',')
            
# #             # 提取subject
# #             subject_match = re.search(r'"subject":\s*"([^"]*)"', line)
# #             if subject_match:
# #                 current_triple['subject'] = subject_match.group(1)
            
# #             # 提取relation
# #             relation_match = re.search(r'"relation":\s*"([^"]*)"', line)
# #             if relation_match:
# #                 current_triple['relation'] = relation_match.group(1)
            
# #             # 提取object
# #             object_match = re.search(r'"object":\s*"([^"]*)"', line)
# #             if object_match:
# #                 current_triple['object'] = object_match.group(1)
                
# #                 # 如果三个字段都有了，添加到列表
# #                 if all(key in current_triple for key in ['subject', 'relation', 'object']):
# #                     valid_triples.append(current_triple.copy())
# #                     current_triple = {}
        
# #         if valid_triples:
# #             print(f"Extracted {len(valid_triples)} valid triples from lines")
            
# #             # 构建JSON
# #             triples_json = []
# #             for triple in valid_triples:
# #                 triple_str = f'{{"subject": "{triple["subject"]}", "relation": "{triple["relation"]}", "object": "{triple["object"]}"}}'
# #                 triples_json.append(triple_str)
            
# #             triples_str = ',\n            '.join(triples_json)
            
# #             fixed_json = f'''[
# #     {{
# #         "block_id": 1,
# #         "triples": [
# #             {triples_str}
# #         ]
# #     }}
# # ]'''
# #             return fixed_json
        
# #         # 如果所有方法都失败，返回空结构
# #         return '[]'
    
# #     def _fix_json_structure(self, json_str):
# #         """尝试修复JSON结构错误"""
# #         import re
        
# #         # 首先尝试解析并修复明显的结构错误
# #         lines = json_str.split('\n')
# #         fixed_lines = []
        
# #         for line in lines:
# #             line = line.strip()
# #             if not line:
# #                 continue
            
# #             # 修复缺少subject字段的triple
# #             if '"relation":' in line and '"object":' in line and '"subject":' not in line:
# #                 # 这可能是一个格式错误的triple，跳过它
# #                 print(f"Skipping malformed triple: {line}")
# #                 continue
            
# #             # 修复嵌套错误的JSON对象
# #             if '"object":' in line and '{"subject":' in line:
# #                 # 这表明object值被错误地嵌套了另一个对象
# #                 # 尝试提取正确的object值
# #                 match = re.search(r'"object":\s*"([^{]*)', line)
# #                 if match:
# #                     object_value = match.group(1).strip()
# #                     if object_value:
# #                         # 重构这一行
# #                         subject_match = re.search(r'"subject":\s*"([^"]*)"', line)
# #                         relation_match = re.search(r'"relation":\s*"([^"]*)"', line)
                        
# #                         if subject_match and relation_match:
# #                             new_line = f'{{"subject": "{subject_match.group(1)}", "relation": "{relation_match.group(1)}", "object": "{object_value}"}}'
# #                             fixed_lines.append(new_line)
# #                             continue
                
# #                 # 如果无法修复，跳过这一行
# #                 print(f"Skipping corrupted line: {line}")
# #                 continue
            
# #             fixed_lines.append(line)
        
# #         # 重新组装JSON
# #         fixed_json = '\n'.join(fixed_lines)
        
# #         # 确保JSON结构完整
# #         if fixed_json.count('[') != fixed_json.count(']'):
# #             bracket_diff = fixed_json.count('[') - fixed_json.count(']')
# #             if bracket_diff > 0:
# #                 fixed_json += ']' * bracket_diff
        
# #         if fixed_json.count('{') != fixed_json.count('}'):
# #             brace_diff = fixed_json.count('{') - fixed_json.count('}')
# #             if brace_diff > 0:
# #                 # 在最后添加缺失的括号
# #                 fixed_json = fixed_json.rstrip() + '}' * brace_diff
        
# #         return fixed_json
    
# #     def _create_fallback_response(self, text_blocks):
# #         """当JSON解析完全失败时，创建一个空的回退响应"""
# #         fallback = []
# #         for i, block in enumerate(text_blocks):
# #             fallback.append({
# #                 "block_id": i + 1,
# #                 "triples": []
# #             })
# #         return fallback

# #     def extract_triples_batch(self, text_blocks):
# #         """
# #         批量从文本块中抽取三元组。
# #         text_blocks: 一个字典列表, 每个字典包含 'text' 和 'url'。
# #         """
# #         if not text_blocks:
# #             return

# #         # 限制批量大小以避免token超限
# #         MAX_BATCH_SIZE = 3  # 每批最多处理3个文本块
# #         all_triples = []
        
# #         for i in range(0, len(text_blocks), MAX_BATCH_SIZE):
# #             batch = text_blocks[i:i + MAX_BATCH_SIZE]
# #             print(f"Processing batch {i//MAX_BATCH_SIZE + 1} with {len(batch)} text blocks")
            
# #             batch_triples = self._extract_batch_internal(batch)
# #             if batch_triples:
# #                 all_triples.extend(batch_triples)
        
# #         return all_triples
    
# #     def _extract_batch_internal(self, text_blocks):
# #         """
# #         内部方法：从文本块中抽取三元组。
# #         text_blocks: 一个字典列表, 每个字典包含 'text' 和 'url'。
# #         """
# #         if not text_blocks:
# #             return

# #         prompt = self._build_prompt(text_blocks)
        
# #         payload = {
# #             "model": config.LLM_MODEL_NAME,
# #             "messages": [{"role": "user", "content": prompt}],
# #             "stream": config.LLM_USE_STREAMING,
# #             "temperature": config.LLM_TEMPERATURE,
# #             "max_tokens": config.LLM_MAX_TOKENS,
# #             # 某些API支持强制JSON输出，这里模拟通用请求
# #             # "response_format": {"type": "json_object"} 
# #         }
# #         headers = {
# #             'Authorization': f'Bearer {self.api_key}',
# #             'Content-Type': 'application/json'
# #         }

# #         max_retries = 3
# #         for attempt in range(max_retries):
# #             try:
# #                 print(f"Making {'streaming' if config.LLM_USE_STREAMING else 'non-streaming'} API request...")
# #                 response = requests.post(self.api_url, headers=headers, data=json.dumps(payload), 
# #                                        timeout=180, stream=config.LLM_USE_STREAMING)
# #                 response.raise_for_status()
                
# #                 # 根据是否流式调用选择不同的响应处理方式
# #                 if config.LLM_USE_STREAMING:
# #                     response_content = self._handle_streaming_response(response)
# #                 else:
# #                     response_content = self._handle_non_streaming_response(response)
                
# #                 if not response_content:
# #                     print("Empty response content received")
# #                     if attempt < max_retries - 1:
# #                         time.sleep(10)
# #                         continue
# #                     else:
# #                         return []
                
# #                 print(f"Raw response content length: {len(response_content)} characters")
# #                 print(f"Response preview: {response_content[:200]}...")
                
# #                 # 尝试多种方法提取JSON
# #                 extracted_data = self._extract_json_from_response(response_content)
# #                 if extracted_data is None:
# #                     print("Failed to extract valid JSON from response")
# #                     if attempt < max_retries - 1:
# #                         time.sleep(10)
# #                         continue
# #                     else:
# #                         print("Using fallback: creating empty response")
# #                         extracted_data = self._create_fallback_response(text_blocks)
                
# #                 # 将源URL信息添加回结果，并分析关系类型
# #                 final_triples = []
# #                 for item in extracted_data:
# #                     block_id = item.get("block_id")
# #                     if block_id and 1 <= block_id <= len(text_blocks):
# #                         source_url = text_blocks[block_id - 1]['url']
# #                         for triple in item.get("triples", []):
# #                             triple['source_url'] = source_url
                            
# #                             # 分析关系类型
# #                             self._analyze_relation_type(triple.get('relation', ''))
                            
# #                             final_triples.append(triple)
                
# #                 print(f"Successfully extracted {len(final_triples)} triples")
# #                 return final_triples

# #             except requests.exceptions.HTTPError as e:
# #                 if e.response.status_code == 429:  # Too Many Requests
# #                     wait_time = (attempt + 1) * 60  # 指数退避：60s, 120s, 180s
# #                     print(f"Rate limit hit (429), waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
# #                     if attempt < max_retries - 1:
# #                         time.sleep(wait_time)
# #                         continue
# #                     else:
# #                         print(f"Max retries reached for rate limit. Skipping this batch.")
# #                         return []
# #                 else:
# #                     print(f"HTTP error during LLM extraction: {e}")
# #                     print(f"Response status code: {e.response.status_code}")
# #                     print(f"Response headers: {dict(e.response.headers)}")
# #                     print(f"Response content: {e.response.text}")
# #                     print(f"Request payload size: {len(json.dumps(payload))} characters")
# #                     print(f"Request headers: {headers}")
# #                     return []
# #             except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
# #                 print(f"LLM extraction failed on attempt {attempt + 1}: {e}")
# #                 if attempt < max_retries - 1:
# #                     time.sleep(10)  # 短暂等待后重试
# #                     continue
# #                 else:
# #                     return []
        
# #         return []
    
# #     def _analyze_relation_type(self, relation):
# #         """分析并统计关系类型的使用情况"""
# #         if not relation:
# #             return
        
# #         self.relation_stats['total_count'] += 1
        
# #         if relation in config.RELATION_TYPES:
# #             # 预定义关系
# #             if relation in self.relation_stats['predefined']:
# #                 self.relation_stats['predefined'][relation] += 1
# #             else:
# #                 self.relation_stats['predefined'][relation] = 1
# #         else:
# #             # 自定义关系
# #             if relation in self.relation_stats['custom']:
# #                 self.relation_stats['custom'][relation] += 1
# #             else:
# #                 self.relation_stats['custom'][relation] = 1
    
# #     def get_relation_statistics(self):
# #         """获取关系类型使用统计"""
# #         total_predefined = sum(self.relation_stats['predefined'].values())
# #         total_custom = sum(self.relation_stats['custom'].values())
# #         total = self.relation_stats['total_count']
        
# #         stats = {
# #             'total_triples': total,
# #             'predefined_relations': {
# #                 'count': total_predefined,
# #                 'percentage': (total_predefined / total * 100) if total > 0 else 0,
# #                 'relations': dict(sorted(self.relation_stats['predefined'].items(), 
# #                                        key=lambda x: x[1], reverse=True))
# #             },
# #             'custom_relations': {
# #                 'count': total_custom,
# #                 'percentage': (total_custom / total * 100) if total > 0 else 0,
# #                 'relations': dict(sorted(self.relation_stats['custom'].items(), 
# #                                        key=lambda x: x[1], reverse=True))
# #             }
# #         }
        
# #         return stats
    
# #     def print_relation_report(self):
# #         """打印关系类型使用报告"""
# #         stats = self.get_relation_statistics()
        
# #         print("\n" + "="*60)
# #         print("                    关系类型使用报告")
# #         print("="*60)
        
# #         print(f"总三元组数量: {stats['total_triples']}")
# #         print()
        
# #         # 预定义关系统计
# #         print(f"📋 预定义关系使用情况:")
# #         print(f"   数量: {stats['predefined_relations']['count']} " +
# #               f"({stats['predefined_relations']['percentage']:.1f}%)")
        
# #         if stats['predefined_relations']['relations']:
# #             print("   使用频率排序:")
# #             for relation, count in list(stats['predefined_relations']['relations'].items())[:10]:
# #                 percentage = (count / stats['total_triples'] * 100) if stats['total_triples'] > 0 else 0
# #                 print(f"     • {relation}: {count}次 ({percentage:.1f}%)")
            
# #             if len(stats['predefined_relations']['relations']) > 10:
# #                 print(f"     ... 还有 {len(stats['predefined_relations']['relations']) - 10} 个关系")
        
# #         print()
        
# #         # 自定义关系统计
# #         print(f"🆕 自定义关系使用情况:")
# #         print(f"   数量: {stats['custom_relations']['count']} " +
# #               f"({stats['custom_relations']['percentage']:.1f}%)")
        
# #         if stats['custom_relations']['relations']:
# #             print("   发现的新关系:")
# #             for relation, count in list(stats['custom_relations']['relations'].items())[:15]:
# #                 percentage = (count / stats['total_triples'] * 100) if stats['total_triples'] > 0 else 0
# #                 print(f"     • {relation}: {count}次 ({percentage:.1f}%)")
            
# #             if len(stats['custom_relations']['relations']) > 15:
# #                 print(f"     ... 还有 {len(stats['custom_relations']['relations']) - 15} 个关系")
        
# #         print("="*60)
    
# #     def extract_facts_batch(self, text_blocks):
# #         """
# #         新的方法名，与原有的extract_triples_batch保持兼容
# #         """
# #         return self.extract_triples_batch(text_blocks)
    
# #     def save_custom_relations_to_config(self, min_frequency=2):
# #         """
# #         将高频的自定义关系保存到配置文件中
# #         min_frequency: 最小出现频率，只有达到这个频率的关系才会被建议添加
# #         """
# #         stats = self.get_relation_statistics()
        
# #         # 找出高频的自定义关系
# #         high_freq_custom = {rel: count for rel, count in stats['custom_relations']['relations'].items() 
# #                            if count >= min_frequency}
        
# #         if high_freq_custom:
# #             print(f"\n发现 {len(high_freq_custom)} 个高频自定义关系（频率≥{min_frequency}）:")
# #             for relation, count in high_freq_custom.items():
# #                 print(f"  • {relation}: {count}次")
            
# #             print("\n建议将这些关系添加到配置文件的RELATION_TYPES列表中。")
            
# #             # 生成配置代码
# #             config_additions = []
# #             for relation in high_freq_custom.keys():
# #                 config_additions.append(f'    "{relation}",')
            
# #             print("\n可以添加到config.py中的代码片段:")
# #             print("```python")
# #             print("# 新发现的高频关系")
# #             for line in config_additions:
# #                 print(line)
# #             print("```")
            
# #             return list(high_freq_custom.keys())
# #         else:
# #             print(f"\n没有发现频率≥{min_frequency}的自定义关系。")
# #             return []
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
import json
import requests
import time
from config import config

class LLMExtractor:
    def __init__(self):
        self.api_key = config.LLM_API_KEY
        self.api_url = config.LLM_API_BASE_URL
        # 用于追踪关系类型使用情况
        self.relation_stats = {
            'predefined': {},  # 预定义关系的使用次数
            'custom': {},      # 自定义关系的使用次数
            'total_count': 0   # 总三元组数量
        }

    def _build_prompt(self, text_blocks):
        """构建用于批量抽取的完整提示"""
        
        # 定义实体和关系模式
        schema_definition = f"""
        # 实体类型定义:
        {json.dumps(config.ENTITY_TYPES, indent=2)}

        # 关系类型定义:
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
        
        注意：每个triple都必须有完整的subject, relation, object字段！
        关系可以使用预定义列表中的，也可以根据文本内容使用合适的中文关系词。
        """

        # 构建待处理的文本块列表
        formatted_blocks = ""
        for i, block in enumerate(text_blocks):
            # 清理文本中的JSON特殊字符，防止注入
            clean_text = json.dumps(block['text'], ensure_ascii=False)
            formatted_blocks += f'{i+1}. {clean_text}\n'

        # 组合成最终提示
        prompt = f"""
        你是一名知识抽取专家。请根据下面提供的模式定义和示例，分析接下来编号的文本块，并抽取出所有符合模式的事实三元组。

        **重要要求：**
        1. 请只返回一个合法的JSON数组，不要包含任何其他文字说明或markdown标记
        2. JSON数组中的每个元素必须包含完整的 block_id 和 triples 字段
        3. 每个triple必须包含完整的 subject, relation, object 三个字段
        4. **关系(relation)必须使用中文，优先使用预定义的关系类型列表中的关系，但如果文本中存在其他有意义的关系，也可以使用新的中文关系名称**
        5. 如果一个文本块中没有可抽出的三元组，则返回空的 triples 列表: []
        6. 确保JSON格式严格正确，所有字符串都用双引号包围
        7. 不要在JSON中嵌套额外的对象
        8. 注意区分不同类型的实体，如"奥斯卡最佳男主角奖"和"奥斯卡最佳女主角奖"是完全不同的实体
        9. 关系名称应该简洁明确，表达实体间的语义关系

        {schema_definition}
        ---
        {in_context_examples}
        ---
        # 现在，请处理以下真实的文本块:
        {formatted_blocks}
        
        直接返回JSON数组（不要包含```json或```标记）：
        """
        return prompt

    def _handle_streaming_response(self, response):
        """处理流式响应，组装完整的响应内容"""
        complete_content = ""
        
        try:
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    # 跳过空行和注释行
                    if line.startswith('data: '):
                        data_str = line[6:]  # 移除 'data: ' 前缀
                        
                        # 检查是否是结束标记
                        if data_str.strip() == '[DONE]':
                            break
                            
                        try:
                            data = json.loads(data_str)
                            # 提取内容
                            if 'choices' in data and len(data['choices']) > 0:
                                choice = data['choices'][0]
                                if 'delta' in choice and 'content' in choice['delta']:
                                    complete_content += choice['delta']['content']
                                elif 'message' in choice and 'content' in choice['message']:
                                    complete_content += choice['message']['content']
                        except json.JSONDecodeError:
                            # 忽略无法解析的行
                            continue
            
            return complete_content
        except Exception as e:
            print(f"Error processing streaming response: {e}")
            return ""

    def _handle_non_streaming_response(self, response):
        """处理非流式响应"""
        try:
            response_data = response.json()
            return response_data['choices'][0]['message']['content']
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            print(f"Error parsing non-streaming response: {e}")
            return ""

    def _extract_json_from_response(self, response_content):
        """从响应中提取JSON数据，使用多种策略"""
        
        # 预处理：移除Markdown代码块标记
        content = response_content.strip()
        if content.startswith('```json'):
            content = content[7:]  # 移除 '```json'
        if content.startswith('```'):
            content = content[3:]   # 移除 '```'
        if content.endswith('```'):
            content = content[:-3]  # 移除结尾的 '```'
        content = content.strip()
        
        # 策略1: 直接尝试解析整个响应
        try:
            parsed_data = json.loads(content)
            # 如果解析成功，检查数据格式
            if isinstance(parsed_data, list):
                return parsed_data
            elif isinstance(parsed_data, dict):
                # 如果是单个对象，包装成数组
                print("Warning: Received single object instead of array, wrapping in array")
                return [parsed_data]
            else:
                print(f"Warning: Unexpected JSON structure type: {type(parsed_data)}")
                return []
        except json.JSONDecodeError:
            pass
        
        # 策略2: 查找JSON数组标记
        start_idx = content.find('[')
        end_idx = content.rfind(']')
        
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_str = content[start_idx:end_idx + 1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"JSON parse error with strategy 2: {e}")
                print(f"Attempted to parse: {json_str[:200]}...")
                
                # 尝试清理JSON字符串
                cleaned_json = self._clean_json_string(json_str)
                try:
                    return json.loads(cleaned_json)
                except json.JSONDecodeError as e2:
                    print(f"JSON parse error after cleaning: {e2}")
                    print(f"Cleaned JSON: {cleaned_json[:200]}...")
                    
                    # 尝试修复JSON结构错误
                    fixed_json = self._fix_json_structure(cleaned_json)
                    if fixed_json:
                        try:
                            return json.loads(fixed_json)
                        except json.JSONDecodeError as e3:
                            print(f"JSON parse error after structure fix: {e3}")
        
        # 策略2.5: 处理可能被截断的JSON（没有找到完整的]）
        elif start_idx != -1:
            # JSON开始了但没有正确结束，可能被截断
            print("Detected truncated JSON, attempting to fix...")
            json_str = content[start_idx:]
            fixed_json = self._fix_truncated_json(json_str)
            try:
                return json.loads(fixed_json)
            except json.JSONDecodeError as e:
                print(f"JSON parse error after truncation fix: {e}")
        
        # 策略3: 查找多个可能的JSON块
        import re
        json_pattern = r'\[.*?\]'
        matches = re.findall(json_pattern, content, re.DOTALL)
        
        for match in matches:
            try:
                cleaned_match = self._clean_json_string(match)
                return json.loads(cleaned_match)
            except json.JSONDecodeError:
                continue
        
        # 策略4: 逐行查找JSON
        lines = content.split('\n')
        json_lines = []
        in_json = False
        
        for line in lines:
            line = line.strip()
            if line.startswith('['):
                in_json = True
                json_lines = [line]
            elif in_json:
                json_lines.append(line)
                if line.endswith(']'):
                    try:
                        json_str = '\n'.join(json_lines)
                        cleaned_json = self._clean_json_string(json_str)
                        return json.loads(cleaned_json)
                    except json.JSONDecodeError:
                        continue
        
        print("All JSON extraction strategies failed")
        print(f"Response content: {response_content}")
        
        # 最后的回退策略：返回空的结构化数据
        print("Using fallback: creating empty response structure")
        return []

    def _clean_json_string(self, json_str):
        """清理JSON字符串中的常见格式错误"""
        # 移除尾随逗号
        import re
        
        # 移除对象中的尾随逗号
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        
        # 移除数组中的尾随逗号
        json_str = re.sub(r',(\s*\])', r'\1', json_str)
        
        return json_str
    
    def _fix_truncated_json(self, json_str):
        """修复被截断的JSON"""
        import re
        
        # 移除可能的Markdown标记
        json_str = json_str.strip()
        if json_str.startswith('```'):
            json_str = json_str[3:]
        if json_str.endswith('```'):
            json_str = json_str[:-3]
        json_str = json_str.strip()
        
        # 尝试找到最后一个完整的三元组
        # 使用正则表达式找到所有完整的三元组
        triple_pattern = r'\{"subject":\s*"[^"]*",\s*"relation":\s*"[^"]*",\s*"object":\s*"[^"]*"\}'
        complete_triples = re.findall(triple_pattern, json_str)
        
        if complete_triples:
            print(f"Found {len(complete_triples)} complete triples")
            
            # 重新构建JSON
            triples_str = ',\n            '.join(complete_triples)
            
            # 提取block_id（如果存在）
            block_id_match = re.search(r'"block_id":\s*(\d+)', json_str)
            block_id = block_id_match.group(1) if block_id_match else "1"
            
            fixed_json = f'''[
    {{
        "block_id": {block_id},
        "triples": [
            {triples_str}
        ]
    }}
]'''
            return fixed_json
        
        # 如果找不到完整的三元组，尝试逐行解析
        lines = json_str.split('\n')
        valid_triples = []
        current_triple = {}
        
        for line in lines:
            line = line.strip().rstrip(',')
            
            # 提取subject
            subject_match = re.search(r'"subject":\s*"([^"]*)"', line)
            if subject_match:
                current_triple['subject'] = subject_match.group(1)
            
            # 提取relation
            relation_match = re.search(r'"relation":\s*"([^"]*)"', line)
            if relation_match:
                current_triple['relation'] = relation_match.group(1)
            
            # 提取object
            object_match = re.search(r'"object":\s*"([^"]*)"', line)
            if object_match:
                current_triple['object'] = object_match.group(1)
            
            # 检查当前三元组是否完整（包含所有必需字段）
            if all(key in current_triple and current_triple[key] for key in ['subject', 'relation', 'object']):
                valid_triples.append(current_triple.copy())
                current_triple = {}
            
            # 如果检测到新的三元组开始（可能是不同行），重置当前三元组
            if '{' in line and any(field in line for field in ['"subject":', '"relation":', '"object":']):
                current_triple = {}
                # 重新尝试提取这一行的字段
                subject_match = re.search(r'"subject":\s*"([^"]*)"', line)
                if subject_match:
                    current_triple['subject'] = subject_match.group(1)
                relation_match = re.search(r'"relation":\s*"([^"]*)"', line)
                if relation_match:
                    current_triple['relation'] = relation_match.group(1)
                object_match = re.search(r'"object":\s*"([^"]*)"', line)
                if object_match:
                    current_triple['object'] = object_match.group(1)
        
        if valid_triples:
            print(f"Extracted {len(valid_triples)} valid triples from lines")
            
            # 构建JSON
            triples_json = []
            for triple in valid_triples:
                triple_str = f'{{"subject": "{triple["subject"]}", "relation": "{triple["relation"]}", "object": "{triple["object"]}"}}'
                triples_json.append(triple_str)
            
            triples_str = ',\n            '.join(triples_json)
            
            fixed_json = f'''[
    {{
        "block_id": 1,
        "triples": [
            {triples_str}
        ]
    }}
]'''
            return fixed_json
        
        # 如果所有方法都失败，返回空结构
        return '[]'
    
    def _fix_json_structure(self, json_str):
        """尝试修复JSON结构错误"""
        import re
        
        # 首先尝试解析并修复明显的结构错误
        lines = json_str.split('\n')
        fixed_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 修复缺少subject字段的triple
            if '"relation":' in line and '"object":' in line and '"subject":' not in line:
                # 这可能是一个格式错误的triple，跳过它
                print(f"Skipping malformed triple: {line}")
                continue
            
            # 修复嵌套错误的JSON对象
            if '"object":' in line and '{"subject":' in line:
                # 这表明object值被错误地嵌套了另一个对象
                # 尝试提取正确的object值
                match = re.search(r'"object":\s*"([^{]*)', line)
                if match:
                    object_value = match.group(1).strip()
                    if object_value:
                        # 重构这一行
                        subject_match = re.search(r'"subject":\s*"([^"]*)"', line)
                        relation_match = re.search(r'"relation":\s*"([^"]*)"', line)
                        
                        if subject_match and relation_match:
                            new_line = f'{{"subject": "{subject_match.group(1)}", "relation": "{relation_match.group(1)}", "object": "{object_value}"}}'
                            fixed_lines.append(new_line)
                            continue
                
                # 如果无法修复，跳过这一行
                print(f"Skipping corrupted line: {line}")
                continue
            
            fixed_lines.append(line)
        
        # 重新组装JSON
        fixed_json = '\n'.join(fixed_lines)
        
        # 确保JSON结构完整
        if fixed_json.count('[') != fixed_json.count(']'):
            bracket_diff = fixed_json.count('[') - fixed_json.count(']')
            if bracket_diff > 0:
                fixed_json += ']' * bracket_diff
        
        if fixed_json.count('{') != fixed_json.count('}'):
            brace_diff = fixed_json.count('{') - fixed_json.count('}')
            if brace_diff > 0:
                # 在最后添加缺失的括号
                fixed_json = fixed_json.rstrip() + '}' * brace_diff
        
        return fixed_json
    
    def _create_fallback_response(self, text_blocks):
        """当JSON解析完全失败时，创建一个空的回退响应"""
        fallback = []
        for i, block in enumerate(text_blocks):
            fallback.append({
                "block_id": i + 1,
                "triples": []
            })
        return fallback

    def extract_triples_batch(self, text_blocks):
        """
        批量从文本块中抽取三元组。
        text_blocks: 一个字典列表, 每个字典包含 'text' 和 'url'。
        """
        if not text_blocks:
            return

        # 限制批量大小以避免token超限
        MAX_BATCH_SIZE = 3  # 每批最多处理3个文本块
        all_triples = []
        
        for i in range(0, len(text_blocks), MAX_BATCH_SIZE):
            batch = text_blocks[i:i + MAX_BATCH_SIZE]
            print(f"Processing batch {i//MAX_BATCH_SIZE + 1} with {len(batch)} text blocks")
            
            batch_triples = self._extract_batch_internal(batch)
            if batch_triples:
                all_triples.extend(batch_triples)
        
        return all_triples
    
    def _extract_batch_internal(self, text_blocks):
        """
        内部方法：从文本块中抽取三元组。
        text_blocks: 一个字典列表, 每个字典包含 'text' 和 'url'。
        """
        if not text_blocks:
            return

        prompt = self._build_prompt(text_blocks)
        
        payload = {
            "model": config.LLM_MODEL_NAME,
            "messages": [{"role": "user", "content": prompt}],
            "stream": config.LLM_USE_STREAMING,
            "temperature": config.LLM_TEMPERATURE,
            "max_tokens": config.LLM_MAX_TOKENS,
            # 某些API支持强制JSON输出，这里模拟通用请求
            # "response_format": {"type": "json_object"} 
        }
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Making {'streaming' if config.LLM_USE_STREAMING else 'non-streaming'} API request...")
                response = requests.post(self.api_url, headers=headers, data=json.dumps(payload), 
                                       timeout=180, stream=config.LLM_USE_STREAMING)
                response.raise_for_status()
                
                # 根据是否流式调用选择不同的响应处理方式
                if config.LLM_USE_STREAMING:
                    response_content = self._handle_streaming_response(response)
                else:
                    response_content = self._handle_non_streaming_response(response)
                
                if not response_content:
                    print("Empty response content received")
                    if attempt < max_retries - 1:
                        time.sleep(10)
                        continue
                    else:
                        return []
                
                print(f"Raw response content length: {len(response_content)} characters")
                print(f"Response preview: {response_content[:200]}...")
                
                # 尝试多种方法提取JSON
                extracted_data = self._extract_json_from_response(response_content)
                if extracted_data is None:
                    print("Failed to extract valid JSON from response")
                    if attempt < max_retries - 1:
                        time.sleep(10)
                        continue
                    else:
                        print("Using fallback: creating empty response")
                        extracted_data = self._create_fallback_response(text_blocks)
                
                # 确保extracted_data是正确的格式
                if not isinstance(extracted_data, list):
                    print(f"Warning: extracted_data is not a list, type: {type(extracted_data)}")
                    print(f"extracted_data content: {extracted_data}")
                    extracted_data = self._create_fallback_response(text_blocks)
                
                # 将源URL信息添加回结果，并分析关系类型
                final_triples = []
                for item in extracted_data:
                    if not isinstance(item, dict):
                        print(f"Warning: item is not a dict, type: {type(item)}, content: {item}")
                        continue
                    
                    block_id = item.get("block_id")
                    if block_id and 1 <= block_id <= len(text_blocks):
                        source_url = text_blocks[block_id - 1]['url']
                        for triple in item.get("triples", []):
                            triple['source_url'] = source_url
                            
                            # 分析关系类型
                            self._analyze_relation_type(triple.get('relation', ''))
                            
                            final_triples.append(triple)
                
                print(f"Successfully extracted {len(final_triples)} triples")
                return final_triples

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    wait_time = (attempt + 1) * 60  # 指数退避：60s, 120s, 180s
                    print(f"Rate limit hit (429), waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Max retries reached for rate limit. Skipping this batch.")
                        return []
                else:
                    print(f"HTTP error during LLM extraction: {e}")
                    print(f"Response status code: {e.response.status_code}")
                    print(f"Response headers: {dict(e.response.headers)}")
                    print(f"Response content: {e.response.text}")
                    print(f"Request payload size: {len(json.dumps(payload))} characters")
                    print(f"Request headers: {headers}")
                    return []
            except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"LLM extraction failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)  # 短暂等待后重试
                    continue
                else:
                    return []
        
        return []
    
    def _analyze_relation_type(self, relation):
        """分析并统计关系类型的使用情况"""
        if not relation:
            return
        
        self.relation_stats['total_count'] += 1
        
        if relation in config.RELATION_TYPES:
            # 预定义关系
            if relation in self.relation_stats['predefined']:
                self.relation_stats['predefined'][relation] += 1
            else:
                self.relation_stats['predefined'][relation] = 1
        else:
            # 自定义关系
            if relation in self.relation_stats['custom']:
                self.relation_stats['custom'][relation] += 1
            else:
                self.relation_stats['custom'][relation] = 1
    
    def get_relation_statistics(self):
        """获取关系类型使用统计"""
        total_predefined = sum(self.relation_stats['predefined'].values())
        total_custom = sum(self.relation_stats['custom'].values())
        total = self.relation_stats['total_count']
        
        stats = {
            'total_triples': total,
            'predefined_relations': {
                'count': total_predefined,
                'percentage': (total_predefined / total * 100) if total > 0 else 0,
                'relations': dict(sorted(self.relation_stats['predefined'].items(), 
                                       key=lambda x: x[1], reverse=True))
            },
            'custom_relations': {
                'count': total_custom,
                'percentage': (total_custom / total * 100) if total > 0 else 0,
                'relations': dict(sorted(self.relation_stats['custom'].items(), 
                                       key=lambda x: x[1], reverse=True))
            }
        }
        
        return stats
    
    def print_relation_report(self):
        """打印关系类型使用报告"""
        stats = self.get_relation_statistics()
        
        print("\n" + "="*60)
        print("                    关系类型使用报告")
        print("="*60)
        
        print(f"总三元组数量: {stats['total_triples']}")
        print()
        
        # 预定义关系统计
        print(f"📋 预定义关系使用情况:")
        print(f"   数量: {stats['predefined_relations']['count']} " +
              f"({stats['predefined_relations']['percentage']:.1f}%)")
        
        if stats['predefined_relations']['relations']:
            print("   使用频率排序:")
            for relation, count in list(stats['predefined_relations']['relations'].items())[:10]:
                percentage = (count / stats['total_triples'] * 100) if stats['total_triples'] > 0 else 0
                print(f"     • {relation}: {count}次 ({percentage:.1f}%)")
            
            if len(stats['predefined_relations']['relations']) > 10:
                print(f"     ... 还有 {len(stats['predefined_relations']['relations']) - 10} 个关系")
        
        print()
        
        # 自定义关系统计
        print(f"🆕 自定义关系使用情况:")
        print(f"   数量: {stats['custom_relations']['count']} " +
              f"({stats['custom_relations']['percentage']:.1f}%)")
        
        if stats['custom_relations']['relations']:
            print("   发现的新关系:")
            for relation, count in list(stats['custom_relations']['relations'].items())[:15]:
                percentage = (count / stats['total_triples'] * 100) if stats['total_triples'] > 0 else 0
                print(f"     • {relation}: {count}次 ({percentage:.1f}%)")
            
            if len(stats['custom_relations']['relations']) > 15:
                print(f"     ... 还有 {len(stats['custom_relations']['relations']) - 15} 个关系")
        
        print("="*60)
    
    def extract_facts_batch(self, text_blocks):
        """
        新的方法名，与原有的extract_triples_batch保持兼容
        """
        return self.extract_triples_batch(text_blocks)
    
    def save_custom_relations_to_config(self, min_frequency=2):
        """
        将高频的自定义关系保存到配置文件中
        min_frequency: 最小出现频率，只有达到这个频率的关系才会被建议添加
        """
        stats = self.get_relation_statistics()
        
        # 找出高频的自定义关系
        high_freq_custom = {rel: count for rel, count in stats['custom_relations']['relations'].items() 
                           if count >= min_frequency}
        
        if high_freq_custom:
            print(f"\n发现 {len(high_freq_custom)} 个高频自定义关系（频率≥{min_frequency}）:")
            for relation, count in high_freq_custom.items():
                print(f"  • {relation}: {count}次")
            
            print("\n建议将这些关系添加到配置文件的RELATION_TYPES列表中。")
            
            # 生成配置代码
            config_additions = []
            for relation in high_freq_custom.keys():
                config_additions.append(f'    "{relation}",')
            
            print("\n可以添加到config.py中的代码片段:")
            print("```python")
            print("# 新发现的高频关系")
            for line in config_additions:
                print(line)
            print("```")
            
            return list(high_freq_custom.keys())
        else:
            print(f"\n没有发现频率≥{min_frequency}的自定义关系。")
            return []