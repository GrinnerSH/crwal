import json
import random
import requests
from config import config
from graph_module.fact_extractor import LLMExtractor

class QuestionGenerator:
    def __init__(self):
        self.llm_extractor = LLMExtractor()

    def _batch_llm_call(self, prompt):
        """使用LLMExtractor调用LLM API"""
        try:
            # 创建一个文本块字典，符合LLMExtractor.extract_triples_batch方法的参数要求
            text_block = {'text': prompt, 'url': 'question_generation'}
            
            # 检查API密钥是否为默认值
            if self.llm_extractor.api_key == "your-api-key-value":
                print("警告: 使用的是默认API密钥。请在环境变量中设置正确的API_KEY。")
            
            # 调用360.cn API的正确格式 (根据API文档调整)
            payload = {
                "model": config.LLM_MODEL_NAME,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": config.LLM_TEMPERATURE,  # 使用配置文件中的温度
                "max_tokens": config.LLM_MAX_TOKENS     # 添加最大tokens参数
            }
            
            # 打印请求信息(调试用，生产环境可以移除)
            print(f"API URL: {self.llm_extractor.api_url}")
            print(f"请求模型: {config.LLM_MODEL_NAME}")
            
            headers = {'Authorization': f'Bearer {self.llm_extractor.api_key}', 'Content-Type': 'application/json'}
            
            response = requests.post(self.llm_extractor.api_url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            
            # 解析API响应
            response_json = response.json()
            if 'choices' in response_json and len(response_json['choices']) > 0:
                content = response_json['choices'][0]['message']['content']
                return content
            else:
                print(f"API响应格式不符合预期: {response_json}")
                return ""
        except requests.exceptions.RequestException as e:
            print(f"LLM API调用失败: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"响应内容: {e.response.text}")
                print(f"响应状态码: {e.response.status_code}")
            return ""
        except Exception as e:
            print(f"LLM API处理时发生错误: {e}")
            return ""
            
    def _call_llm_for_verbalization(self, path_samples):
        """调用LLM将路径转换为自然语言表述"""
        verbalization_prompt = "你是一个路径转述机器人。请将以下每个结构化路径转换为一个简单的、连贯的自然语言句子。\n\n"
        for i, sample in enumerate(path_samples):
            path_str = " -> ".join([f"({t['subject']},{t['relation']},{t['object']})" for t in sample['path']])
            verbalization_prompt += f"路径 {i+1}: {path_str}\n"
        verbalization_prompt += "\n请按格式返回: \n句子 1: [转换后的句子]\n句子 2: [转换后的句子]\n..."
        
        response = self._batch_llm_call(verbalization_prompt)
        
        # 解析结果
        verbalized_paths = []
        for line in response.split('\n'):
            if line.startswith('句子 ') and ':' in line:
                verbalized_paths.append(line.split(':', 1)[1].strip())
        
        return verbalized_paths
    
    def _call_llm_for_complex_narrative(self, path_samples):
        """调用LLM生成复杂叙事"""
        obfuscation_prompt = "你是一个语言混淆专家。请使用随机的混淆技巧（如时间模糊化、身份替代、复杂从句）重写以下句子，使其更复杂、更隐晦。每个句子应该有不同的表达方式。\n\n"
        for i, sample in enumerate(path_samples):
            obfuscation_prompt += f"句子 {i+1}: {sample['simple_statement']}\n"
        obfuscation_prompt += "\n请按格式返回: \n复杂句 1: [混淆后的句子]\n复杂句 2: [混淆后的句子]\n..."
        
        response = self._batch_llm_call(obfuscation_prompt)
        
        # 解析结果
        complex_narratives = []
        for line in response.split('\n'):
            if line.startswith('复杂句 ') and ':' in line:
                complex_narratives.append(line.split(':', 1)[1].strip())
        
        return complex_narratives
    
    def _call_llm_for_question_generation(self, path_samples):
        """调用LLM生成最终问题"""
        formulation_prompt = "你是一个问题构建专家。请根据提供的复杂叙事和目标实体，构建一个完整、通顺的多跳问句。问题应直接询问目标实体的某个属性。每个问题应该有不同的表达方式。\n\n"
        for i, sample in enumerate(path_samples):
            formulation_prompt += f"叙事 {i+1}: {sample['complex_narrative']}\n"
            formulation_prompt += f"目标实体 {i+1}: {sample['target_entity']}\n"
            formulation_prompt += f"问题类型 {i+1}: {sample['question_payload_type']}\n"
        formulation_prompt += "\n请按JSON格式返回问题和答案列表，格式如下: \n[\n{\"question\": \"问题1\", \"answer\": \"答案1\"},\n{\"question\": \"问题2\", \"answer\": \"答案2\"},\n...]\n"
        
        response = self._batch_llm_call(formulation_prompt)
        
        # 解析JSON结果
        try:
            # 找到JSON数据的起始和结束位置
            start_idx = response.find('[')
            end_idx = response.rfind(']') + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response[start_idx:end_idx]
                final_questions = json.loads(json_str)
                return final_questions
            else:
                print("无法在LLM响应中找到有效的JSON格式")
                return []
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            return []

    def generate_questions_cascade(self, path_samples):
        """
        执行完整的三阶段问题生成级联。
        path_samples: 一个字典列表，每个字典是一个处理单元。
        """
        # --- 阶段一: 批量路径言语化 ---
        verbalization_prompt = "你是一个路径转述机器人。请将以下每个结构化路径转换为一个简单的、连贯的自然语言句子。\n\n"
        for i, sample in enumerate(path_samples):
            path_str = " -> ".join([f"({t['subject']},{t['relation']},{t['object']})" for t in sample['path']])
            verbalization_prompt += f"路径 {i+1}: {path_str}\n"
        verbalization_prompt += "\n请按格式返回: \n句子 1: [转换后的句子]\n句子 2: [转换后的句子]\n..."
        
        # 此处应有解析LLM返回结果的逻辑，然后填充到sample中
        # for i, sample in enumerate(path_samples):
        #     sample['simple_statement'] = parsed_statements[i]

        # --- 阶段二: 批量约束编织与语言混淆 ---
        obfuscation_prompt = "你是一个语言混淆专家。请使用随机的混淆技巧（如时间模糊化、身份替代、复杂从句）重写以下句子，使其更复杂、更隐晦。\n\n"
        # for i, sample in enumerate(path_samples):
        #     obfuscation_prompt += f"句子 {i+1}: {sample['simple_statement']}\n"
        #... 调用LLM并填充 sample['complex_narrative']

        # --- 阶段三: 批量问题构建与负载附加 ---
        formulation_prompt = "你是一个问题构建专家。请根据提供的复杂叙事、目标实体和问题类型，构建一个完整、通顺的多跳问句。\n\n"
        # for i, sample in enumerate(path_samples):
        #     formulation_prompt += f"叙事 {i+1}: {sample['complex_narrative']}\n"
        #     formulation_prompt += f"目标 {i+1}: {sample['target_entity']}\n"
        #     formulation_prompt += f"问题类型 {i+1}: {sample['question_payload_type']}\n"
        #... 调用LLM并填充 sample['final_question'] 和 sample['answer'] (答案即目标实体)

        # --- 阶段一: 实现路径言语化 ---
        verbalized_paths = self._call_llm_for_verbalization(path_samples)
        for i, sample in enumerate(path_samples):
            sample['simple_statement'] = verbalized_paths[i] if i < len(verbalized_paths) else f"关于{sample['target_entity']}的描述"
        
        # --- 阶段二: 实现复杂叙事生成 ---
        complex_narratives = self._call_llm_for_complex_narrative(path_samples)
        for i, sample in enumerate(path_samples):
            sample['complex_narrative'] = complex_narratives[i] if i < len(complex_narratives) else f"描述{sample['target_entity']}的复杂叙事"
        
        # --- 阶段三: 实现最终问题构建 ---
        final_questions = self._call_llm_for_question_generation(path_samples)
        for i, sample in enumerate(path_samples):
            if i < len(final_questions):
                sample['final_question'] = final_questions[i]['question']
                sample['answer'] = final_questions[i].get('answer', sample['target_entity'])
            else:
                sample['final_question'] = f"根据所给信息，{sample['target_entity']}的{sample['question_payload_type']}是什么？"
                sample['answer'] = sample['target_entity']
                
        return path_samples