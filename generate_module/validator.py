import json
from config import config

def calculate_complexity(path, graph):
    """根据用户文档中的公式计算复杂度得分"""
    path_length = len(path)
    
    domains = set()
    for triple in path:
        subj_domain = graph.nodes[triple['subject']].get('type', 'Unknown')
        obj_domain = graph.nodes[triple['object']].get('type', 'Unknown')
        domains.add(subj_domain)
        domains.add(obj_domain)
    domain_jumps = len(domains) - 1 if len(domains) > 1 else 0
    
    # 约束特异性(S)在此简化为常数，实际需要更复杂的评估模型
    constraint_specificity_mean = 2.5 
    
    complexity = path_length * (1 + domain_jumps) * constraint_specificity_mean
    
    return {
        "path_length": path_length,
        "domain_jumps": domain_jumps,
        "constraint_specificity": constraint_specificity_mean,
        "overall": complexity
    }

def assemble_final_json(question_id, generated_sample, graph):
    """组装成最终的数据点格式"""
    path = generated_sample['path']
    
    # 构建证据片段
    supporting_evidence = []
    urls_in_path = set()
    for triple in path:
        # 假设三元组在添加时已附带source_url
        url = graph.get_edge_data(triple['subject'], triple['object'], key=triple['relation']).get('source_url')
        if url and url not in urls_in_path:
            supporting_evidence.append({"url": url, "text_snippet": "..."}) # 片段需要从原始HTML中提取
            urls_in_path.add(url)

    data_point = {
        "id": f"mq_id_{question_id:05d}",
        "question": generated_sample['final_question'],
        "answer": generated_sample['answer'],
        "complexity_scores": calculate_complexity(path, graph),
        "reasoning_architecture": generated_sample['architecture'],
        "reasoning_trajectory": path,
        "supporting_evidence": supporting_evidence,
        "validation_flags": {
            "is_factually_grounded": True, # 初始假设为真
            "passed_solver_llm": False,
            "passed_human_review": False
        }
    }
    return data_point