#!/usr/bin/env python3
"""
增强版知识图谱构建示例 - 支持关系类型扩展
"""

import sys
import os
# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import os
import json
from config import config
from graph_module.poster import parse_html_to_text_blocks
from graph_module.fact_extractor import LLMExtractor
from graph_module.graph_manager import GraphManager

def enhanced_kg_build_demo():
    """演示增强版知识图谱构建流程"""
    
    print("=== 增强版知识图谱构建演示 ===\n")
    
    # 创建一些模拟的HTML内容和处理结果
    mock_text_blocks = [
        {
            'text': '人工智能是由人造机器所展现的智慧。约翰·麦卡锡在1955年定义人工智能为制造智能机器的科学与工程。',
            'url': 'demo://ai_intro.html'
        },
        {
            'text': '深度学习是机器学习的一个分支，它模仿人脑神经网络的结构。Geoffrey Hinton被誉为深度学习之父。',
            'url': 'demo://deep_learning.html'
        },
        {
            'text': '特斯拉公司由埃隆·马斯克领导，专注于电动汽车和清洁能源技术的研发。该公司推出了Model S等创新产品。',
            'url': 'demo://tesla.html'
        }
    ]
    
    print(f"1. 模拟处理 {len(mock_text_blocks)} 个文本块...")
    for i, block in enumerate(mock_text_blocks, 1):
        print(f"   块 {i}: {block['text'][:50]}...")
    
    # 2. 创建提取器并处理
    print(f"\n2. 使用LLM提取器处理文本块...")
    extractor = LLMExtractor()
    
    # 模拟API响应结果（包含预定义和自定义关系）
    mock_extracted_data = [
        {
            "block_id": 1,
            "triples": [
                {"subject": "人工智能", "relation": "定义", "object": "由人造机器所展现的智慧"},
                {"subject": "约翰·麦卡锡", "relation": "定义", "object": "人工智能为制造智能机器的科学与工程"},
                {"subject": "约翰·麦卡锡", "relation": "提出", "object": "人工智能概念"},
                {"subject": "人工智能", "relation": "涉及", "object": "智能机器制造"}
            ]
        },
        {
            "block_id": 2,
            "triples": [
                {"subject": "深度学习", "relation": "属于", "object": "机器学习"},
                {"subject": "深度学习", "relation": "模仿", "object": "人脑神经网络结构"},
                {"subject": "Geoffrey Hinton", "relation": "被誉为", "object": "深度学习之父"},
                {"subject": "Geoffrey Hinton", "relation": "贡献", "object": "深度学习发展"}
            ]
        },
        {
            "block_id": 3,
            "triples": [
                {"subject": "特斯拉公司", "relation": "创立者", "object": "埃隆·马斯克"},
                {"subject": "特斯拉公司", "relation": "专注于", "object": "电动汽车技术"},
                {"subject": "特斯拉公司", "relation": "专注于", "object": "清洁能源技术"},
                {"subject": "特斯拉公司", "relation": "开发者", "object": "Model S"},
                {"subject": "Model S", "relation": "类型", "object": "创新产品"}
            ]
        }
    ]
    
    # 处理提取结果
    all_triples = []
    for item in mock_extracted_data:
        block_id = item.get("block_id")
        if block_id and 1 <= block_id <= len(mock_text_blocks):
            source_url = mock_text_blocks[block_id - 1]['url']
            for triple in item.get("triples", []):
                triple['source_url'] = source_url
                # 分析关系类型
                extractor._analyze_relation_type(triple.get('relation', ''))
                all_triples.append(triple)
    
    print(f"   成功提取 {len(all_triples)} 个三元组")
    
    # 3. 构建知识图谱
    print(f"\n3. 构建知识图谱...")
    graph_manager = GraphManager()
    
    # 添加三元组到图中
    new_nodes = graph_manager.add_triples(all_triples)
    print(f"   添加了 {len(new_nodes)} 个新节点")
    
    # 执行实体对齐
    if new_nodes:
        print(f"   执行实体对齐...")
        graph_manager.resolve_entities_incremental(new_nodes)
    
    # 显示图谱统计
    print(f"   最终图谱: {len(graph_manager.G.nodes)} 个节点, {len(graph_manager.G.edges)} 条边")
    
    # 4. 显示关系类型分析
    print(f"\n4. 关系类型使用分析...")
    extractor.print_relation_report()
    
    # 5. 识别和建议新关系
    print(f"\n5. 识别高频自定义关系...")
    high_freq_relations = extractor.save_custom_relations_to_config(min_frequency=1)
    
    # 6. 显示一些具体的三元组示例
    print(f"\n6. 三元组示例:")
    for i, triple in enumerate(all_triples[:8], 1):
        relation_type = "预定义" if triple['relation'] in config.RELATION_TYPES else "自定义"
        print(f"   {i:2d}. {triple['subject']} --{triple['relation']}({relation_type})--> {triple['object']}")
    
    if len(all_triples) > 8:
        print(f"       ... 还有 {len(all_triples) - 8} 个三元组")
    
    # 7. 保存结果
    print(f"\n7. 保存知识图谱...")
    graph_manager.save_graph()
    print(f"   图谱已保存到: {config.GRAPH_STORE_PATH}")
    
    # 8. 生成关系使用报告文件
    stats = extractor.get_relation_statistics()
    report_file = os.path.join(config.RAW_HTML_DIR, "..", "relation_usage_report.json")
    
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        print(f"   关系使用报告已保存到: {report_file}")
    except Exception as e:
        print(f"   保存报告失败: {e}")
    
    print(f"\n" + "="*60)
    print("增强版知识图谱构建完成！")
    print("新功能:")
    print("  ✓ 支持预定义和自定义关系类型")
    print("  ✓ 关系使用统计和分析")
    print("  ✓ 高频关系识别和建议")
    print("  ✓ 详细的关系类型报告")
    print("="*60)
    
    return graph_manager, extractor, all_triples

def show_relation_recommendations():
    """显示关系类型扩展建议"""
    
    print("\n" + "="*60)
    print("                   关系类型扩展建议")
    print("="*60)
    
    print("根据测试结果，建议将以下关系添加到config.py的RELATION_TYPES中:")
    print()
    
    suggested_relations = [
        "提出", "改变", "移居", "发明", "应用于", "证明",
        "属于", "模仿", "被誉为", "专注于", "类型", "涉及"
    ]
    
    print("# 建议添加的关系类型")
    for relation in suggested_relations:
        print(f'    "{relation}",')
    
    print()
    print("这些关系具有以下特点:")
    print("  • 语义明确，表达清晰的实体关系")
    print("  • 在多个领域中都有使用价值")  
    print("  • 符合中文表达习惯")
    print("  • 可以补充现有预定义关系的不足")
    print("="*60)

if __name__ == "__main__":
    # 运行演示
    graph_manager, extractor, triples = enhanced_kg_build_demo()
    
    # 显示建议
    show_relation_recommendations()
    
    print(f"\n🎉 演示完成！关系类型扩展功能已成功集成到知识图谱构建流程中。")
