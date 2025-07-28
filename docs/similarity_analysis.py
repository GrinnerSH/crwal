#!/usr/bin/env python3
"""
深入分析Jaro-Winkler相似度计算问题
"""

from jarowinkler import jarowinkler_similarity

def test_similarity_calculations():
    """测试相似度计算的详细情况"""
    
    print("=" * 70)
    print("            Jaro-Winkler相似度计算深度分析")
    print("=" * 70)
    
    test_cases = [
        ('历史', '对过去的系统研究'),
        ('人工智能', 'AI'),
        ('人工智能', '人工智慧'),  # 简体vs繁体
        ('约翰·麦卡锡', '约翰 麦卡锡'),
        ('约翰·麦卡锡', 'John McCarthy'),
        ('深度学习', '深度学习技术'),
        ('特斯拉公司', '特斯拉'),
        ('特斯拉', 'Tesla'),
        ('北京大学', '北大'),
        ('清华大学', '清华'),
        ('机器学习', 'Machine Learning'),
        ('神经网络', '神经网络模型')
    ]
    
    print(f"\n📊 详细相似度计算结果:")
    print(f"{'实体1':<15} {'实体2':<15} {'相似度':<8} {'字符分析'}")
    print("-" * 70)
    
    for name1, name2 in test_cases:
        similarity = jarowinkler_similarity(name1, name2)
        
        # 字符分析
        len1, len2 = len(name1), len(name2)
        common_chars = set(name1) & set(name2)
        
        analysis = f"长度:{len1}/{len2}, 共同字符:{len(common_chars)}"
        
        print(f"{name1:<15} {name2:<15} {similarity:<8.4f} {analysis}")

def analyze_chinese_similarity_issues():
    """分析中文相似度计算的特殊问题"""
    
    print(f"\n" + "=" * 70)
    print("                 中文相似度计算问题分析")
    print("=" * 70)
    
    print(f"\n🔍 问题分析:")
    
    # 测试中英文混合的问题
    mixed_cases = [
        ('AI', '人工智能'),
        ('Tesla', '特斯拉'),
        ('MIT', '麻省理工学院'),
        ('GDP', '国内生产总值'),
        ('CPU', '中央处理器')
    ]
    
    print(f"\n📋 中英文混合匹配测试:")
    for en, cn in mixed_cases:
        sim = jarowinkler_similarity(en, cn)
        print(f"   '{en}' <-> '{cn}': {sim:.4f}")
    
    print(f"\n💡 发现的问题:")
    print(f"   1. 中英文字符完全不同，导致相似度为0")
    print(f"   2. 缩写与全称之间没有字符级别的相似性")
    print(f"   3. Jaro-Winkler算法主要适用于拼写错误修正，不适合语义相似性")
    
    # 测试中文同义词
    print(f"\n📋 中文同义词测试:")
    synonym_cases = [
        ('电脑', '计算机'),
        ('手机', '移动电话'),
        ('汽车', '机动车'),
        ('大学', '高等学校'),
        ('公司', '企业')
    ]
    
    for word1, word2 in synonym_cases:
        sim = jarowinkler_similarity(word1, word2)
        print(f"   '{word1}' <-> '{word2}': {sim:.4f}")

def suggest_improvements():
    """建议改进方案"""
    
    print(f"\n" + "=" * 70)
    print("                   改进建议")
    print("=" * 70)
    
    print(f"\n🎯 当前实体对齐策略的问题:")
    print(f"   1. 过度依赖字符级相似度")
    print(f"   2. 无法处理中英文对应关系")
    print(f"   3. 无法识别语义相似但字符不同的实体")
    print(f"   4. 阈值0.95过于严格，连明显的变体都被拒绝")
    
    print(f"\n💡 改进方案:")
    print(f"   方案1: 降低阈值到0.85-0.9")
    print(f"   方案2: 增加中英文映射词典")
    print(f"   方案3: 增加语义相似度计算")
    print(f"   方案4: 基于实体类型的不同处理策略")
    
    print(f"\n🔧 推荐的阈值调整:")
    thresholds = [0.85, 0.9, 0.95]
    test_cases = [
        ('约翰·麦卡锡', '约翰 麦卡锡', 0.9111),
        ('深度学习', '深度学习技术', 0.9333),
        ('特斯拉公司', '特斯拉', 0.9067),
        ('北京大学', '北大', 0.5556),
        ('清华大学', '清华', 0.5556)
    ]
    
    print(f"\n   {'实体对':<25} {'相似度':<8} " + " ".join(f"阈值{t:<5}" for t in thresholds))
    print("   " + "-" * 60)
    
    for name1, name2, sim in test_cases:
        results = []
        for threshold in thresholds:
            results.append("✅" if sim >= threshold else "❌")
        
        pair_name = f"'{name1}' <-> '{name2}'"
        if len(pair_name) > 23:
            pair_name = pair_name[:20] + "..."
        
        print(f"   {pair_name:<25} {sim:<8.4f} " + " ".join(f"{r:<8}" for r in results))
    
    print(f"\n✅ 建议:")
    print(f"   推荐阈值: 0.9 (平衡准确率和召回率)")
    print(f"   这将允许明显的变体匹配，同时避免错误合并")

if __name__ == "__main__":
    test_similarity_calculations()
    analyze_chinese_similarity_issues()
    suggest_improvements()
    
    print(f"\n🎉 分析完成！")
