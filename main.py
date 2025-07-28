# import os
# import argparse
# import json
# import shutil
# from datetime import datetime
# from config import config
# from scraper_module.scraper import Scraper
# from graph_module.poster import parse_html_to_text_blocks
# from graph_module.fact_extractor import LLMExtractor
# from graph_module.graph_manager import GraphManager
# from generate_module.path_sampler import PathSampler
# from generate_module.question_generator import QuestionGenerator
# from generate_module.validator import assemble_final_json

# class RealTimeGraphManager(GraphManager):
    
#     def __init__(self, graph_path=None, auto_save=True, save_frequency=1):
#         """
#         初始化图谱管理器
        
#         Args:
#             graph_path: 图谱文件路径
#             auto_save: 是否启用自动保存
#             save_frequency: 保存频率（处理多少个文件后保存一次）
#         """
#         super().__init__(graph_path)
        
#         self.auto_save = auto_save
#         self.save_frequency = save_frequency
#         self.processed_files = 0
        
#         # 设置文件路径
#         self.graph_path = graph_path or config.GRAPH_STORE_PATH
#         self.backup_path = self.graph_path.replace('.gpickle', '_backup.gpickle')
#         self.triples_log_path = os.path.join(
#             os.path.dirname(self.graph_path), 
#             "triples_log.jsonl"
#         )
#         self.progress_report_path = os.path.join(
#             os.path.dirname(self.graph_path),
#             "progress_report.json"
#         )
        
#         print(f"✅ 实时保存图谱管理器初始化完成")
#         print(f"   主图谱文件: {os.path.basename(self.graph_path)}")
#         print(f"   备份文件: {os.path.basename(self.backup_path)}")
#         print(f"   三元组日志: {os.path.basename(self.triples_log_path)}")
#         print(f"   自动保存: {'启用' if auto_save else '禁用'}")
#         if auto_save:
#             print(f"   保存频率: 每处理{save_frequency}个文件保存一次")

#     def add_triples_with_logging(self, triples, source_url):
#         """
#         添加三元组并记录日志，支持实时保存
        
#         Args:
#             triples: 三元组列表
#             source_url: 源URL
#         """
#         if not triples:
#             return []
        
#         # 记录到日志文件
#         self._log_triples(triples, source_url)
        
#         # 添加到图谱
#         new_nodes = self.add_triples(triples)
        
#         # 执行实体对齐
#         if new_nodes:
#             self.resolve_entities_incremental(new_nodes)
        
#         # 更新处理计数
#         self.processed_files += 1
        
#         # 检查是否需要自动保存
#         if self.auto_save and self.processed_files % self.save_frequency == 0:
#             self._auto_save()
        
#         return new_nodes
    
#     def _log_triples(self, triples, source_url):
#         """将三元组记录到日志文件"""
#         log_entry = {
#             "timestamp": datetime.now().isoformat(),
#             "source_url": source_url,
#             "triples_count": len(triples),
#             "triples": triples
#         }
        
#         try:
#             with open(self.triples_log_path, 'a', encoding='utf-8') as f:
#                 f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
#             print(f"📝 已记录 {len(triples)} 个三元组到日志文件")
            
#         except Exception as e:
#             print(f"⚠️  记录三元组日志失败: {e}")
    
#     def _auto_save(self):
#         """自动保存图谱"""
#         print(f"💾 自动保存图谱 (已处理{self.processed_files}个文件)...")
        
#         # 创建单一备份
#         self._create_single_backup()
        
#         # 保存主图谱
#         self.save_graph()
        
#         # 保存进度报告
#         self._save_progress_report()
        
#         print(f"✅ 自动保存完成")
    
#     def _create_single_backup(self):
#         """创建单一图谱备份，覆盖之前的备份"""
#         if not os.path.exists(self.graph_path):
#             return
        
#         try:
#             # 如果备份文件存在，先删除
#             if os.path.exists(self.backup_path):
#                 os.remove(self.backup_path)
#                 print(f"🗑️  删除旧备份文件")
            
#             # 复制当前图谱文件作为备份
#             shutil.copy2(self.graph_path, self.backup_path)
#             print(f"📦 已创建备份: {os.path.basename(self.backup_path)}")
            
#         except Exception as e:
#             print(f"⚠️  创建备份失败: {e}")
    
#     def _save_progress_report(self):
#         """保存处理进度报告"""
#         report = {
#             "timestamp": datetime.now().isoformat(),
#             "processed_files": self.processed_files,
#             "total_nodes": len(self.G.nodes),
#             "total_edges": len(self.G.edges),
#             "graph_file": self.graph_path,
#             "backup_file": self.backup_path,
#             "triples_log": self.triples_log_path,
#             "auto_save_enabled": self.auto_save,
#             "save_frequency": self.save_frequency
#         }
        
#         try:
#             with open(self.progress_report_path, 'w', encoding='utf-8') as f:
#                 json.dump(report, f, ensure_ascii=False, indent=2)
#             print(f"📊 已保存进度报告")
#         except Exception as e:
#             print(f"⚠️  保存进度报告失败: {e}")
    
#     def force_save(self):
#         """强制保存图谱和日志"""
#         print(f"💾 强制保存图谱...")
#         self._create_single_backup()
#         self.save_graph()
#         self._save_progress_report()
#         print(f"✅ 强制保存完成")
    
#     def get_statistics(self):
#         """获取图谱统计信息"""
#         last_save_time = "未保存"
#         if os.path.exists(self.graph_path):
#             mod_time = datetime.fromtimestamp(os.path.getmtime(self.graph_path))
#             last_save_time = mod_time.strftime('%Y-%m-%d %H:%M:%S')
        
#         stats = {
#             "processed_files": self.processed_files,
#             "total_nodes": len(self.G.nodes),
#             "total_edges": len(self.G.edges),
#             "graph_file_exists": os.path.exists(self.graph_path),
#             "backup_file_exists": os.path.exists(self.backup_path),
#             "triples_log_exists": os.path.exists(self.triples_log_path),
#             "last_save_time": last_save_time,
#             "auto_save_enabled": self.auto_save,
#             "save_frequency": self.save_frequency
#         }
#         return stats


# def main(args):
#     # --- 步骤一: 知识获取 ---
#     if args.run_scraper:
#         with open(config.SEED_URLS_FILE, 'r') as f:
#             seed_urls = [line.strip() for line in f if line.strip()]
#         scraper = Scraper(seed_urls, config.MAX_DEPTH, config.MAX_PAGES_TO_CRAWL)
#         scraper.run()

#     # --- 步骤二 & 三: 图谱构建（支持实时保存）---
#     # 使用实时保存的图谱管理器
#     graph_manager = RealTimeGraphManager(
#         config.GRAPH_STORE_PATH, 
#         auto_save=True, 
#         save_frequency=getattr(args, 'save_frequency', 1)
#     )
#     llm_extractor = LLMExtractor()

#     if args.build_graph:
#         html_files = [f for f in os.listdir(config.RAW_HTML_DIR) if f.endswith('.html')]
#         print(f"Found {len(html_files)} HTML files to process")
        
#         processed_count = 0
#         skipped_count = 0
#         successful_extractions = 0
#         total_triples = 0
        
#         for filename in html_files:
#             filepath = os.path.join(config.RAW_HTML_DIR, filename)
#             try:
#                 with open(filepath, 'r', encoding='utf-8') as f:
#                     content = f.read()
#             except (PermissionError, OSError) as e:
#                 print(f"Skipping {filename}: {e}")
#                 skipped_count += 1
#                 continue
            
#             # 从HTML注释中恢复URL，使用更健壮的解析方法
#             try:
#                 lines = content.split('\n')
#                 source_url = None
                
#                 # 方法1: 在前几行中查找URL注释（Wikipedia格式）
#                 for line in lines[:10]:  # 只检查前10行
#                     if 'URL:' in line:
#                         url_part = line.split('URL:')[1].strip()
#                         # 移除HTML注释符号
#                         source_url = url_part.rstrip('-->').strip()
#                         break
                
#                 # 方法2: 如果没找到URL注释，尝试从百度百科页面提取URL
#                 if not source_url:
#                     # 检查是否是百度百科页面
#                     if 'baike.baidu.com' in content or 'data-lemmatitle=' in content:
#                         # 尝试从lemmatitle属性提取词条名
#                         import re
#                         lemma_match = re.search(r'data-lemmatitle="([^"]*)"', content)
#                         if lemma_match:
#                             lemma_title = lemma_match.group(1)
#                             # 构造百度百科URL
#                             import urllib.parse
#                             encoded_title = urllib.parse.quote(lemma_title)
#                             source_url = f"https://baike.baidu.com/item/{encoded_title}"
#                             print(f"Extracted Baidu Baike URL: {source_url}")
#                         else:
#                             # 尝试从页面标题提取
#                             title_match = re.search(r'<title>([^_]+)_百度百科</title>', content)
#                             if title_match:
#                                 title = title_match.group(1).strip()
#                                 encoded_title = urllib.parse.quote(title)
#                                 source_url = f"https://baike.baidu.com/item/{encoded_title}"
#                                 print(f"Extracted Baidu Baike URL from title: {source_url}")
                
#                 if not source_url:
#                     print(f"Warning: Could not extract URL from {filename}, skipping...")
#                     skipped_count += 1
#                     continue
                    
#             except Exception as e:
#                 print(f"Error parsing URL from {filename}: {e}, skipping...")
#                 skipped_count += 1
#                 skipped_count += 1
#                 continue

#             text_blocks = parse_html_to_text_blocks(content, source_url)
#             print(f"Extracted {len(text_blocks)} text blocks from {filename}")
            
#             triples = llm_extractor.extract_triples_batch(text_blocks)
            
#             if triples:
#                 # 使用实时保存功能
#                 new_nodes = graph_manager.add_triples_with_logging(triples, source_url)
#                 successful_extractions += 1
#                 total_triples += len(triples)
#                 print(f"Added {len(triples)} triples to graph from {filename}")
#             else:
#                 print(f"No triples extracted from {filename}")
            
#             processed_count += 1
#             print(f"Processed {filename} ({processed_count}/{len(html_files)}) - Total triples so far: {total_triples}")
        
#         print(f"Processing complete: {processed_count} processed, {skipped_count} skipped")
#         print(f"Successful extractions: {successful_extractions}/{processed_count}")
#         print(f"Total triples extracted: {total_triples}")
        
#         # 最终保存
#         graph_manager.force_save()
        
#         if args.save_to_neo4j:
#             graph_manager.save_to_neo4j()
        
#         # 显示统计信息
#         stats = graph_manager.get_statistics()
#         print(f"\n📊 最终统计信息:")
#         for key, value in stats.items():
#             print(f"   {key}: {value}")

#     # --- 步骤四: 问题生成 ---
#     if args.generate_questions:
#         path_sampler = PathSampler(graph_manager.G)
#         question_generator = QuestionGenerator()
        
#         # 采样链式推理路径
#         chain_paths = path_sampler.sample_chain_reasoning(num_samples=config.NUM_QUESTIONS_TO_GENERATE)
        
#         # 执行生成级联
#         generated_samples = question_generator.generate_questions_cascade(chain_paths)
        
#         # --- 步骤五: 验证与输出 ---
#         os.makedirs(os.path.dirname(config.OUTPUT_DATASET_PATH), exist_ok=True)
#         with open(config.OUTPUT_DATASET_PATH, 'w', encoding='utf-8') as f:
#             for i, sample in enumerate(generated_samples):
#                 final_data_point = assemble_final_json(i, sample, graph_manager.G)
#                 f.write(json.dumps(final_data_point, ensure_ascii=False) + '\n')
        
#         print(f"Successfully generated {len(generated_samples)} questions to {config.OUTPUT_DATASET_PATH}")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Multi-Hop QA Dataset Generation Pipeline with Real-time Saving")
#     parser.add_argument("--run-scraper", action="store_true", help="Run the web scraping stage.")
#     parser.add_argument("--build-graph", action="store_true", help="Build the knowledge graph from scraped HTML.")
#     parser.add_argument("--generate-questions", action="store_true", help="Generate questions from the knowledge graph.")
#     parser.add_argument("--save-to-neo4j", action="store_true", help="Save the final graph to Neo4j database.")
#     parser.add_argument("--save-frequency", type=int, default=1, help="Auto-save frequency (save after processing N files, default: 1)")
    
#     args = parser.parse_args()
#     main(args)
import os
import argparse
import json
import shutil
from datetime import datetime
from config import config
from scraper_module.scraper import Scraper
from graph_module.poster import parse_html_to_text_blocks
from graph_module.fact_extractor import LLMExtractor
from graph_module.graph_manager import GraphManager
from generate_module.path_sampler import PathSampler
from generate_module.question_generator import QuestionGenerator
from generate_module.validator import assemble_final_json

class RealTimeGraphManager(GraphManager):
    
    def __init__(self, graph_path=None, auto_save=True, save_frequency=1):
        """
        初始化图谱管理器
        
        Args:
            graph_path: 图谱文件路径
            auto_save: 是否启用自动保存
            save_frequency: 保存频率（处理多少个文件后保存一次）
        """
        super().__init__(graph_path)
        
        self.auto_save = auto_save
        self.save_frequency = save_frequency
        self.processed_files = 0
        
        # 设置文件路径
        self.graph_path = graph_path or config.GRAPH_STORE_PATH
        self.backup_path = self.graph_path.replace('.gpickle', '_backup.gpickle')
        self.triples_log_path = os.path.join(
            os.path.dirname(self.graph_path), 
            "triples_log.jsonl"
        )
        self.progress_report_path = os.path.join(
            os.path.dirname(self.graph_path),
            "progress_report.json"
        )
        
        print(f"✅ 实时保存图谱管理器初始化完成")
        print(f"   主图谱文件: {os.path.basename(self.graph_path)}")
        print(f"   备份文件: {os.path.basename(self.backup_path)}")
        print(f"   三元组日志: {os.path.basename(self.triples_log_path)}")
        print(f"   自动保存: {'启用' if auto_save else '禁用'}")
        if auto_save:
            print(f"   保存频率: 每处理{save_frequency}个文件保存一次")

    def add_triples_with_logging(self, triples, source_url):
        """
        添加三元组并记录日志，支持实时保存
        
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
        self.processed_files += 1
        
        # 检查是否需要自动保存
        if self.auto_save and self.processed_files % self.save_frequency == 0:
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
        print(f"💾 自动保存图谱 (已处理{self.processed_files}个文件)...")
        
        # 创建单一备份
        self._create_single_backup()
        
        # 保存主图谱
        self.save_graph()
        
        # 保存进度报告
        self._save_progress_report()
        
        print(f"✅ 自动保存完成")
    
    def _create_single_backup(self):
        """创建单一图谱备份，覆盖之前的备份"""
        if not os.path.exists(self.graph_path):
            return
        
        try:
            # 如果备份文件存在，先删除
            if os.path.exists(self.backup_path):
                os.remove(self.backup_path)
                print(f"🗑️  删除旧备份文件")
            
            # 复制当前图谱文件作为备份
            shutil.copy2(self.graph_path, self.backup_path)
            print(f"📦 已创建备份: {os.path.basename(self.backup_path)}")
            
        except Exception as e:
            print(f"⚠️  创建备份失败: {e}")
    
    def _save_progress_report(self):
        """保存处理进度报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "processed_files": self.processed_files,
            "total_nodes": len(self.G.nodes),
            "total_edges": len(self.G.edges),
            "graph_file": self.graph_path,
            "backup_file": self.backup_path,
            "triples_log": self.triples_log_path,
            "auto_save_enabled": self.auto_save,
            "save_frequency": self.save_frequency
        }
        
        try:
            with open(self.progress_report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"📊 已保存进度报告")
        except Exception as e:
            print(f"⚠️  保存进度报告失败: {e}")
    
    def force_save(self):
        """强制保存图谱和日志"""
        print(f"💾 强制保存图谱...")
        self._create_single_backup()
        self.save_graph()
        self._save_progress_report()
        print(f"✅ 强制保存完成")
    
    def get_statistics(self):
        """获取图谱统计信息"""
        last_save_time = "未保存"
        if os.path.exists(self.graph_path):
            mod_time = datetime.fromtimestamp(os.path.getmtime(self.graph_path))
            last_save_time = mod_time.strftime('%Y-%m-%d %H:%M:%S')
        
        stats = {
            "processed_files": self.processed_files,
            "total_nodes": len(self.G.nodes),
            "total_edges": len(self.G.edges),
            "graph_file_exists": os.path.exists(self.graph_path),
            "backup_file_exists": os.path.exists(self.backup_path),
            "triples_log_exists": os.path.exists(self.triples_log_path),
            "last_save_time": last_save_time,
            "auto_save_enabled": self.auto_save,
            "save_frequency": self.save_frequency
        }
        return stats


def main(args):
    # --- 步骤一: 知识获取 ---
    if args.run_scraper:
        with open(config.SEED_URLS_FILE, 'r') as f:
            seed_urls = [line.strip() for line in f if line.strip()]
        scraper = Scraper(seed_urls, config.MAX_DEPTH, config.MAX_PAGES_TO_CRAWL)
        scraper.run()

    # --- 步骤二 & 三: 图谱构建（支持实时保存）---
    # 使用实时保存的图谱管理器
    graph_manager = RealTimeGraphManager(
        config.GRAPH_STORE_PATH, 
        auto_save=True, 
        save_frequency=getattr(args, 'save_frequency', 1)
    )
    llm_extractor = LLMExtractor()

    if args.build_graph:
        all_html_files = [f for f in os.listdir(config.RAW_HTML_DIR) if f.endswith('.html')]
        total_files = len(all_html_files)
        print(f"Found {total_files} HTML files total")
        
        # 断点续传功能：从指定索引开始处理
        start_index = getattr(args, 'start_from', 0)
        if start_index > 0:
            print(f"Resuming from file index {start_index} (skipping first {start_index} files)")
            html_files = all_html_files[start_index:]
        else:
            html_files = all_html_files
        
        print(f"Will process {len(html_files)} files (from index {start_index} to {total_files-1})")
        
        processed_count = start_index  # 从指定索引开始计数
        skipped_count = 0
        successful_extractions = 0
        total_triples = 0
        
        for filename in html_files:
            filepath = os.path.join(config.RAW_HTML_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
            except (PermissionError, OSError) as e:
                print(f"Skipping {filename}: {e}")
                skipped_count += 1
                continue
            
            # 从HTML注释中恢复URL，使用更健壮的解析方法
            try:
                lines = content.split('\n')
                source_url = None
                
                # 方法1: 在前几行中查找URL注释（Wikipedia格式）
                for line in lines[:10]:  # 只检查前10行
                    if 'URL:' in line:
                        url_part = line.split('URL:')[1].strip()
                        # 移除HTML注释符号
                        source_url = url_part.rstrip('-->').strip()
                        break
                
                # 方法2: 如果没找到URL注释，尝试从百度百科页面提取URL
                if not source_url:
                    # 检查是否是百度百科页面
                    if 'baike.baidu.com' in content or 'data-lemmatitle=' in content:
                        # 尝试从lemmatitle属性提取词条名
                        import re
                        lemma_match = re.search(r'data-lemmatitle="([^"]*)"', content)
                        if lemma_match:
                            lemma_title = lemma_match.group(1)
                            # 构造百度百科URL
                            import urllib.parse
                            encoded_title = urllib.parse.quote(lemma_title)
                            source_url = f"https://baike.baidu.com/item/{encoded_title}"
                            print(f"Extracted Baidu Baike URL: {source_url}")
                        else:
                            # 尝试从页面标题提取
                            title_match = re.search(r'<title>([^_]+)_百度百科</title>', content)
                            if title_match:
                                title = title_match.group(1).strip()
                                encoded_title = urllib.parse.quote(title)
                                source_url = f"https://baike.baidu.com/item/{encoded_title}"
                                print(f"Extracted Baidu Baike URL from title: {source_url}")
                
                if not source_url:
                    print(f"Warning: Could not extract URL from {filename}, skipping...")
                    skipped_count += 1
                    continue
                    
            except Exception as e:
                print(f"Error parsing URL from {filename}: {e}, skipping...")
                skipped_count += 1
                skipped_count += 1
                continue

            text_blocks = parse_html_to_text_blocks(content, source_url)
            print(f"Extracted {len(text_blocks)} text blocks from {filename}")
            
            triples = llm_extractor.extract_triples_batch(text_blocks)
            
            if triples:
                # 使用实时保存功能
                new_nodes = graph_manager.add_triples_with_logging(triples, source_url)
                successful_extractions += 1
                total_triples += len(triples)
                print(f"Added {len(triples)} triples to graph from {filename}")
            else:
                print(f"No triples extracted from {filename}")
            
            processed_count += 1
            print(f"Processed {filename} ({processed_count}/{total_files}) - Total triples so far: {total_triples}")
        
        print(f"Processing complete: {processed_count} processed, {skipped_count} skipped")
        print(f"Successful extractions: {successful_extractions}/{processed_count}")
        print(f"Total triples extracted: {total_triples}")
        
        # 最终保存
        graph_manager.force_save()
        
        if args.save_to_neo4j:
            graph_manager.save_to_neo4j()
        
        # 显示统计信息
        stats = graph_manager.get_statistics()
        print(f"\n📊 最终统计信息:")
        for key, value in stats.items():
            print(f"   {key}: {value}")

    # --- 步骤四: 问题生成 ---
    if args.generate_questions:
        print(f"\n🚀 开始使用新的三阶段深度问题生成策略...")
        
        # 初始化新的问题生成器
        question_generator = QuestionGenerator()
        
        # 检查图的基本信息
        total_nodes = len(graph_manager.G.nodes)
        total_edges = len(graph_manager.G.edges)
        print(f"📊 知识图谱信息:")
        print(f"   节点数: {total_nodes}")
        print(f"   边数: {total_edges}")
        
        if total_nodes < 10:
            print(f"⚠️  警告: 图节点数量过少 ({total_nodes})，可能影响问题生成效果")
            print(f"💡 建议: 先运行 --build-graph 构建更完整的知识图谱")
            return
        
        # 根据参数选择生成方式
        if hasattr(args, 'use_old_strategy') and args.use_old_strategy:
            print(f"📚 使用旧版策略 (已废弃，仅用于对比)")
            path_sampler = PathSampler(graph_manager.G)
            question_generator_old = QuestionGeneratorOld()
            
            # 采样链式推理路径
            chain_paths = path_sampler.sample_chain_reasoning(num_samples=config.NUM_QUESTIONS_TO_GENERATE)
            
            # 执行旧版生成级联
            generated_samples = question_generator_old.generate_questions_cascade(chain_paths, batch_size=10)
            
            # 旧版输出格式
            os.makedirs(os.path.dirname(config.OUTPUT_DATASET_PATH), exist_ok=True)
            with open(config.OUTPUT_DATASET_PATH, 'w', encoding='utf-8') as f:
                for i, sample in enumerate(generated_samples):
                    final_data_point = assemble_final_json(i, sample, graph_manager.G)
                    f.write(json.dumps(final_data_point, ensure_ascii=False) + '\n')
            
            print(f"📄 旧版策略生成完成: {len(generated_samples)} 个问题 → {config.OUTPUT_DATASET_PATH}")
        
        else:
            print(f"🌟 使用新的三阶段深度策略")
            
            # 确定生成参数
            total_questions = getattr(args, 'num_questions', config.NUM_QUESTIONS_TO_GENERATE)
            batch_size = getattr(args, 'batch_size', 10)
            
            print(f"📋 生成参数:")
            print(f"   目标问题数: {total_questions}")
            print(f"   批次大小: {batch_size}")
            
            # 使用新的一键运行方法
            result = question_generator.run_new_strategy(
                graph=graph_manager.G,
                total_questions=total_questions,
                batch_size=batch_size
            )
            
            # 报告结果
            if result['success']:
                print(f"\n🎉 新策略生成成功!")
                print(f"📊 生成结果:")
                print(f"   总样本数: {result['total_samples']}")
                print(f"   验证通过数: {result['validated_count']}")
                print(f"   验证通过率: {result['validation_rate']:.1f}%")
                print(f"   简化格式问题数: {result['simple_count']}")
                
                # 输出文件位置
                output_dir = os.path.join(os.path.dirname(config.OUTPUT_DATASET_PATH), "output")
                print(f"\n📁 输出文件:")
                print(f"   验证通过的问题: {output_dir}/validated_qa_new_strategy.jsonl")
                print(f"   简化问答格式: {output_dir}/simple_qa_new_strategy.jsonl")
                print(f"   详细调试信息: {output_dir}/detailed_qa_new_strategy.jsonl")
                
                # 为了向后兼容，也生成传统格式
                if result['validated_count'] > 0:
                    print(f"\n📄 同时生成传统格式文件以保持兼容性...")
                    os.makedirs(os.path.dirname(config.OUTPUT_DATASET_PATH), exist_ok=True)
                    
                    # 读取验证通过的问题并转换为传统格式
                    validated_file = os.path.join(output_dir, "validated_qa_new_strategy.jsonl")
                    if os.path.exists(validated_file):
                        with open(validated_file, 'r', encoding='utf-8') as f_in, \
                             open(config.OUTPUT_DATASET_PATH, 'w', encoding='utf-8') as f_out:
                            
                            for i, line in enumerate(f_in):
                                try:
                                    qa_item = json.loads(line)
                                    # 转换为传统格式
                                    traditional_format = {
                                        "id": i,
                                        "question": qa_item.get('question', ''),
                                        "answer": qa_item.get('answer', ''),
                                        "context": qa_item.get('background_story', ''),
                                        "reasoning_path": [],  # 新策略中路径结构不同
                                        "metadata": {
                                            "strategy": "new_three_stage_strategy",
                                            "validation_status": qa_item.get('validation_status', ''),
                                            "clue_count": len(qa_item.get('clue_entities', [])),
                                            "generation_method": "multi_source_clue_aggregation"
                                        }
                                    }
                                    f_out.write(json.dumps(traditional_format, ensure_ascii=False) + '\n')
                                except Exception as e:
                                    print(f"⚠️  转换第{i}行时出错: {e}")
                        
                        print(f"✅ 传统格式兼容文件已生成: {config.OUTPUT_DATASET_PATH}")
                
            else:
                print(f"\n❌ 新策略生成失败: {result.get('error', '未知错误')}")
                print(f"💡 建议:")
                print(f"   1. 检查知识图谱是否正确构建")
                print(f"   2. 确认API配置是否正确") 
                print(f"   3. 尝试减少目标问题数或批次大小")
                print(f"   4. 查看详细错误日志")
                return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Hop QA Dataset Generation Pipeline with New Three-Stage Strategy")
    
    # 原有参数
    parser.add_argument("--run-scraper", action="store_true", help="Run the web scraping stage.")
    parser.add_argument("--build-graph", action="store_true", help="Build the knowledge graph from scraped HTML.")
    parser.add_argument("--generate-questions", action="store_true", help="Generate questions from the knowledge graph using NEW three-stage strategy.")
    parser.add_argument("--save-to-neo4j", action="store_true", help="Save the final graph to Neo4j database.")
    parser.add_argument("--save-frequency", type=int, default=1, help="Auto-save frequency (save after processing N files, default: 1)")
    parser.add_argument("--start-from", type=int, default=0, help="Resume processing from file index N (0-based indexing, default: 0)")
    
    # 新增的问题生成参数
    parser.add_argument("--num-questions", type=int, default=None, help="Number of questions to generate (default: from config)")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for question generation (default: 10)")
    parser.add_argument("--use-old-strategy", action="store_true", help="Use old question generation strategy (deprecated, for comparison only)")
    
    args = parser.parse_args()
    
    # 如果没有指定问题数量，使用配置文件中的值
    if args.num_questions is None:
        args.num_questions = config.NUM_QUESTIONS_TO_GENERATE
    
    print(f"🚀 多跳问答数据集生成流水线")
    print(f"策略: {'新三阶段深度策略' if not args.use_old_strategy else '旧版策略(已废弃)'}")
    print(f"="*60)
    
    main(args)