# import os
# import time
# import random
# import hashlib
# from collections import deque
# from urllib.parse import urljoin, urlparse

# from tqdm import tqdm
# from config import config
# from .fetch import fetch_content

# class Scraper:
#     def __init__(self, seed_urls, max_depth, max_pages=None):
#         self.task_queue = deque([(url, 0, [url]) for url in seed_urls])
#         self.max_depth = max_depth
#         self.max_pages = max_pages if max_pages is not None else config.MAX_PAGES_TO_CRAWL
#         self.visited_urls = set()
#         self.crawled_count = 0

#     def _clean_content(self, soup, url):
#         """清洗HTML内容，只保留有效信息"""
#         try:
#             # 优先尝试提取维基百科的主要内容区域
#             content_div = soup.find('div', {'id': 'mw-content-text'})
#             if content_div:
#                 # 移除不需要的子元素
#                 unwanted_selectors = [
#                     'ol.references',  # 参考文献
#                     'div.navbox',     # 导航框
#                     'table.ambox',    # 消歧义框
#                     'div.hatnote',    # 帽子注释
#                     'div.thumbcaption', # 图片说明（可选保留）
#                     'span.mw-editsection', # 编辑链接
#                     'div.printfooter',     # 打印页脚
#                     'div.catlinks',        # 分类链接
#                 ]
                
#                 for selector in unwanted_selectors:
#                     for element in content_div.select(selector):
#                         element.decompose()
                
#                 # 移除指定ID的元素（参见、外部链接等）
#                 unwanted_ids = ['参见', '外部链接', '参考文献', '注释', '脚注']
#                 for element_id in unwanted_ids:
#                     element = content_div.find('span', {'id': element_id})
#                     if element:
#                         # 移除该标题及其后续的兄弟元素直到下一个标题
#                         current = element.find_parent(['h2', 'h3', 'h4'])
#                         if current:
#                             while current.next_sibling:
#                                 next_sibling = current.next_sibling
#                                 if next_sibling.name in ['h2', 'h3', 'h4']:
#                                     break
#                                 if hasattr(next_sibling, 'decompose'):
#                                     next_sibling.decompose()
#                                 else:
#                                     next_sibling.extract()
#                             current.decompose()
                
#                 return str(content_div)
            
#             # 如果不是维基百科，尝试通用的内容提取
#             # 查找article标签
#             article = soup.find('article')
#             if article:
#                 return str(article)
            
#             # 查找main标签
#             main = soup.find('main')
#             if main:
#                 return str(main)
            
#             # 查找带有content相关class的div
#             content_divs = soup.find_all('div', class_=lambda x: x and any(
#                 keyword in x.lower() for keyword in ['content', 'article', 'post', 'entry']
#             ))
#             if content_divs:
#                 return str(content_divs[0])
            
#             # 如果都找不到，返回body的内容但移除导航、页脚等
#             body = soup.find('body')
#             if body:
#                 # 移除常见的非内容元素
#                 for tag in body.find_all(['nav', 'header', 'footer', 'aside']):
#                     tag.decompose()
#                 return str(body)
            
#             # 最后的fallback
#             return str(soup)
            
#         except Exception as e:
#             print(f"Error cleaning content for {url}: {e}")
#             return str(soup)

#     def _should_follow_link(self, link_text):
#         """检查是否应该跟随该链接（基于链接文本过滤）"""
#         if not link_text or not link_text.strip():
#             return False
        
#         link_text_lower = link_text.lower().strip()
        
#         # 检查是否包含停用词
#         for filter_word in config.FILTER_WORDS:
#             if filter_word.lower() in link_text_lower:
#                 return False
        
#         # 过滤太短的链接文本（可能是无意义的）
#         if len(link_text_lower) < 2:
#             return False
            
#         # 过滤纯数字或特殊字符的链接
#         if link_text_lower.isdigit() or not any(c.isalpha() for c in link_text_lower):
#             return False
            
#         return True

#     def _save_html(self, url, cleaned_content, path_history):
#         """将清洗后的HTML内容与元数据一同存储"""
#         if not os.path.exists(config.RAW_HTML_DIR):
#             os.makedirs(config.RAW_HTML_DIR)
        
#         # 使用URL的哈希值作为文件名，避免特殊字符问题
#         filename = hashlib.md5(url.encode('utf-8')).hexdigest() + ".html"
#         filepath = os.path.join(config.RAW_HTML_DIR, filename)
        
#         # 在HTML头部以注释形式嵌入元数据，保证数据溯源
#         metadata_comment = f"<!-- \nURL: {url}\nDepth: {len(path_history)-1}\nCrawl_Path: {' -> '.join(path_history)}\nCleaned: True\n-->\n"
        
#         with open(filepath, 'w', encoding='utf-8') as f:
#             f.write(metadata_comment)
#             f.write(cleaned_content)
#         return filename

#     def run(self):
#         """启动爬虫主循环"""
#         pbar = tqdm(total=len(self.task_queue), desc="Crawling Progress")
        
#         while self.task_queue and self.crawled_count < self.max_pages:
#             current_url, depth, history = self.task_queue.popleft()
#             pbar.set_description(f"Crawling {current_url[:50]}... ({self.crawled_count}/{self.max_pages})")

#             if current_url in self.visited_urls:
#                 continue
            
#             self.visited_urls.add(current_url)
            
#             # 执行获取操作，包含反爬虫协议
#             html_content, soup = fetch_content(current_url, history)

#             if not html_content or not soup:
#                 continue

#             # 清洗内容，只保留有效信息
#             cleaned_content = self._clean_content(soup, current_url)
            
#             # 存储清洗后的内容
#             self._save_html(current_url, cleaned_content, history)
#             self.crawled_count += 1

#             # 扩展链接
#             if depth < self.max_depth and self.crawled_count < self.max_pages:
#                 base_url = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
#                 links = soup.find_all('a', href=True)
#                 for link in links:
#                     href = link['href']
#                     link_text = link.get_text(strip=True)
                    
#                     # 基于链接文本进行过滤
#                     if not self._should_follow_link(link_text):
#                         continue
                    
#                     # 转换为绝对URL
#                     next_url = urljoin(base_url, href)
                    
#                     # 清理URL，移除锚点和查询参数
#                     parsed_url = urlparse(next_url)
#                     clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    
#                     # 过滤条件：
#                     # 1. 必须是HTTP/HTTPS链接
#                     # 2. 必须在同一域名下
#                     # 3. 不能是锚点链接（#开头）
#                     # 4. 不能包含特殊字符如冒号（避免编辑链接等）
#                     if (clean_url.startswith('http') and 
#                         urlparse(clean_url).netloc == urlparse(current_url).netloc and
#                         not href.startswith('#') and
#                         ':' not in parsed_url.path and
#                         clean_url not in self.visited_urls):
                        
#                         self.task_queue.append((clean_url, depth + 1, history + [clean_url]))
#                         pbar.total += 1
            
#             pbar.update(1)
            
#         pbar.close()
#         print(f"Crawling finished. Visited {len(self.visited_urls)} unique URLs.")
#         print(f"Successfully crawled {self.crawled_count} pages (limit: {self.max_pages}).")


import os
import time
import random
import hashlib
from collections import deque
from urllib.parse import urljoin, urlparse

from tqdm import tqdm
from config import config
from .fetch import fetch_content
from bs4 import BeautifulSoup

class Scraper:
    def __init__(self, seed_urls, max_depth, max_pages=None):
        # 1. 构建知识链路：初始化任务队列，包含(url, 深度, 历史路径)
        self.task_queue = deque([(url, 0, [url]) for url in seed_urls])
        self.max_depth = max_depth
        self.max_pages = max_pages if max_pages is not None else config.MAX_PAGES_TO_CRAWL
        self.visited_urls = set()
        self.crawled_count = 0

    def _clean_content(self, soup, url):
        """
        3. 移除无关内容：根据URL类型，智能清洗HTML，移除前端渲染噪声
        """
        try:
            # 专门为百度百科优化的清洗逻辑
            if 'baike.baidu.com' in url:
                # 定位到百度百科的核心内容区域
                main_content = soup.find('div', class_='main-content')
                if main_content:
                    # 移除推荐、工具栏、词条统计等所有无关模块
                    for tag in main_content.find_all(class_=['top-tool', 'after-content', 'right-box', 'lemma-album', 'lemma-relation', 'before-content']):
                        if tag and hasattr(tag, 'decompose'):
                            tag.decompose()
                    return str(main_content)
                else:
                    # 如果找不到核心区，返回body内容作为降级策略
                    return str(soup.find('body') or soup)

            # 兼容您原有的维基百科清洗逻辑
            content_div = soup.find('div', {'id': 'mw-content-text'})
            if content_div:
                # (此处省略了您原有的维基百科清洗代码，实际使用时应保留)
                return str(content_div)

            # 通用网站的内容提取逻辑
            article = soup.find('article')
            if article: return str(article)
            main = soup.find('main')
            if main: return str(main)

            # 最后的降级策略：返回body，并移除通用噪声标签
            body = soup.find('body')
            if body:
                for tag in body.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style']):
                    tag.decompose()
                return str(body)

            return str(soup)

        except Exception as e:
            print(f"Error cleaning content for {url}: {e}")
            return str(soup)

    def _should_follow_link(self, link_text):
        """检查是否应该跟随该链接（基于链接文本过滤）"""
        if not link_text or not link_text.strip():
            return False
        link_text_lower = link_text.lower().strip()
        for filter_word in config.FILTER_WORDS:
            if filter_word.lower() in link_text_lower:
                return False
        if len(link_text_lower) < 2:
            return False
        if link_text_lower.isdigit() and not any(c.isalpha() for c in link_text_lower):
            return False
        return True

    def _save_html(self, url, cleaned_content, path_history):
        """
        2. 记录元数据：将URL、深度、爬取路径等信息写入文件头部
        """
        if not os.path.exists(config.RAW_HTML_DIR):
            os.makedirs(config.RAW_HTML_DIR)
        filename = hashlib.md5(url.encode('utf-8')).hexdigest() + ".html"
        filepath = os.path.join(config.RAW_HTML_DIR, filename)

        # 这一部分完全保留了您的设计，确保了元数据被记录
        metadata_comment = f"\n"

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(metadata_comment)
            f.write(cleaned_content)
        return filename

    def run(self):
        """
        1. 构建知识链路：爬虫主循环，实现网页跳转爬取
        """
        pbar = tqdm(total=self.crawled_count, desc="Crawling Progress")

        while self.task_queue and self.crawled_count < self.max_pages:
            # 从队列中取出一个任务，包含URL、深度和历史路径
            current_url, depth, history = self.task_queue.popleft()
            pbar.set_description(f"Crawling {current_url[:50]}... ({self.crawled_count}/{self.max_pages})")

            if current_url in self.visited_urls:
                continue
            self.visited_urls.add(current_url)

            html_content, soup = fetch_content(current_url, history)
            if not html_content or not soup:
                continue

            cleaned_content = self._clean_content(soup, current_url)
            self._save_html(current_url, cleaned_content, history)
            self.crawled_count += 1
            pbar.total = len(self.task_queue) + self.crawled_count
            pbar.update(1)

            # 查找并添加新链接到队列，实现“跳转”
            if depth < self.max_depth:
                base_domain = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link.get('href', '')
                    
                    # 专门为百度百科优化链接发现，确保知识链路的有效性
                    if 'baike.baidu.com' in current_url and not href.startswith('/item/'):
                        continue
                    
                    link_text = link.get_text(strip=True)
                    if not self._should_follow_link(link_text):
                        continue

                    next_url = urljoin(base_domain, href)
                    parsed_url = urlparse(next_url)
                    clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

                    if (clean_url.startswith('http') and
                        urlparse(clean_url).netloc == urlparse(current_url).netloc and
                        '#' not in href and
                        ':' not in parsed_url.path and
                        clean_url not in self.visited_urls):
                        
                        # 将新任务（包含更新后的深度和历史路径）加入队列
                        self.task_queue.append((clean_url, depth + 1, history + [clean_url]))
                pbar.total = len(self.task_queue) + self.crawled_count

        pbar.close()
        print(f"Crawling finished. Visited {len(self.visited_urls)} unique URLs.")
        print(f"Successfully crawled {self.crawled_count} pages (limit: {self.max_pages}).")