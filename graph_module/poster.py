# import trafilatura
# from bs4 import BeautifulSoup

# def parse_html_to_text_blocks(html_content, source_url):
#     """
#     结合trafilatura和BeautifulSoup进行高保真内容提取。
#     返回一个文本块列表。
#     """
#     text_blocks = []

#     # 步骤1: 使用trafilatura提取主要内容
#     # favor_precision=True 倾向于更准确但可能更短的输出
#     main_text = trafilatura.extract(html_content, 
#                                     favor_precision=True, 
#                                     include_comments=False, 
#                                     include_tables=True) # 保留表格内容
#     if main_text:
#         text_blocks.append({
#             "text": main_text.strip(),
#             "source": "trafilatura_main",
#             "url": source_url
#         })

#     # 步骤2: 使用BeautifulSoup进行补充性、靶向性提取
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     # 提取infobox (维基百科等常见模式)
#     infoboxes = soup.select('.infobox,.infobox_v2,.infobox_v3')
#     for box in infoboxes:
#         box_text = box.get_text(separator='\n', strip=True)
#         if box_text:
#             text_blocks.append({
#                 "text": box_text,
#                 "source": "bs4_infobox",
#                 "url": source_url
#             })
            
#     # 提取摘要段落 (通常在文章开头)
#     # 此处选择器需要根据目标网站进行定制，这里提供一个通用示例
#     summary_paragraphs = soup.select('p.summary, div.abstract > p')
#     for p in summary_paragraphs:
#         p_text = p.get_text(strip=True)
#         if p_text:
#             text_blocks.append({
#                 "text": p_text,
#                 "source": "bs4_summary",
#                 "url": source_url
#             })
            
#     # 去重和清洗
#     unique_blocks = []
#     seen_texts = set()
#     for block in text_blocks:
#         if block["text"] not in seen_texts:
#             unique_blocks.append(block)
#             seen_texts.add(block["text"])

#     return unique_blocks
import trafilatura
from bs4 import BeautifulSoup

def parse_html_to_text_blocks(html_content, source_url):
    """
    结合trafilatura和BeautifulSoup进行高保真内容提取。
    返回一个文本块列表。
    """
    text_blocks = []

    # 步骤1: 使用trafilatura提取主要内容
    # favor_precision=True 倾向于更准确但可能更短的输出
    main_text = trafilatura.extract(html_content, 
                                    favor_precision=True, 
                                    include_comments=False, 
                                    include_tables=True) # 保留表格内容
    if main_text:
        text_blocks.append({
            "text": main_text.strip(),
            "source": "trafilatura_main",
            "url": source_url
        })

    # 步骤2: 使用BeautifulSoup进行补充性、靶向性提取
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 提取infobox (维基百科等常见模式)
    infoboxes = soup.select('.infobox,.infobox_v2,.infobox_v3')
    for box in infoboxes:
        box_text = box.get_text(separator='\n', strip=True)
        if box_text:
            text_blocks.append({
                "text": box_text,
                "source": "bs4_infobox",
                "url": source_url
            })
            
    # 提取摘要段落 (通常在文章开头)
    # 此处选择器需要根据目标网站进行定制，这里提供一个通用示例
    summary_paragraphs = soup.select('p.summary, div.abstract > p')
    for p in summary_paragraphs:
        p_text = p.get_text(strip=True)
        if p_text:
            text_blocks.append({
                "text": p_text,
                "source": "bs4_summary",
                "url": source_url
            })
            
    # 去重和清洗
    unique_blocks = []
    seen_texts = set()
    for block in text_blocks:
        if block["text"] not in seen_texts:
            unique_blocks.append(block)
            seen_texts.add(block["text"])

    return unique_blocks