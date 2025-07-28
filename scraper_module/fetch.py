import time
import random
import requests
from bs4 import BeautifulSoup
import backoff
from playwright.sync_api import sync_playwright

from config import config

def _get_random_headers(referer=None):
    """从配置中获取一个随机的、完整的请求头"""
    headers = {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "DNT": "1" # Do Not Track
    }
    if referer:
        headers["Referer"] = referer
    return headers

@backoff.on_exception(backoff.expo, 
                      (requests.exceptions.RequestException, IOError), 
                      max_tries=5,
                      jitter=backoff.full_jitter)
def fetch_content(url, path_history):
    """
    混合内容获取函数。
    首先尝试requests，如果内容可疑（过小或含<noscript>），则使用Playwright。
    """
    print(f"Fetching {url}...")
    # 智能节流
    time.sleep(random.uniform(*config.CRAWL_DELAY_RANGE))
    
    referer = path_history[-2] if len(path_history) > 1 else None
    headers = _get_random_headers(referer)
    
    try:
        # 阶段一：使用requests
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html_content = response.text

        # 启发式规则判断是否需要JS渲染
        # 规则1: 内容体积过小 (可能是一个空的加载页)
        # 规则2: 包含 <noscript> 标签，明确提示需要JS
        content_length = len(html_content)
        has_noscript = '<noscript>' in html_content.lower()
        
        if content_length < 500 or has_noscript:
            print(f"Heuristic triggered for {url}. Content length: {content_length}, Has noscript: {has_noscript}. Switching to Playwright.")
            raise ValueError("Potential dynamic content")

        soup = BeautifulSoup(html_content, 'html.parser')
        return html_content, soup

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Requests failed or heuristic triggered for {url}: {e}. Retrying with Playwright.")
        # 阶段二：使用Playwright作为备选
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(user_agent=random.choice(config.USER_AGENTS))
                page.goto(url, wait_until='networkidle', timeout=60000)
                html_content = page.content()
                browser.close()
                
                soup = BeautifulSoup(html_content, 'html.parser')
                print(f"Successfully fetched {url} with Playwright.")
                return html_content, soup
        except Exception as playwright_err:
            print(f"Playwright also failed for {url}: {playwright_err}")
            return None, None