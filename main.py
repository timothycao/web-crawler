import time
from collections import deque, defaultdict

from input.seed import get_seeds
from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url
from logger.log import log_url, log_summary

MAX_PAGES = 100

def main():
    # Crawl state initialization
    seeds = get_seeds()
    queue = deque((url, 0) for url in seeds) # (url, depth)
    visited = set()

    # Crawl statistics
    total_pages = 0
    total_bytes = 0
    status_counts = defaultdict(int)
    start_time = time.time()

    with open('log.txt', 'w') as log:
        # BFS crawl loop
        while queue and total_pages < MAX_PAGES:
            url, depth = queue.popleft()
            if url in visited: continue
            
            # Check robots.txt permission
            if not is_allowed(url):
                print('Skipping', url)
                continue

            # Fetch page and record status
            html, meta = fetch_page(url)
            status_counts[meta['status_code']] += 1
            if not html or meta['status_code'] != 200: continue
            
            # Track visited and update crawl stats
            visited.add(url)
            total_pages += 1
            total_bytes += meta['content_length']
            log_url(log, url, meta, depth)

            # Extract and enqueue child links
            links = extract_links(html, url)
            for link in links:
                link = clean_url(link)
                if link in visited: continue
                queue.append((link, depth + 1))
        
        # Log crawl summary
        total_time = time.time() - start_time
        log_summary(log, total_pages, total_bytes, total_time, status_counts)

if __name__ == '__main__':
    main()