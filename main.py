from time import time
from heapq import heappush, heappop
from collections import defaultdict
from urllib.parse import urlparse

from input.seed import get_seeds
from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url
from utils.priority import compute_priority
from logger.log import log_url, log_summary

MAX_PAGES = 100

def main():
    # Crawl state initialization
    seeds = get_seeds()
    max_heap = [] # Simulated max-heap using -priority
    visited = set()
    domain_counts = defaultdict(int)
    
    for seed in seeds:
        heappush(max_heap, (0, seed, 0)) # (-priority, url, depth)

    # Crawl statistics
    total_pages = 0
    total_bytes = 0
    status_counts = defaultdict(int)
    start_time = time()

    with open('log.txt', 'w') as log:
        # BFS crawl loop
        while max_heap and total_pages < MAX_PAGES:
            _, url, depth = heappop(max_heap)
            if url in visited: continue
            
            # Check robots.txt permission
            if not is_allowed(url):
                print('Skipping', url)
                continue

            # Fetch page and record status
            html, meta = fetch_page(url)
            status_counts[meta['status_code']] += 1
            if not html or meta['status_code'] != 200: continue
            
            # Update crawl stats
            visited.add(url)
            total_pages += 1
            total_bytes += meta['content_length']

            # Compute priority and log result
            domain = urlparse(url).netloc
            domain_counts[domain] += 1
            priority = compute_priority(domain_counts[domain])
            log_url(log, url, meta, depth, priority)

            # Extract and enqueue child links
            links = extract_links(html, url)
            for link in links:
                link = clean_url(link)
                if link in visited: continue

                link_domain = urlparse(link).netloc
                link_priority = compute_priority(domain_counts[link_domain])
                heappush(max_heap, (-link_priority, link, depth + 1))
        
        # Log crawl summary
        total_time = time() - start_time
        log_summary(log, total_pages, total_bytes, total_time, status_counts, domain_counts)

if __name__ == '__main__':
    main()