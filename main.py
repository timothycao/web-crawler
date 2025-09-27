from time import time
from heapq import heappush, heappop
from collections import defaultdict
from urllib.parse import urlsplit
from socket import setdefaulttimeout

from input.seed import get_seeds
from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url, validate_url
from utils.priority import compute_priority
from logger.log import log_url, log_summary

MAX_PAGES = 100
MAX_TIMEOUTS = 2 # Max allowed fetch failures per domain (status 0, no content)

# Set timeout for all socket operations (e.g. urlopen, RobotFileParser.read)
setdefaulttimeout(5)

def main():
    # Crawl state initialization
    seeds = get_seeds()
    max_heap = []           # Simulated max-heap using -priority
    scheduled = set()       # URLs scheduled to be visited (in heap)
    visited = set()         # URLs that were fetched
    disallowed = set()      # URLs blocked by robots.txt
    timeout_counts = {}     # Count timeout-related fetch failures per domain
    
    # Seed initial crawl queue
    for seed in seeds:
        # Normalize URL: strip query, fragment, and trailing slash
        seed = clean_url(seed)

        # Skip if already handled or invalid
        if (
            seed in scheduled           # Already in heap
            or seed in disallowed       # Blocked by robots.txt
            or not validate_url(seed)   # Invalid URL (not http/https)
        ):
            continue
        
        # Check robots.txt permission
        if not is_allowed(seed):
            disallowed.add(seed)
            print('Skipping', seed)
            continue
        
        heappush(max_heap, (0, seed, 0)) # (-priority, url, depth)
        scheduled.add(seed)
    
    # Crawl statistics
    total_bytes = 0         # Total bytes of fetched pages
    status_counts = {}      # Count responses per HTTP status code
    crawl_counts = {}       # Count pages successfully crawled per domain
    start_time = time()

    with open('log.txt', 'w') as log:
        # Crawl loop using max heap (priority-based BFS)
        while max_heap and len(visited) < MAX_PAGES:
            # Pop URL with highest priority
            priority, url, depth = heappop(max_heap)

            # Extract domain from URL
            domain = urlsplit(url).netloc
            
            # Fetch and validate page
            html, meta = fetch_page(url)

            # Count timeout-related failures (status 0 and no content)
            if meta['status_code'] == 0 and meta['content_length'] == 0:
                timeout_counts[domain] = timeout_counts.get(domain, 0) + 1
            
            # Log result
            log_url(log, url, meta, depth, -priority)

            # Update crawl stats
            visited.add(url)
            total_bytes += meta['content_length']
            status_counts[meta['status_code']] = status_counts.get(meta['status_code'], 0) + 1

            # Skip link extraction if fetch failed or content missing (or not html)
            if meta['status_code'] != 200 or not html: continue

            # Increment successful crawl count per domain
            crawl_counts[domain] = crawl_counts.get(domain, 0) + 1

            # Extract and enqueue child links
            links = extract_links(html, url)
            for link in links:
                # Normalize URL: strip query, fragment, and trailing slash
                link = clean_url(link)
                link_domain = urlsplit(link).netloc # Extract domain

                # Skip if already handled or invalid
                if (
                    link in scheduled           # Already in heap
                    or link in visited          # Already fetched
                    or link in disallowed       # Blocked by robots.txt
                    or not validate_url(link)   # Invalid URL (not http/https)
                    or timeout_counts.get(link_domain, 0) >= MAX_TIMEOUTS   # Domain exceeded failure limit
                ):
                    continue

                # Check robots.txt permission
                if not is_allowed(link):
                    disallowed.add(link)
                    print('Skipping', link)
                    continue

                # Compute priority and enqueue
                link_priority = compute_priority(crawl_counts.get(link_domain, 0))
                heappush(max_heap, (-link_priority, link, depth + 1))
                scheduled.add(link)
        
        # Log crawl summary
        total_time = time() - start_time
        log_summary(log, len(visited), total_bytes, total_time, status_counts, crawl_counts)

if __name__ == '__main__':
    main()