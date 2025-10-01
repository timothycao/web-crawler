from time import time
from collections import defaultdict
from heapq import heappush
from socket import setdefaulttimeout
# from threading import Lock

from query.ddg import query_ddg
from fetcher.robots import is_allowed
from utils.url import clean_url, is_valid_url, is_cgi_url, is_blocked_extension
from logger.log import log_summary
from multithread.worker import crawl_with_workers
from config import QUERY, MAX_PAGES, MAX_TIME, MAX_TIMEOUTS, NUM_THREADS, DEBUG

# Set timeout for all socket operations (e.g. urlopen, RobotFileParser.read)
setdefaulttimeout(5)

def main():
    state = {
        # Limits
        'max_timeouts': MAX_TIMEOUTS,
        
        # URL states
        'scheduled': set(),     # URLs scheduled to be visited (in heap)
        'visited': set(),       # URLs that were fetched
        'disallowed': set(),    # URLs blocked by robots.txt
        'robots_cache': {},     # Stores robot parser per domain
        'timeout_counts': {},   # Count timeout-related fetch failures per domain
        
        # Stats (for log)
        'total_bytes': 0,                           # Total bytes of fetched pages
        'status_counts': {},                        # Count responses per HTTP status code
        'domain_crawl_counts': {},                  # Count pages successfully crawled per domain
        'superdomain_domains': defaultdict(set),    # Track unique domains under each superdomain

        # Exit flag
        'exit': False,  # Global exit flag for workers

        # Locks
        # 'scheduled_lock': Lock(),
        # 'visited_lock': Lock(),
        # 'disallowed_lock': Lock(),
        # 'robots_cache_lock': Lock(),
        # 'timeout_counts_lock': Lock(),
        # 'total_bytes_lock': Lock(),
        # 'status_counts_lock': Lock(),
        # 'domain_crawl_counts_lock': Lock(),
        # 'superdomain_domains_lock': Lock(),
    }

    if DEBUG:
        state.update({
            'skipped_invalid': 0,   # Total invalid URLs skipped
            'skipped_dupes': 0,     # Total duplicate URLs skipped
            'skipped_robots': 0,    # Total robots-blocked URLs skipped
            'skipped_timeout': 0,   # Total URLs skipped due to timeout failures
            # 'skipped_invalid_lock': Lock(),
            # 'skipped_dupes_lock': Lock(),
            # 'skipped_robots_lock': Lock(),
            # 'skipped_timeout_lock': Lock(),
        })

    # Fetch seed URLs
    seeds = query_ddg(QUERY, max_results=10)

    # Seed initial crawl queue
    max_heap = [] # Simulated max-heap using -priority
    for seed in seeds:
        # Normalize URL: strip query, fragment, and trailing slash
        seed = clean_url(seed)

        # Skip if already handled or invalid
        if (
            seed in state['scheduled']      # Already in heap
            or seed in state['disallowed']  # Blocked by robots.txt
            or not is_valid_url(seed)       # Invalid scheme
            or is_cgi_url(seed)             # CGI script
            or is_blocked_extension(seed)   # Blocked extension
        ):
            continue
        
        # Skip if already in robots block list
        if not is_allowed(seed, state['robots_cache']):
            state['disallowed'].add(seed)
            if DEBUG: print('Skipping', seed)
            continue
        
        heappush(max_heap, (0, seed, 0)) # (-priority, url, depth)
        state['scheduled'].add(seed)

    # Start crawl timer and open log
    start_time = time()
    log = open('log.txt', 'w')

    # Launch worker thread pool
    crawl_with_workers(max_heap, state, log, NUM_THREADS, MAX_PAGES, MAX_TIME, start_time)

    # Log crawl summary
    total_time = time() - start_time
    log_summary(log, state, total_time)
    log.close()

if __name__ == '__main__':
    main()