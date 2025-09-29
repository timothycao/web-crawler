from time import time
from heapq import heappush, heappop
from socket import setdefaulttimeout
from threading import Lock

from query.ddg import query_ddg
from fetcher.robots import is_allowed
from utils.url import clean_url, validate_url
from logger.log import log_summary
from multithread.worker import crawl_pages

QUERY = 'dogs and cats'
MAX_PAGES = 1000
MAX_TIMEOUTS = 2 # Max allowed fetch failures per domain (status 0, no content)
NUM_THREADS = 16

# Set timeout for all socket operations (e.g. urlopen, RobotFileParser.read)
setdefaulttimeout(5)

def main():
    # Crawl state initialization
    seeds = query_ddg(QUERY, max_results=10)
    max_heap = []                                       # Simulated max-heap using -priority
    scheduled, scheduled_lock = set(), Lock()           # URLs scheduled to be visited (in heap)
    visited, visited_lock = set(), Lock()               # URLs that were fetched
    disallowed, disallowed_lock = set(), Lock()         # URLs blocked by robots.txt
    robots_cache, robots_cache_lock = {}, Lock()        # Stores robot parser per domain
    timeout_counts, timeout_counts_lock = {}, Lock()    # Count timeout-related fetch failures per domain
    
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
        if not is_allowed(seed, robots_cache):
            disallowed.add(seed)
            print('Skipping', seed)
            continue
        
        heappush(max_heap, (0, seed, 0)) # (-priority, url, depth)
        scheduled.add(seed)
    
    # Crawl statistics
    total_bytes, total_bytes_lock = 0, Lock()       # Total bytes of fetched pages
    status_counts, status_counts_lock = {}, Lock()  # Count responses per HTTP status code
    crawl_counts, crawl_counts_lock = {}, Lock()    # Count pages successfully crawled per domain
    start_time = time()

    with open('log.txt', 'w') as log:
        # Crawl loop using max heap (priority-based BFS)
        while max_heap and len(visited) < MAX_PAGES:
            # Prepare batch of next URLs to fetch
            batch = []
            while max_heap and len(batch) < NUM_THREADS and len(visited) + len(batch) < MAX_PAGES:
                batch.append(heappop(max_heap))

            # Shared mutable state across threads
            shared_state = {
                'visited': visited,
                'scheduled': scheduled,
                'disallowed': disallowed,
                'robots_cache': robots_cache,
                'timeout_counts': timeout_counts,
                'status_counts': status_counts,
                'crawl_counts': crawl_counts,
                'total_bytes': total_bytes,
                'max_timeouts': MAX_TIMEOUTS,

                # locks
                'scheduled_lock': scheduled_lock,
                'visited_lock': visited_lock,
                'disallowed_lock': disallowed_lock,
                'robots_cache_lock': robots_cache_lock,
                'timeout_counts_lock': timeout_counts_lock,
                'status_counts_lock': status_counts_lock,
                'crawl_counts_lock': crawl_counts_lock,
                'total_bytes_lock': total_bytes_lock,
            }

            # Fetch in parallel
            links = crawl_pages(batch, shared_state, log, NUM_THREADS)

            # Update stats (batch modifies shared_state directly, but we sync total_bytes here)
            total_bytes = shared_state['total_bytes']

            # Enqueue new links
            for link in links:
                heappush(max_heap, link)

        # Log crawl summary
        total_time = time() - start_time
        log_summary(log, len(visited), total_bytes, total_time, status_counts, crawl_counts)

if __name__ == '__main__':
    main()