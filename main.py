from time import time
from heapq import heappush, heappop
from socket import setdefaulttimeout

from input.seed import get_seeds
from fetcher.robots import is_allowed
from utils.url import clean_url, validate_url
from logger.log import log_summary
from multithread.worker import crawl_pages

MAX_PAGES = 100
MAX_TIMEOUTS = 2 # Max allowed fetch failures per domain (status 0, no content)
NUM_THREADS = 16

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
            # Prepare batch of next URLs to fetch
            batch = []
            while max_heap and len(batch) < NUM_THREADS and len(visited) + len(batch) < MAX_PAGES:
                batch.append(heappop(max_heap))

            # Shared mutable state across threads
            shared_state = {
                'visited': visited,
                'scheduled': scheduled,
                'disallowed': disallowed,
                'timeout_counts': timeout_counts,
                'status_counts': status_counts,
                'crawl_counts': crawl_counts,
                'total_bytes': total_bytes,
                'max_timeouts': MAX_TIMEOUTS,
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