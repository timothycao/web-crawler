from asyncio import run, get_event_loop
from heapq import heappush, heappop

from aiohttp import ClientSession

from query.ddg import query_ddg
from fetcher.robots import is_allowed
from utils.url import clean_url, validate_url
from logger.log import log_summary
from asynchronous.worker import crawl_pages

QUERY = 'dogs and cats'
MAX_PAGES = 100
MAX_TIMEOUTS = 2 # Max allowed fetch failures per domain (status 0, no content)
MAX_CONCURRENT_REQUESTS = 16

async def main():
    shared_state = {
        # Limits
        'max_timeouts': MAX_TIMEOUTS,
        
        # URL states
        'scheduled': set(),     # URLs scheduled to be visited (in heap)
        'visited': set(),       # URLs that were fetched
        'disallowed': set(),    # URLs blocked by robots.txt
        'robots_cache': {},     # Stores robot parser per domain
        'timeout_counts': {},   # Count timeout-related fetch failures per domain
        
        # Stats (for log)
        'total_bytes': 0,       # Total bytes of fetched pages
        'status_counts': {},    # Count responses per HTTP status code
        'crawl_counts': {},     # Count pages successfully crawled per domain
        'skipped_invalid': 0,   # Total invalid URLs skipped
        'skipped_dupes': 0,     # Total duplicate URLs skipped
        'skipped_robots': 0,    # Total robots-blocked URLs skipped
        'skipped_timeout': 0,   # Total URLs skipped due to timeout failures
    }
    
    seeds = query_ddg(QUERY, max_results=10)
    max_heap = [] # Simulated max-heap using -priority
    
    # Seed initial crawl queue
    for seed in seeds:
        # Normalize URL: strip query, fragment, and trailing slash
        seed = clean_url(seed)

        # Skip if already handled or invalid
        if (
            seed in shared_state['scheduled']       # Already in heap
            or seed in shared_state['disallowed']   # Blocked by robots.txt
            or not validate_url(seed)               # Invalid URL (not http/https)
        ):
            continue
        
        # Check robots.txt permission
        if not is_allowed(seed, shared_state['robots_cache']):
            shared_state['disallowed'].add(seed)
            print('Skipping', seed)
            continue
        
        heappush(max_heap, (0, seed, 0)) # (-priority, url, depth)
        shared_state['scheduled'].add(seed)
    
    start_time = get_event_loop().time()
    log = open('log_async.txt', 'w')

    async with ClientSession() as session:
        # Crawl loop using max heap (priority-based BFS)
        while max_heap and len(shared_state['visited']) < MAX_PAGES:
            # Prepare batch of next URLs to fetch
            batch = []
            while max_heap and len(batch) < MAX_CONCURRENT_REQUESTS and len(shared_state['visited']) + len(batch) < MAX_PAGES:
                batch.append(heappop(max_heap))

            # Fetch concurrently
            links = await crawl_pages(batch, shared_state, log, session)

            # Enqueue new links
            for link in links:
                heappush(max_heap, link)

    # Log crawl summary
    total_time = get_event_loop().time() - start_time
    log_summary(log, shared_state, total_time)
    log.close()

if __name__ == '__main__':
    run(main())