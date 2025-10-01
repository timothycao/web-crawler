from time import time
from urllib.parse import urlsplit
from heapq import heappush, heappop
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url, get_superdomain, is_valid_url, is_cgi_url, is_blocked_extension
from utils.priority import compute_priority
from logger.log import log_url
from config import DEBUG

def crawl(item, state, log, max_pages, max_time, start_time):
    # Exit early if max config reached
    if state['exit'] or len(state['visited']) >= max_pages or time() - start_time >= max_time:
        state['exit'] = True
        return []

    # Unpack item from heap
    priority, url, depth = item

    # Fetch first to resolve any redirects
    final_url, html, meta = fetch_page(url)
    final_url = clean_url(final_url)

    # Extract domain and superdomain
    domain = urlsplit(final_url).netloc
    superdomain = get_superdomain(final_url)
    
    # Skip if already in robots block list
    if not is_allowed(final_url, state['robots_cache']):
        state['disallowed'].add(final_url)
        if DEBUG: print('Skipping', final_url)
        return []
    
    # Skip if already visited
    if final_url in state['visited']:
        if DEBUG: print('Skipping', final_url)
        return []
    state['visited'].add(final_url)

    # Track timeout failures
    if meta['status_code'] == 0 and meta['content_length'] == 0:
        state['timeout_counts'][domain] = state['timeout_counts'].get(domain, 0) + 1

    # Log result
    log_url(log, final_url, meta, depth, -priority)

    # Update crawl stats
    state['total_bytes'] += meta['content_length']
    state['status_counts'][meta['status_code']] = state['status_counts'].get(meta['status_code'], 0) + 1

    # Skip link extraction if fetch failed or not html
    if meta['status_code'] != 200 or not html: return []

    # Track domain crawl count
    state['domain_crawl_counts'][domain] = state['domain_crawl_counts'].get(domain, 0) + 1
    state['superdomain_domains'][superdomain].add(domain)

    # Exit if max config reached
    if state['exit'] or len(state['visited']) >= max_pages or time() - start_time >= max_time:
        state['exit'] = True
        return []

    # Extract and enqueue child links
    result = []
    for link in extract_links(html, final_url):
        # Normalize URL: strip query, fragment, and trailing slash
        link = clean_url(link)

        # Extract domain and superdomain
        link_domain = urlsplit(link).netloc
        link_superdomain = get_superdomain(link)
        
        # Skip if invalid (bad scheme, CGI path, or blocked extension)
        if not is_valid_url(link) or is_cgi_url(link) or is_blocked_extension(link):
            if DEBUG:
                print('Skipping', link)
                state['skipped_invalid'] += 1
            continue

        # Skip if already scheduled (in heap)
        if link in state['scheduled']:
            if DEBUG:
                print('Skipping', link)
                state['skipped_dupes'] += 1
            continue

        # Skip if already fetched
        if link in state['visited']:
            if DEBUG:
                print('Skipping', link)
                state['skipped_dupes'] += 1
            continue

        # Skip if already in robots block list
        if link in state['disallowed']:
            if DEBUG:
                print('Skipping', link)
                state['skipped_robots'] += 1
            continue

        # Skip if domain already exceeded timeout failure limit
        if state['timeout_counts'].get(link_domain, 0) >= state['max_timeouts']:
            if DEBUG:
                print('Skipping', link)
                state['skipped_timeout'] += 1
            continue

        # Skip if blocked by robots.txt
        if not is_allowed(link, state['robots_cache']):
            state['disallowed'].add(link)
            if DEBUG:
                print('Skipping', link)
                state['skipped_robots'] += 1
            continue

        # Track domain crawl count and compute priority
        state['superdomain_domains'][link_superdomain].add(link_domain)
        domain_crawl_count = state['domain_crawl_counts'].get(link_domain, 0)
        superdomain_domain_count = len(state['superdomain_domains'][link_superdomain])
        priority = compute_priority(domain_crawl_count, superdomain_domain_count)
        
        # Enqueue
        state['scheduled'].add(link)
        result.append((-priority, link, depth + 1))

    # Return new links
    return result

def crawl_with_workers(max_heap, state, log, num_threads, max_pages, max_time, start_time):
    # Create thread pool with fixed number of workers
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = set()
        
        # Fill the initial batch of worker tasks
        _fill_worker_pool(futures, max_heap, executor, state, log, num_threads, max_pages, max_time, start_time)

        # Main thread loop (wait for tasks to complete, refill as needed)
        while futures:
            # Wait for at least one task to finish
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            
            # Enqueue new links from each task
            for future in done:
                for link in future.result():
                    heappush(max_heap, link)
            
            # Refill worker pool
            _fill_worker_pool(futures, max_heap, executor, state, log, num_threads, max_pages, max_time, start_time)

def _fill_worker_pool(futures, max_heap, executor, state, log, max_threads, max_pages, max_time, start_time):
    # Fill up thread pool to capacity
    while len(futures) < max_threads:
        # Exit early if no more URLs to crawl
        if not max_heap: break
        
        # Exit if max limits reached
        page_count, total_time = len(state['visited']), time() - start_time
        if page_count >= max_pages or total_time >= max_time:
            print(f'[EXIT] Reached limit — fetched {page_count} pages in {total_time:.2f} seconds')
            state['exit'] = True
            break

        # Pop next item and clean from scheduled set
        priority, url, depth = heappop(max_heap)
        state['scheduled'].discard(url)
        if url in state['visited']: continue
        
        # Submit crawl task to the executor for concurrent execution
        futures.add(executor.submit(crawl, (priority, url, depth), state, log, max_pages, max_time, start_time))