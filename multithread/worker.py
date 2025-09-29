from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit

from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url, validate_url
from utils.priority import compute_priority
from logger.log import log_url

def crawl_page(item, state, log):
    priority, url, depth = item

    # Skip if already visited (thread-safe early check)
    if url in state['visited']: return None
    state['visited'].add(url)
    
    # Extract domain from URL
    domain = urlsplit(url).netloc

    # Fetch and validate page
    html, meta = fetch_page(url)

    # Count timeout-related failures (status 0 and no content)
    if meta['status_code'] == 0 and meta['content_length'] == 0:
        state['timeout_counts'][domain] = state['timeout_counts'].get(domain, 0) + 1

    # Log result
    log_url(log, url, meta, depth, -priority)

    # Update crawl stats
    state['total_bytes'] += meta['content_length']
    state['status_counts'][meta['status_code']] = state['status_counts'].get(meta['status_code'], 0) + 1

    # Skip link extraction if fetch failed or content missing (or not html)
    if meta['status_code'] != 200 or not html: return None

    # Increment successful crawl count per domain
    state['crawl_counts'][domain] = state['crawl_counts'].get(domain, 0) + 1

    # Extract and enqueue child links
    result = []
    links = extract_links(html, url)
    for link in links:
        # Normalize URL: strip query, fragment, and trailing slash
        link = clean_url(link)
        link_domain = urlsplit(link).netloc # Extract domain

        # Skip if already handled or invalid
        if (
            link in state['scheduled']      # Already in heap
            or link in state['visited']     # Already fetched
            or link in state['disallowed']  # Blocked by robots.txt
            or not validate_url(link)       # Invalid URL (not http/https)
            or state['timeout_counts'].get(link_domain, 0) >= state['max_timeouts'] # Domain exceeded failure limit
        ):
            continue

        # Check robots.txt permission
        if not is_allowed(link, state['robots_cache']):
            state['disallowed'].add(link)
            print('Skipping', link)
            continue

        # Compute priority and enqueue
        priority = compute_priority(state['crawl_counts'].get(link_domain, 0))
        result.append((-priority, link, depth + 1))
        state['scheduled'].add(link)

    # Return new links
    return result

def crawl_pages(batch, shared_state, log, num_threads):
    result = []

    # Create thread pool with fixed number of worker threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all URLs in the batch to be processed in parallel
        futures = [executor.submit(crawl_page, item, shared_state, log) for item in batch]

        # Wait for all futures to complete
        for future in as_completed(futures):
            # Add new links from each thread
            links = future.result()
            if links: result.extend(links)
    
    # Return all new links
    return result