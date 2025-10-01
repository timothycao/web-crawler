from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit

from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url, get_superdomain, is_valid_url, is_cgi_url, is_blocked_extension
from utils.priority import compute_priority
from logger.log import log_url
from config import DEBUG

def crawl_page(item, state, log):
    priority, url, depth = item

    # Skip if already visited (thread-safe early check)
    with state['visited_lock']:
        if url in state['visited']: return []
        state['visited'].add(url)
    
    # Extract domain
    domain = urlsplit(url).netloc

    # Fetch and validate page
    html, meta = fetch_page(url)

    # Count timeout-related failures (status 0 and no content)
    if meta['status_code'] == 0 and meta['content_length'] == 0:
        with state['timeout_counts_lock']:
            state['timeout_counts'][domain] = state['timeout_counts'].get(domain, 0) + 1

    # Log result
    log_url(log, url, meta, depth, -priority)

    # Update crawl stats
    with state['total_bytes_lock']:
        state['total_bytes'] += meta['content_length']
    with state['status_counts_lock']:
        state['status_counts'][meta['status_code']] = state['status_counts'].get(meta['status_code'], 0) + 1

    # Skip link extraction if fetch failed or not html
    if meta['status_code'] != 200 or not html: return []

    # Increment successful crawl count per domain
    with state['domain_crawl_counts_lock']:
        state['domain_crawl_counts'][domain] = state['domain_crawl_counts'].get(domain, 0) + 1

    # Extract and enqueue child links
    result = []
    links = extract_links(html, url)
    for link in links:
        # Normalize URL: strip query, fragment, and trailing slash
        link = clean_url(link)

        # Extract domain and superdomain
        link_domain = urlsplit(link).netloc
        link_superdomain = get_superdomain(link)
        
        # Skip if invalid (bad scheme, CGI path, or blocked extension)
        if not is_valid_url(link) or is_cgi_url(link) or is_blocked_extension(link):
            if DEBUG:
                print('Skipping', link)
                with state['skipped_invalid_lock']:
                    state['skipped_invalid'] += 1
            continue

        # Skip if already scheduled (in heap)
        with state['scheduled_lock']:
            if link in state['scheduled']:
                if DEBUG:
                    print('Skipping', link)
                    with state['skipped_dupes_lock']:
                        state['skipped_dupes'] += 1
                continue

        # Skip if already fetched
        with state['visited_lock']:
            if link in state['visited']:
                if DEBUG:
                    print('Skipping', link)
                    with state['skipped_dupes_lock']:
                        state['skipped_dupes'] += 1
                continue

        # Skip if already in robots block list
        with state['disallowed_lock']:
            if link in state['disallowed']:
                if DEBUG:
                    print('Skipping', link)
                    with state['skipped_robots_lock']:
                        state['skipped_robots'] += 1
                continue

        # Skip if domain already exceeded timeout failure limit
        with state['timeout_counts_lock']:
            if state['timeout_counts'].get(link_domain, 0) >= state['max_timeouts']:
                if DEBUG:
                    print('Skipping', link)
                    with state['skipped_timeout_lock']:
                        state['skipped_timeout'] += 1
                continue

        # Skip if blocked by robots.txt
        with state['robots_cache_lock']:
            allowed = is_allowed(link, state['robots_cache'])
        if not allowed:
            with state['disallowed_lock']:
                state['disallowed'].add(link)
            if DEBUG:
                print('Skipping', link)
                with state['skipped_robots_lock']:
                    state['skipped_robots'] += 1
            continue

        # Get the domain's current crawl count
        with state['domain_crawl_counts_lock']:
            domain_crawl_count = state['domain_crawl_counts'].get(link_domain, 0)

        # Add domain to its superdomain set and get unique count
        with state['superdomain_domains_lock']:
            state['superdomain_domains'][link_superdomain].add(link_domain)
            superdomain_domain_count = len(state['superdomain_domains'][link_superdomain])

        # Compute priority
        priority = compute_priority(domain_crawl_count, superdomain_domain_count)
        
        # Enqueue
        with state['scheduled_lock']:
            state['scheduled'].add(link)
        result.append((-priority, link, depth + 1))

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
            result.extend(future.result())
    
    # Return all new links
    return result