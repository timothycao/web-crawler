from asyncio import gather
from urllib.parse import urlsplit

from fetcher.page import fetch_page_async
from fetcher.robots import is_allowed_async
from parser.html import extract_links
from utils.url import clean_url, validate_url
from utils.priority import compute_priority
from logger.log import log_url

async def crawl_page(item, state, log, session):
    priority, url, depth = item

    # Skip if already visited
    if url in state['visited']: return []
    state['visited'].add(url)

    # Extract domain from URL
    domain = urlsplit(url).netloc

    # Fetch page asynchronously
    html, meta = await fetch_page_async(url, session)

    # Count timeout-related failures (status 0 and no content)
    if meta['status_code'] == 0 and meta['content_length'] == 0:
        state['timeout_counts'][domain] = state['timeout_counts'].get(domain, 0) + 1

    # Log result
    log_url(log, url, meta, depth, -priority)
    
    # Update crawl stats
    state['total_bytes'] += meta['content_length']
    state['status_counts'][meta['status_code']] = state['status_counts'].get(meta['status_code'], 0) + 1

    # Skip link extraction if fetch failed or content missing (or not html)
    if meta['status_code'] != 200 or not html: return []

    # Increment successful crawl count per domain
    state['crawl_counts'][domain] = state['crawl_counts'].get(domain, 0) + 1

    # Extract and enqueue child links
    result = []
    links = extract_links(html, url)
    for link in links:
        # Normalize URL: strip query, fragment, and trailing slash
        link = clean_url(link)
        link_domain = urlsplit(link).netloc # Extract domain

        # Skip if invalid URL (not http/https)
        if not validate_url(link):
            state['skipped_invalid'] += 1
            print('Skipping', link)
            continue
        
        # Skip if already scheduled (in heap)
        if link in state['scheduled']:
            state['skipped_dupes'] += 1
            print('Skipping', link)
            continue

        # Skip if already fetched
        if link in state['visited']:
            state['skipped_dupes'] += 1
            print('Skipping', link)
            continue

        # Skip if already in robots block list
        if link in state['disallowed']:
            state['skipped_robots'] += 1
            print('Skipping', link)
            continue

        # Skip if domain already exceeded timeout failure limit
        if state['timeout_counts'].get(link_domain, 0) >= state['max_timeouts']:
            state['skipped_timeout'] += 1
            print('Skipping', link)
            continue

        # Skip if blocked by robots.txt
        if not await is_allowed_async(link, state['robots_cache'], session):
            state['disallowed'].add(link)
            state['skipped_robots'] += 1
            print('Skipping', link)
            continue

        # Compute priority and enqueue
        priority = compute_priority(state['crawl_counts'].get(link_domain, 0))
        result.append((-priority, link, depth + 1))
        state['scheduled'].add(link)

    # Return new links
    return result

async def crawl_pages(batch, state, log, session):
    result = []

    # Submit all URLs in the batch to be processed concurrently
    tasks = [crawl_page(item, state, log, session) for item in batch]
    
    # Wait for all tasks to complete
    for links in await gather(*tasks):
        # Add new links from each task
        result.extend(links)

    # Return all new links
    return result