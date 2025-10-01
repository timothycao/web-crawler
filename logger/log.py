from collections import defaultdict

from utils.url import get_superdomain
from config import DEBUG

def log_url(log_file, url, meta, depth, priority):
    line = f'{url}\t{meta["timestamp"]}\t{meta["content_length"]}\t{depth}\t{meta["status_code"]}\t{priority:.6f}\n'
    log_file.write(line)
    log_file.flush() # immediately write to disk

def log_summary(log_file, state, total_time):
    total_pages = len(state['visited'])
    total_bytes = state['total_bytes']
    status_counts = state['status_counts']
    domain_crawl_counts = state['domain_crawl_counts']

    log_file.write('\nFetch Summary:\n')
    log_file.write(f'Total pages: {total_pages}\n')
    log_file.write(f'Total size: {total_bytes} bytes\n')
    log_file.write(f'Total time: {total_time:.2f} seconds\n')
    for status_code, count in status_counts.items():
        log_file.write(f'{status_code} responses: {count}\n')
    
    if DEBUG:
        skipped_invalid = state['skipped_invalid']
        skipped_dupes = state['skipped_dupes']
        skipped_robots = state['skipped_robots']
        skipped_timeout = state['skipped_timeout']

        log_file.write('\nSkip Summary:\n')
        log_file.write(f'Invalid URLs: {skipped_invalid}\n')
        log_file.write(f'Duplicates: {skipped_dupes}\n')
        log_file.write(f'Blocked by robots.txt: {skipped_robots}\n')
        log_file.write(f'Timeout failures: {skipped_timeout}\n')  

    # Aggregate domain counts into superdomains
    superdomain_crawl_counts = defaultdict(int)
    for domain, count in domain_crawl_counts.items():
        superdomain = get_superdomain(domain)
        superdomain_crawl_counts[superdomain] += count

    log_file.write(f'\nTotal pages crawled (status 200 and html): {sum(domain_crawl_counts.values())}\n')
    for superdomain, count in sorted(superdomain_crawl_counts.items(), key=lambda x: -x[1]):
        log_file.write(f'{superdomain}: {count}\n')
    
    log_file.flush()