from collections import deque

from input.seed import get_seeds
from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links
from utils.url import clean_url, log_url

def main():
    seeds = get_seeds()
    queue = deque(deque((url, 0) for url in seeds)) # (url, depth)
    visited = set()

    with open('log.txt', 'w') as log:
        while queue:
            url, depth = queue.popleft()
            if url in visited: continue
            
            if not is_allowed(url):
                print('Skipping', url)
                continue

            print('Fetching', url)
            html, meta = fetch_page(url)
            if not html or meta['status_code'] != 200: continue
            
            visited.add(url)
            log_url(log, url, meta, depth)

            links = extract_links(html, url)
            for link in links:
                link = clean_url(link)
                if link in visited: continue
                queue.append((link, depth + 1))

if __name__ == '__main__':
    main()