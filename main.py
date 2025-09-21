from collections import deque

from input.seed import get_seeds
from fetcher.page import fetch_page
from fetcher.robots import is_allowed
from parser.html import extract_links

def main():
    seeds = get_seeds()
    queue, visited = deque(seeds), set()

    with open('log.txt', 'w') as log:
        while queue:
            url = queue.popleft()
            if url in visited: continue
            
            if not is_allowed(url):
                print('Skipping', url)
                continue

            print('Fetching', url)
            html = fetch_page(url)
            if not html: continue
            
            visited.add(url)
            log.write(url + '\n')

            links = extract_links(html, url)
            for link in links:
                if link in visited: continue
                queue.append(link)
        

if __name__ == '__main__':
    main()