from bs4 import BeautifulSoup
from urllib.parse import urljoin

from config import DEBUG

def extract_links(html, base_url):
    links = []
    
    # Parse raw HTML into searchable DOM
    soup = BeautifulSoup(html, 'html.parser')

    # Resolve all relative hrefs to full URLs
    for tag in soup.find_all('a', href=True):
        try:
            # print(tag)
            full_url = urljoin(base_url, tag['href'])
            links.append(full_url)
        except Exception as e:
            if DEBUG: print(f'[WARNING] Skipping malformed link {tag["href"]}: {e}')
            continue
    
    return links