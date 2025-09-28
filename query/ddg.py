from requests import post
from bs4 import BeautifulSoup

DUCKDUCKGO_SEARCH_URL = 'https://html.duckduckgo.com/html/'
HEADERS = {
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

def query_ddg(query: str, max_results: int = 10) -> list[str]:
    results = []
    
    try:
        print(f'Querying DuckDuckGo: "{query}"')
        response = post(DUCKDUCKGO_SEARCH_URL, headers=HEADERS, data={'q': query}, timeout=5)
        response.raise_for_status() # Raise if 4xx/5xx error
    except Exception as e:
        print(f'[ERROR] Failed to fetch search results: {e}')
        return results

    # Parse raw HTML into searchable DOM
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract result URLs
    for tag in soup.find_all('a', class_='result__a', href=True):
        href = tag['href']
        if href.startswith('https://duckduckgo.com/y.js'): continue # skip ads
        if href.startswith('http'): results.append(href)
        if len(results) >= max_results: break

    # print('results:', results)
    return results

if __name__ == '__main__':
    query = 'dogs and cats'
    query_ddg(query, max_results=10)