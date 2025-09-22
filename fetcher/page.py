from urllib.request import urlopen, Request
from io import BytesIO
import gzip

def fetch_page(url):
    headers = {
        'Accept': 'text/html',
        # Browser headers to avoid blocks
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }

    try:
        request = Request(url, headers=headers)
        response = urlopen(request)

        # Skip non-HTML content (i.e. image, pdf, etc.)
        if 'text/html' not in response.headers.get('Content-Type', ''):
            print('Skipping non-html content')
            return
        
        # Decompress gzip if needed
        if response.headers.get('Content-Encoding') == 'gzip':
            print('Decompressing gzip content')
            raw_bytes = response.read()
            buffer = BytesIO(raw_bytes)
            decompressed = gzip.GzipFile(fileobj=buffer)
            return decompressed.read().decode('utf-8')
        
        return response.read().decode('utf-8')
    
    except Exception as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')