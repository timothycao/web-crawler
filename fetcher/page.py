from urllib.request import urlopen, Request
from datetime import datetime, timezone
from io import BytesIO
import gzip

def fetch_page(url):
    headers = {
        'Accept': 'text/html',
        # Browser headers to avoid blocks
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }

    meta = {
        'status_code': 0,
        'content_length': 0,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    try:
        print(f'Fetching {url}')

        # Send request
        request = Request(url, headers=headers)
        response = urlopen(request)
        meta['status_code'] = response.getcode() or 0
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()

        # Skip non-HTML content (i.e. image, pdf, etc.)
        if 'text/html' not in response.headers.get('Content-Type', ''):
            print('Skipping non-html content')
            return None, meta
        
        # Read raw response
        raw_bytes = response.read()
        
        # Decompress if gzipped
        if response.headers.get('Content-Encoding') == 'gzip':
            print('Decompressing gzip content')
            buffer = BytesIO(raw_bytes)
            raw_bytes = gzip.GzipFile(fileobj=buffer).read() # decompressed
        
        # Decode to HTML
        html = raw_bytes.decode('utf-8')
        meta['content_length'] = len(raw_bytes)
        return html, meta
    
    except Exception as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta