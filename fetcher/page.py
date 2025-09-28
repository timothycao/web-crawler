from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from aiohttp import ClientResponseError, ClientConnectorError
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
        request = Request(url, headers=headers)
        response = urlopen(request)
    
    # Handle HTTP response errors (e.g. 404, 403, 500)
    except HTTPError as e:
        meta['status_code'] = e.code
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta

    # Handle network-level errors (e.g. DNS failure, connection timeout)
    except URLError as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta
    
    # Handle unexpected errors
    except Exception as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta
        
    # Update metadata after successful fetch
    meta['status_code'] = response.getcode() or 0
    meta['timestamp'] = datetime.now(timezone.utc).isoformat()

    # Skip non-HTML content (e.g. image, pdf, etc.)
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

async def fetch_page_async(url, session):
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
        async with session.get(url, headers=headers, timeout=5) as response:
            # Update metadata after successful fetch
            meta['status_code'] = response.status
            meta['timestamp'] = datetime.now(timezone.utc).isoformat()

            # Skip non-HTML content (e.g. image, pdf, etc.)
            if 'text/html' not in response.headers.get('Content-Type', ''):
                print('Skipping non-html content')
                return None, meta

            # Read raw response
            raw_bytes = await response.read()

            # aiohttp handles gzip decompression

            # Decode to HTML
            html = raw_bytes.decode('utf-8')
            meta['content_length'] = len(raw_bytes)
            return html, meta

    # Handle HTTP response errors (e.g. 404, 403, 500)
    except ClientResponseError as e:
        meta['status_code'] = e.status
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta

    # Handle network-level errors (e.g. DNS failure, connection timeout)
    except ClientConnectorError as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta
    
    # Handle unexpected errors
    except Exception as e:
        print(f'[ERROR] Failed to fetch {url}: {e}')
        return None, meta