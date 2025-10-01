from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone
from io import BytesIO
from gzip import GzipFile
from re import search, IGNORECASE
from socket import timeout

from aiohttp import ClientResponseError, ClientConnectorError
from chardet import detect

from config import DEBUG

HEADERS = {
    'Accept': 'text/html',
    # Browser headers to avoid blocks
    'Accept-Language': 'en-US,en;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

def _detect_encoding(raw_bytes, content_type):
    # Get charset from Content-Type header if present
    charset_match = search(r'charset=([^\s;]+)', content_type, IGNORECASE)
    if charset_match: return charset_match.group(1)
    
    # Otherwise guess encoding from raw bytes
    detected = detect(raw_bytes)
    
    # Fallback to UTF-8
    return detected['encoding'] or 'utf-8'

def fetch_page(url):
    meta = {
        'status_code': 0,
        'content_length': 0,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    try:
        print('Fetching', url)
        request = Request(url, headers=HEADERS)
        response = urlopen(request)
        final_url = response.geturl() # resolved URL after any redirects

        # Update metadata after successful fetch
        meta['status_code'] = response.getcode() or 0
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()

        # Skip non-HTML content (e.g. image, pdf, etc.)
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' not in content_type:
            if DEBUG: print('Skipping non-html content')
            return final_url, None, meta
        
        # Read raw response
        try:
            raw_bytes = response.read()
        except timeout:
            if DEBUG: print(f'[TIMEOUT] Reading from {url} took too long')
            return final_url, None, meta
        
        # Decompress if gzipped
        if response.headers.get('Content-Encoding') == 'gzip':
            if DEBUG: print('Decompressing gzip content')
            buffer = BytesIO(raw_bytes)
            raw_bytes = GzipFile(fileobj=buffer).read() # decompressed
        
        # Detect encoding
        encoding = _detect_encoding(raw_bytes, content_type)
        try:
            # Decode to HTML
            html = raw_bytes.decode(encoding, errors='replace')
        except Exception as e:
            if DEBUG: print(f'[ERROR] Failed to decode {url} using {encoding}: {e}')
            return final_url, None, meta
        
        meta['content_length'] = len(raw_bytes)
        return final_url, html, meta
    
    # Handle HTTP response errors (e.g. 404, 403, 500)
    except HTTPError as e:
        meta['status_code'] = e.code
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta

    # Handle network-level errors (e.g. DNS failure, connection timeout)
    except URLError as e:
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta
    
    # Handle unexpected errors
    except Exception as e:
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta

async def fetch_page_async(url, session):
    meta = {
        'status_code': 0,
        'content_length': 0,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    try:
        print('Fetching', url)
        async with session.get(url, headers=HEADERS, timeout=5) as response:
            final_url = str(response.url) # resolved URL after any redirects
            
            # Update metadata after successful fetch
            meta['status_code'] = response.status
            meta['timestamp'] = datetime.now(timezone.utc).isoformat()

            # Skip non-HTML content (e.g. image, pdf, etc.)
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                if DEBUG: print('Skipping non-html content')
                return final_url, None, meta

            # Read raw response
            raw_bytes = await response.read()

            # aiohttp handles gzip decompression

            # Detect encoding
            encoding = _detect_encoding(raw_bytes, content_type)
            try:
                # Decode to HTML
                html = raw_bytes.decode(encoding, errors='replace')
            except Exception as e:
                if DEBUG: print(f'[ERROR] Failed to decode {url} using {encoding}: {e}')
                return final_url, None, meta
            
            meta['content_length'] = len(raw_bytes)
            return final_url, html, meta

    # Handle HTTP response errors (e.g. 404, 403, 500)
    except ClientResponseError as e:
        meta['status_code'] = e.status
        meta['timestamp'] = datetime.now(timezone.utc).isoformat()
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta

    # Handle network-level errors (e.g. DNS failure, connection timeout)
    except ClientConnectorError as e:
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta
    
    # Handle unexpected errors
    except Exception as e:
        if DEBUG: print(f'[ERROR] Failed to fetch {url}: {e}')
        return url, None, meta