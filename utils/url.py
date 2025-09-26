from urllib.parse import urlsplit, urlunsplit

def clean_url(url):
    # Parse URL into components
    scheme, netloc, path, query, fragment = urlsplit(url)

    # Remove query and fragment
    cleaned = urlunsplit((
        scheme, # e.g. https
        netloc, # e.g. example.com
        path,   # e.g. /docs/page
        '',     # strip query string (e.g. ?ref=abc)
        ''      # strip fragment (e.g. #section)
    ))

    # Strip trailing slash unless part of root path
    if cleaned.endswith('/') and cleaned.count('/') > 2:
        cleaned = cleaned.rstrip('/')
    
    return cleaned