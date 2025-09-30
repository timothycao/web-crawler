from os.path import splitext
from urllib.parse import urlsplit, urlunsplit

import validators

BLACKLIST = {
    '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.exe',
    '.js', '.css', '.mp4', '.mp3', '.avi', '.mov', '.svg',
    '.doc', '.ppt', '.xls', '.rar', '.tar', '.dmg',
    '.php', '.jsp', '.cgi', '.aspx' # script-based extensions
}

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

def is_valid_url(url):
    # return url.startswith('http://') or url.startswith('https://')
    return validators.url(url)

def is_cgi_url(url):
    path = urlsplit(url).path.lower()
    return 'cgi' in path

def is_blocked_extension(url):
    path = urlsplit(url).path
    _, ext = splitext(path)
    return ext.lower() in BLACKLIST