from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

cache = {}  # domain: robot parser

def is_allowed(url, user_agent='*'):
    parse_result = urlparse(url)
    base_url = f'{parse_result.scheme}://{parse_result.netloc}'

    if base_url in cache:
        return cache[base_url].can_fetch(user_agent, url)
    
    rp = RobotFileParser()
    rp.set_url(base_url + '/robots.txt')
    
    try:
        rp.read()
        cache[base_url] = rp
        return rp.can_fetch(user_agent, url)
    
    except Exception as e:
        # Print warning, but still allow crawl
        print(f'[WARNING] Failed to fetch {base_url}/robots.txt: {e}')
        return True