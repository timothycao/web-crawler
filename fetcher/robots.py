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
    rp.read()
    cache[base_url] = rp

    return rp.can_fetch(user_agent, url)