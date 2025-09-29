from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin

def is_allowed(url, cache, user_agent='*'):
    parse_result = urlparse(url)
    base_url = f'{parse_result.scheme}://{parse_result.netloc}'
    robots_url = urljoin(base_url, '/robots.txt')

    # Use cached parser if available
    if base_url in cache:
        rp = cache[base_url]
        return rp is None or rp.can_fetch(user_agent, url)  # If None, allow everything
    
    rp = RobotFileParser()
    rp.set_url(robots_url)
    
    try:
        rp.read()
        cache[base_url] = rp
        return rp.can_fetch(user_agent, url)
    
    except Exception as e:
        # Print warning, but still allow crawl
        print(f'[WARNING] Failed to fetch {robots_url}: {e}')
        cache[base_url] = None
        return True

async def is_allowed_async(url, cache, session, user_agent='*'):
    parse_result = urlparse(url)
    base_url = f'{parse_result.scheme}://{parse_result.netloc}'
    robots_url = urljoin(base_url, '/robots.txt')

    # Use cached parser if available
    if base_url in cache:
        rp = cache[base_url]
        return rp is None or rp.can_fetch(user_agent, url)  # If None, allow everything

    rp = RobotFileParser()

    try:
        async with session.get(robots_url, timeout=5) as response:
            # Read and parse lines
            content = await response.text()
            rp.parse(content.splitlines())
            cache[base_url] = rp
            return rp.can_fetch(user_agent, url)

    except Exception as e:
        # Print warning, but still allow crawl
        print(f'[WARNING] Failed to fetch {robots_url}: {e}')
        cache[base_url] = None
        return True