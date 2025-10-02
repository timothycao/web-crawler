# Seed query
QUERY = 'brooklyn pizza'

# Max number of pages to visit (fetch)
MAX_PAGES = 10000

# Max crawl time in seconds
MAX_TIME = 1800

# Max allowed fetch failures per domain (status 0, no content)
MAX_TIMEOUTS = 2

# Thread/concurrent limit (used in both multithread and async)
NUM_THREADS = MAX_CONCURRENT_REQUESTS = 50

# Toggle verbose logging (skipped counts, skips, warnings, errors)
DEBUG = False