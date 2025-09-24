from utils.url import clean_url

def get_seeds():
    seeds = ['http://example.com']
    return [clean_url(seed) for seed in seeds]