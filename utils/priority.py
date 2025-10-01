from math import log

def compute_priority(domain_crawl_count, superdomain_domain_count):
    # return 1 / log(1 + domain_crawl_count + 1e-6)
    return 1 / log(2 + domain_crawl_count) + 1 / (1 + superdomain_domain_count)