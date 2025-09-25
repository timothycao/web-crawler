from math import log

def compute_priority(domain_count):
    return 1 / log(1 + domain_count + 1e-6)