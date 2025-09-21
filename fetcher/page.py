from urllib.request import urlopen

def fetch_page(url):
    return urlopen(url).read().decode('utf-8')