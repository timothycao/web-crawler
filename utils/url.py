from urllib.parse import urldefrag

def clean_url(url):
    # Remove anchor fragments like #section
    url, fragment = urldefrag(url)
    return url