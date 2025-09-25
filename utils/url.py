from urllib.parse import urldefrag

def clean_url(url):
    # Remove anchor fragments like #section
    url, fragment = urldefrag(url)
    return url

def log_url(log_file, url, meta, depth):
    line = f'{url}\t{meta["timestamp"]}\t{meta["content_length"]}\t{depth}\t{meta["status_code"]}\n'
    log_file.write(line)
    log_file.flush() # immediately write to disk