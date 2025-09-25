def log_url(log_file, url, meta, depth):
    line = f'{url}\t{meta["timestamp"]}\t{meta["content_length"]}\t{depth}\t{meta["status_code"]}\n'
    log_file.write(line)
    log_file.flush() # immediately write to disk

def log_summary(log_file, num_pages, total_size, total_time, status_counts):
    log_file.write("\nCrawl Summary:\n")
    log_file.write(f'Total pages crawled: {num_pages}\n')
    log_file.write(f'Total size: {total_size} bytes\n')
    log_file.write(f'Total time: {total_time:.2f} seconds\n')

    for status_code, count in status_counts.items():
        log_file.write(f'{status_code} responses: {count}\n')
    log_file.flush()