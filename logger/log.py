def log_url(log_file, url, meta, depth, priority):
    line = f'{url}\t{meta["timestamp"]}\t{meta["content_length"]}\t{depth}\t{meta["status_code"]}\t{priority:.6f}\n'
    log_file.write(line)
    log_file.flush() # immediately write to disk

def log_summary(log_file, num_pages, total_size, total_time, status_counts, crawl_counts):
    log_file.write("\nFetch Summary:\n")
    log_file.write(f'Total pages: {num_pages}\n')
    log_file.write(f'Total size: {total_size} bytes\n')
    log_file.write(f'Total time: {total_time:.2f} seconds\n')
    for status_code, count in status_counts.items():
        log_file.write(f'{status_code} responses: {count}\n')
    
    log_file.write(f'\nTotal pages crawled (status 200 and html): {sum(crawl_counts.values())}\n')
    for domain, count in sorted(crawl_counts.items(), key=lambda x: -x[1]):
        log_file.write(f"{domain}: {count}\n")   
    
    log_file.flush()