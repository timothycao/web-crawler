# CS 6913 – Assignment 1: Web Crawler

This project implements a multithreaded web crawler that:

- Starts from 10+ seed URLs (via DuckDuckGo)
- Follows a priority-based BFS strategy to maximize domain diversity
- Honors `robots.txt`, avoids duplicate/invalid/CGI/script URLs
- Logs crawled pages and summarizes fetch statistics

The final implementation uses Python's `ThreadPoolExecutor` for concurrency, with HTML filtering and timeout/error handling to ensure crawl stability.

## Setup Instructions

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

## Run the Crawler

Tune crawler configurations (including query) in `config.py`.

### Run the multithreaded version

```bash
python -m multithread.main
```

Output will be written to `log.txt` during execution.

### Run the asynchronous version

```bash
python -m asynchronous.main
```

Output will be written to `log_async.txt` during execution.

## File Descriptions

### Root

- `config.py`: Global constants (query, crawl limits, number of workers, debug mode)
- `requirements.txt`: Python package dependencies
- `crawl_log1.txt`: Sample crawl log using query "dogs and cats"
- `crawl_log2.txt`: Sample crawl log using query "brooklyn pizza"

### multithread/

- `main.py`: Initializes crawler state, fetches seed URLs, and starts the thread pool
- `worker.py`: Handles page fetching, link extraction, and task scheduling across threads

### asynchronous/ (exploratory version)

- `main.py`: Initializes crawler state, fetches seed URLs, and manages the async crawl loop
- `worker.py`: Handles page fetching, link extraction, and task scheduling using async coroutines

### fetcher/

- `page.py`: Handles page fetching and metadata (sync and async versions)
- `robots.py`: Fetches and parses `robots.txt` rules via `RobotFileParser` (sync and async versions)

### query/

- `ddg.py`: Scrapes DuckDuckGo using POST request to get seed result URLs

### utils/

- `url.py`: URL validation, normalization, extension filtering, and superdomain extraction
- `priority.py`: Computes crawl priority based on domain and superdomain diversity

### logger/

- `log.py`: Logs per-page crawl results and final crawl summary stats
