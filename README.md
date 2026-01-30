Avature ATS Scraper (API-first, no browser automation)

Goal
Collect as many valid Avature-hosted job postings as possible across as many Avature tenants as possible.

Key Ideas
- API-first scraping with HTML fallback for completeness.
- Discovery at scale using seed lists + Common Crawl CC index scanning.
- Polite, resilient fetching (timeouts, retries, backoff, per-host throttling).
- Structured JSON output + aggressive deduplication.

What This Repo Contains
- Tenant discovery and URL expansion: `scraper/discover.py`
- Common Crawl CC index scanner (local streaming): `scraper/cc_index_scan.py`
- API endpoint discovery: `scraper/endpoints.py`
- HTML fallback for SearchJobs / JobDetail: `scraper/html_fallback.py`
- Job normalization + parsing: `scraper/normalize.py`, `scraper/parse_jobdetail.py`

Inputs / Outputs
- Input seed list: `urls.txt` (starter pack + extensions you discover)
- Tenant list used by scraper: `input/avature_sites.json`
- Discovered Avature URLs from CC scan: `output/avature_discovered.txt`
- Scraped jobs (NDJSON): `output/jobs.ndjson` (streaming-friendly and faster than pretty JSON)

How Discovery Works
1) Seed ingestion
   - `scraper/discover.py` reads `urls.txt` and groups Avature URLs by tenant.
   - For each tenant, it stores a normalized `careers_url` and keeps all raw `seed_urls` to avoid missing jobs.
2) CC index scan (go beyond the seed list)
   - `scraper/cc_index_scan.py` streams Common Crawl CC index shards locally.
   - It extracts any URL containing `avature.net` and writes them to `output/avature_discovered.txt`.
   - Use this file to expand `urls.txt` and re-run discovery.

How Scraping Works
1) Endpoint discovery (API-first)
   - `scraper/endpoints.py` attempts to locate Avature API endpoints.
2) HTML fallback
   - If API discovery yields no jobs, it falls back to `SearchJobs` + `JobDetail` HTML parsing.
   - Async fetch with per-host throttling, backoff, and retries to be polite.
3) Normalize + dedupe
   - Jobs are normalized and deduplicated before writing output.

Instructions (Recommended Order)
1) CC index scan (expand beyond seed list)
   Prereqs:
   - Download the CC index paths file:
     - `curl -L -o /tmp/cc-index.paths.gz https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2026-04/indexes/cc-index.paths.gz`
     - `gunzip -c /tmp/cc-index.paths.gz > /tmp/cc-index.paths`
   Run a small test first:
   - `poetry run python scraper/cc_index_scan.py --paths-file /tmp/cc-index.paths --max-files 5`
   Output:
   - `output/avature_discovered.txt` (raw Avature URLs)
   - `output/cc_index_state.txt` (checkpoint)
2) Update tenant list from seed URLs
   - Add newly discovered URLs to `urls.txt`
   - Run:
     - `poetry run python scraper/discover.py --seeds urls.txt --output input/avature_sites.json --discovered-out output/avature_discovered.txt`
   Output:
   - `input/avature_sites.json` (tenants + seed URLs)
3) Scrape jobs (NDJSON output)
   - `poetry run python main.py`

Notes
- Runtime code does not depend on LLMs or browser automation frameworks.
- The CC scan is streaming and resumable; see `output/cc_index_state.txt`.

Time Spent:
- TODO: fill in

Next Improvements
- Add WAT outlink extraction for deeper tenant discovery (likely on a stronger machine).
- Promote into a proper Python package with service classes (SiteDiscovery, JobScraper, etc.).
- Locale-aware endpoint handling.
- Job detail page enrichment and schema cleanup.
