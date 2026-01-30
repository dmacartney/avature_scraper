from __future__ import annotations

import argparse
import asyncio
import json
import os

from scraper.endpoints import discover_job_endpoint
from scraper.fetch import fetch_jobs
from scraper.html_fallback import (
    HostThrottle,
    extract_jobdetail_urls,
    fetch_jobdetail_async,
    iter_searchjobs_pages_async,
)
from scraper.instant_search import discover_instant_search_tokens, instant_search_detail_urls
from scraper.normalize import normalize
from scraper.parse_jobdetail import parse_jobdetail

INPUT_FILE = "input/avature_sites.json"
OUTPUT_FILE = "output/jobs.ndjson"
FAILED_URLS_FILE = "output/failed_jobdetail_urls.txt"


def ensure_output_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def scrape_via_api(careers_url: str, tenant: str) -> list[dict]:
    try:
        endpoint = discover_job_endpoint(careers_url)
    except Exception as e:
        print(f"[api] endpoint discovery failed tenant={tenant} url={careers_url}: {e}")
        return []

    print(f"[endpoint] {endpoint}")
    if not endpoint:
        return []

    try:
        raw_jobs = fetch_jobs(endpoint)
    except Exception as e:
        print(f"[api] fetch failed tenant={tenant} url={endpoint}: {e}")
        return []
    print(f"[api] raw_jobs={len(raw_jobs)}")
    return [normalize(job, tenant) for job in raw_jobs]


async def scrape_via_html_async(
    careers_url: str,
    tenant: str,
    *,
    page_size: int = 25,
    max_pages: int = 1000,
    per_host: int = 4,
    min_delay: float = 0.3,
    retries: int = 3,
    min_new_per_page: int = 0,
    max_low_yield_pages: int = 3,
) -> list[dict]:
    job_urls: set[str] = set()

    low_yield_streak = 0
    async for page_url, html in iter_searchjobs_pages_async(
        careers_url,
        page_size=page_size,
        max_pages=max_pages,
        per_host=per_host,
        min_delay=min_delay,
        retries=retries,
    ):
        urls = set(extract_jobdetail_urls(page_url, html))
        new_urls = urls - job_urls
        job_urls.update(urls)
        new_count = len(new_urls)
        if new_count < min_new_per_page:
            low_yield_streak += 1
        else:
            low_yield_streak = 0
        if low_yield_streak >= max_low_yield_pages:
            print(
                f"[html] low yield for {low_yield_streak} pages, stopping early "
                f"(min_new_per_page={min_new_per_page})"
            )
            break

    job_urls = sorted(job_urls)
    print(f"[html] jobdetail_urls={len(job_urls)}")

    return await fetch_jobdetails_from_urls_async(
        job_urls,
        tenant,
        per_host=per_host,
        min_delay=min_delay,
        retries=retries,
    )


async def fetch_jobdetails_from_urls_async(
    job_urls: list[str],
    tenant: str,
    *,
    per_host: int = 4,
    min_delay: float = 0.3,
    retries: int = 3,
) -> list[dict]:
    jobs: list[dict] = []
    if not job_urls:
        return jobs

    import httpx
    from utils.http import DEFAULT_HEADERS

    state = HostThrottle(per_host)
    timeout = httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=10.0)

    ensure_output_dir(FAILED_URLS_FILE)
    with open(FAILED_URLS_FILE, "a", encoding="utf-8") as failed_log:
        async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
            async def _fetch_one(url: str):
                try:
                    url_out, job_html = await fetch_jobdetail_async(
                        url,
                        client=client,
                        state=state,
                        min_delay=min_delay,
                        retries=retries,
                        timeout=timeout,
                    )
                    return url_out, job_html, None
                except Exception as e:
                    return url, None, e

            tasks = [asyncio.create_task(_fetch_one(url)) for url in job_urls]
            done = 0
            for fut in asyncio.as_completed(tasks):
                done += 1
                if done == 1 or done % 50 == 0 or done == len(job_urls):
                    print(f"[html] done {done}/{len(job_urls)}")
                url, job_html, err = await fut
                if err is not None:
                    print(f"[html] error processing job {url}: {err}")
                    failed_log.write(url)
                    failed_log.write("\n")
                    continue
                jobs.append(parse_jobdetail(job_html, url, tenant))

    return jobs


def _job_key(job: dict) -> tuple[str | None, str | None]:
    if job.get("id") or job.get("fingerprint"):
        return (job.get("tenant"), job.get("id") or job.get("fingerprint"))
    source = job.get("source") or {}
    return (job.get("tenant"), source.get("url"))


def _load_existing_ndjson(path: str) -> set[tuple[str | None, str | None]]:
    if not os.path.exists(path):
        return set()
    seen: set[tuple[str | None, str | None]] = set()
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                job = json.loads(line)
            except Exception:
                continue
            seen.add(_job_key(job))
    return seen


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Avature jobs")
    parser.add_argument("--fresh", action="store_true", help="Ignore existing NDJSON output and start fresh")
    args = parser.parse_args()

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        sites = json.load(f)

    ensure_output_dir(OUTPUT_FILE)
    if args.fresh:
        seen: set[tuple[str | None, str | None]] = set()
        mode = "w"
        print("[resume] fresh run, ignoring existing output")
    else:
        seen = _load_existing_ndjson(OUTPUT_FILE)
        if seen:
            print(f"[resume] loaded_existing={len(seen)}")
        mode = "a"

    with open(OUTPUT_FILE, mode, encoding="utf-8") as ndjson:
        total = 0
        kept = 0

        for site in sites:
            tenant = site["tenant"]
            careers_url = site["careers_url"]
            print(f"\n[site] tenant={tenant} url={careers_url}")

            try:
                seed_urls = site.get("seed_urls") or []
                if seed_urls:
                    seed_urls = [u for u in seed_urls if "JobDetail" in u]
                    if seed_urls:
                        print(f"[seed] jobdetail_urls={len(seed_urls)}")
                        jobs = asyncio.run(fetch_jobdetails_from_urls_async(seed_urls, tenant))
                    else:
                        jobs = []
                else:
                    jobs = []

                # 1) Try API-first
                if not jobs:
                    jobs = scrape_via_api(careers_url, tenant)

                # 2) If API yielded nothing, fallback to HTML
                if not jobs:
                    print("[fallback] using instantSearch")
                    tokens = discover_instant_search_tokens(careers_url)
                    if tokens:
                        urls = sorted(instant_search_detail_urls(careers_url, tokens))
                        if urls:
                            print(f"[instant] jobdetail_urls={len(urls)}")
                            jobs = asyncio.run(fetch_jobdetails_from_urls_async(urls, tenant))
                    if not jobs:
                        print("[fallback] using html SearchJobs/JobDetail")
                        jobs = asyncio.run(scrape_via_html_async(careers_url, tenant))
            except Exception as e:
                print(f"[site] error tenant={tenant} url={careers_url}: {e}")
                jobs = []

            print(f"[site] collected={len(jobs)}")
            total += len(jobs)

            for job in jobs:
                key = _job_key(job)
                if key in seen:
                    continue
                seen.add(key)
                ndjson.write(json.dumps(job, ensure_ascii=False))
                ndjson.write("\n")
                kept += 1

    print(f"\n[total] before_dedupe={total} after_dedupe={kept}")

    print(f"[write] {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
