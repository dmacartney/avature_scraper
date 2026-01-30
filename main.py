from __future__ import annotations

import asyncio
import json
import os

from scraper.dedupe import dedupe
from scraper.endpoints import discover_job_endpoint
from scraper.fetch import fetch_jobs
from scraper.html_fallback import (
    HostThrottle,
    extract_jobdetail_urls,
    fetch_jobdetail_async,
    iter_searchjobs_pages_async,
)
from scraper.normalize import normalize
from scraper.parse_jobdetail import parse_jobdetail

INPUT_FILE = "input/avature_sites.json"
OUTPUT_FILE = "output/jobs.json"


def ensure_output_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def scrape_via_api(careers_url: str, tenant: str) -> list[dict]:
    endpoint = discover_job_endpoint(careers_url)
    print(f"[endpoint] {endpoint}")
    if not endpoint:
        return []

    raw_jobs = fetch_jobs(endpoint)
    print(f"[api] raw_jobs={len(raw_jobs)}")
    return [normalize(job, tenant) for job in raw_jobs]


async def scrape_via_html_async(
    careers_url: str,
    tenant: str,
    *,
    page_size: int = 25,
    max_pages: int = 200,
    per_host: int = 4,
    min_delay: float = 0.3,
    retries: int = 3,
) -> list[dict]:
    job_urls: set[str] = set()

    pages = await iter_searchjobs_pages_async(
        careers_url,
        page_size=page_size,
        max_pages=max_pages,
        per_host=per_host,
        min_delay=min_delay,
        retries=retries,
    )
    for page_url, html in pages:
        job_urls.update(extract_jobdetail_urls(page_url, html))

    job_urls = sorted(job_urls)
    print(f"[html] jobdetail_urls={len(job_urls)}")

    jobs: list[dict] = []
    if not job_urls:
        return jobs

    import httpx
    from utils.http import DEFAULT_HEADERS

    state = HostThrottle(per_host)
    timeout = httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=10.0)

    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
        task_map: dict[asyncio.Task, str] = {}
        for url in job_urls:
            task = asyncio.create_task(
                fetch_jobdetail_async(
                    url,
                    client=client,
                    state=state,
                    min_delay=min_delay,
                    retries=retries,
                    timeout=timeout,
                )
            )
            task_map[task] = url
        done = 0
        for task in asyncio.as_completed(task_map):
            done += 1
            if done == 1 or done % 50 == 0 or done == len(job_urls):
                print(f"[html] done {done}/{len(job_urls)}")
            try:
                url, job_html = await task
                jobs.append(parse_jobdetail(job_html, url, tenant))
            except Exception as e:
                print(f"[html] error processing job {task_map[task]}: {e}")

    return jobs


def main() -> None:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        sites = json.load(f)

    all_jobs: list[dict] = []

    for site in sites:
        tenant = site["tenant"]
        careers_url = site["careers_url"]
        print(f"\n[site] tenant={tenant} url={careers_url}")

        # 1) Try API-first
        jobs = scrape_via_api(careers_url, tenant)

        # 2) If API yielded nothing, fallback to HTML
        if not jobs:
            print("[fallback] using html SearchJobs/JobDetail")
            jobs = asyncio.run(scrape_via_html_async(careers_url, tenant))

        print(f"[site] collected={len(jobs)}")
        all_jobs.extend(jobs)

    clean_jobs = dedupe(all_jobs)
    print(f"\n[total] before_dedupe={len(all_jobs)} after_dedupe={len(clean_jobs)}")

    ensure_output_dir(OUTPUT_FILE)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(clean_jobs, f, indent=2, ensure_ascii=False)

    print(f"[write] {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
