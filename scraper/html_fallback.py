from __future__ import annotations

from bs4 import BeautifulSoup
import asyncio
import hashlib
import os
import random
import time
from urllib.parse import urljoin, urlencode

import httpx
from utils.http import DEFAULT_HEADERS, get

CACHE_DIR = ".cache/jobdetail"
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class HostThrottle:
    def __init__(self, per_host: int) -> None:
        self.semaphore = asyncio.Semaphore(per_host)
        self.lock = asyncio.Lock()
        self.last_request_ts: float | None = None


async def _read_text(path: str) -> str:
    return await asyncio.to_thread(_read_text_sync, path)


def _read_text_sync(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


async def _write_text(path: str, text: str) -> None:
    await asyncio.to_thread(_write_text_sync, path, text)


def _write_text_sync(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


async def _polite_wait(state: HostThrottle, min_delay: float) -> None:
    if min_delay <= 0:
        return
    async with state.lock:
        now = time.monotonic()
        if state.last_request_ts is not None:
            gap = now - state.last_request_ts
            if gap < min_delay:
                await asyncio.sleep(min_delay - gap)


async def _record_request(state: HostThrottle) -> None:
    async with state.lock:
        state.last_request_ts = time.monotonic()


async def _get_with_retries(
    client: httpx.AsyncClient,
    url: str,
    state: HostThrottle,
    min_delay: float,
    retries: int,
    timeout: httpx.Timeout,
) -> httpx.Response:
    for attempt in range(retries + 1):
        await state.semaphore.acquire()
        try:
            await _polite_wait(state, min_delay)
            try:
                resp = await client.get(url, timeout=timeout)
            except httpx.RequestError:
                await _record_request(state)
                if attempt == retries:
                    raise
                backoff = 0.6 * (2**attempt)
                jitter = random.uniform(0.0, 0.4)
                await asyncio.sleep(backoff + jitter)
                continue
            await _record_request(state)
        finally:
            state.semaphore.release()

        if resp.status_code < 400:
            return resp

        if resp.status_code not in RETRYABLE_STATUS or attempt == retries:
            resp.raise_for_status()

        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = None
        else:
            delay = None

        if delay is None:
            backoff = 0.6 * (2**attempt)
            jitter = random.uniform(0.0, 0.4)
            delay = backoff + jitter

        await asyncio.sleep(delay)

    return resp


def iter_searchjobs_pages(base_careers_url: str, page_size: int = 25, max_pages: int = 200):
    """
    base_careers_url: e.g. https://bloomberg.avature.net/careers
    yields HTML for SearchJobs pages with increasing offsets
    """
    search_url = base_careers_url.rstrip("/") + "/SearchJobs"
    offset = 0

    for _ in range(max_pages):
        qs = urlencode({"jobOffset": offset, "jobRecordsPerPage": page_size})
        url = f"{search_url}/?{qs}"
        resp = get(url)
        if resp.status_code != 200:
            return

        html = resp.text
        if "JobDetail" not in html:
            return

        yield url, html
        offset += page_size


def extract_jobdetail_urls(page_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: set[str] = set()

    for a in soup.select("a[href*='JobDetail']"):
        href = a.get("href")
        if not href:
            continue
        full = urljoin(page_url, href)
        # Normalize by stripping fragments
        full = full.split("#", 1)[0]
        urls.add(full)

    return sorted(urls)


def fetch_jobdetail(job_url: str) -> tuple[str, str]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    key = hashlib.sha256(job_url.encode("utf-8")).hexdigest()
    path = os.path.join(CACHE_DIR, f"{key}.html")

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return job_url, f.read()

    resp = get(job_url)
    resp.raise_for_status()
    html = resp.text

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    return job_url, html


async def iter_searchjobs_pages_async(
    base_careers_url: str,
    page_size: int = 25,
    max_pages: int = 200,
    *,
    per_host: int = 4,
    min_delay: float = 0.3,
    retries: int = 3,
) -> list[tuple[str, str]]:
    search_url = base_careers_url.rstrip("/") + "/SearchJobs"
    offset = 0
    state = HostThrottle(per_host)
    timeout = httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=10.0)

    pages: list[tuple[str, str]] = []
    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
        for _ in range(max_pages):
            qs = urlencode({"jobOffset": offset, "jobRecordsPerPage": page_size})
            url = f"{search_url}/?{qs}"
            resp = await _get_with_retries(
                client,
                url,
                state,
                min_delay=min_delay,
                retries=retries,
                timeout=timeout,
            )
            if resp.status_code != 200:
                break

            html = resp.text
            if "JobDetail" not in html:
                break

            pages.append((url, html))
            offset += page_size

    return pages


async def fetch_jobdetail_async(
    job_url: str,
    *,
    client: httpx.AsyncClient,
    state: HostThrottle,
    min_delay: float,
    retries: int,
    timeout: httpx.Timeout,
) -> tuple[str, str]:
    os.makedirs(CACHE_DIR, exist_ok=True)
    key = hashlib.sha256(job_url.encode("utf-8")).hexdigest()
    path = os.path.join(CACHE_DIR, f"{key}.html")

    if os.path.exists(path):
        return job_url, await _read_text(path)

    resp = await _get_with_retries(
        client,
        job_url,
        state,
        min_delay=min_delay,
        retries=retries,
        timeout=timeout,
    )
    resp.raise_for_status()
    html = resp.text

    await _write_text(path, html)
    return job_url, html
