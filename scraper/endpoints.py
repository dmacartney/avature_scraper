import os
import re
from urllib.parse import urlparse, urljoin

from utils.http import get, post


COMMON_ENDPOINTS = [
    "/services/avature/search",
    "/services/avature/jobs",
    "/api/jobs",
]


def _origin(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _candidate_pages(base_url: str) -> list[str]:
    base = base_url.rstrip("/")
    origin = _origin(base_url)
    pages = [
        base,
        base + "/SearchJobs",
        origin + "/careers",
        origin + "/careers/SearchJobs",
    ]
    return list(dict.fromkeys(pages))


def _extract_endpoint_from_html(html: str, base_url: str) -> str | None:
    for ep in COMMON_ENDPOINTS:
        if ep in html:
            return urljoin(base_url.rstrip("/") + "/", ep.lstrip("/"))

    matches = re.findall(r"(\/services\/[^\"']+)", html)
    if matches:
        return urljoin(base_url.rstrip("/") + "/", matches[0].lstrip("/"))

    return None


def _probe_endpoint(endpoint: str) -> bool:
    payload = {"page": 1, "pageSize": 1}
    try:
        resp = post(endpoint, json=payload, timeout=10)
    except Exception:
        return False
    if resp.status_code >= 400:
        return False
    try:
        data = resp.json()
    except Exception:
        return False
    return bool(data.get("jobs") or data.get("results"))


def discover_job_endpoint(base_url: str) -> str | None:
    origin = _origin(base_url)
    debug = os.getenv("DEBUG_ENDPOINTS") == "1"

    # 1) Try to discover from HTML pages.
    for page in _candidate_pages(base_url):
        try:
            html = get(page, timeout=15).text
        except Exception:
            if debug:
                print(f"[endpoint] fetch failed page={page}")
            continue
        endpoint = _extract_endpoint_from_html(html, page)
        if endpoint:
            if debug:
                print(f"[endpoint] found_in_html page={page} endpoint={endpoint}")
            return endpoint
        if debug:
            print(f"[endpoint] no_match page={page}")

    # 2) Probe common endpoints (origin and base-url mounted).
    candidates = []
    for ep in COMMON_ENDPOINTS:
        candidates.append(urljoin(origin.rstrip("/") + "/", ep.lstrip("/")))
        candidates.append(urljoin(base_url.rstrip("/") + "/", ep.lstrip("/")))

    for endpoint in list(dict.fromkeys(candidates)):
        if debug:
            print(f"[endpoint] probe {endpoint}")
        if _probe_endpoint(endpoint):
            if debug:
                print(f"[endpoint] probe_ok {endpoint}")
            return endpoint

    return None
