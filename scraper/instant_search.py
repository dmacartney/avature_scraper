from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from utils.http import get, post


TOKEN_KEYS = ("listSpecId", "searchIndexId", "contextValues", "richFieldId")
DEFAULT_SEARCH_TERMS = (
    [""]
    + [chr(c) for c in range(ord("a"), ord("z") + 1)]
    + [str(d) for d in range(0, 10)]
    + [
        "engineer",
        "manager",
        "analyst",
        "intern",
        "senior",
        "assistant",
        "director",
        "associate",
        "lead",
        "developer",
        "software",
        "data",
        "sales",
        "marketing",
        "product",
        "finance",
        "security",
    ]
)


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
        origin + "/en_US/careers",
        origin + "/en_US/careers/SearchJobs",
        origin + "/en-GB/careers",
        origin + "/en-GB/careers/SearchJobs",
        origin + "/careers?locale=en_US",
    ]
    return list(dict.fromkeys(pages))


def _extract_tokens_from_text(text: str) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for key in TOKEN_KEYS:
        # Match JSON-like "key":"value"
        m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', text)
        if not m:
            m = re.search(rf"{re.escape(key)}\s*[:=]\s*\"([^\"]+)\"", text)
        if m:
            tokens[key] = m.group(1)
    return tokens


def _extract_script_urls(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    base_host = urlparse(base_url).netloc.lower()
    for tag in soup.select("script[src]"):
        src = tag.get("src")
        if not src:
            continue
        full = urljoin(base_url, src)
        host = urlparse(full).netloc.lower()
        if host and host != base_host:
            # Skip third-party scripts.
            continue
        urls.append(full)
    return list(dict.fromkeys(urls))


def discover_instant_search_tokens(base_url: str, max_scripts: int = 5) -> dict[str, str] | None:
    for page in _candidate_pages(base_url):
        try:
            html = get(page, timeout=15).text
        except Exception:
            continue

        tokens = _extract_tokens_from_text(html)
        if all(k in tokens for k in TOKEN_KEYS):
            return tokens

        for script_url in _extract_script_urls(html, page)[:max_scripts]:
            try:
                script_text = get(script_url, timeout=15).text
            except Exception:
                continue
            tokens.update(_extract_tokens_from_text(script_text))
            if all(k in tokens for k in TOKEN_KEYS):
                return tokens

    return None


def instant_search_detail_urls(
    base_url: str,
    tokens: dict[str, str],
    search_terms: list[str] | None = None,
) -> set[str]:
    terms = search_terms or DEFAULT_SEARCH_TERMS
    endpoint = base_url.rstrip("/") + "/_instantSearch"
    detail_urls: set[str] = set()

    for term in terms:
        payload = {
            "requestType": "job",
            "listSpecId": tokens.get("listSpecId"),
            "richFieldId": tokens.get("richFieldId"),
            "searchTerm": term,
            "searchIndexId": tokens.get("searchIndexId"),
            "contextValues": tokens.get("contextValues"),
            "isSemanticSearch": False,
        }
        try:
            resp = post(endpoint, json=payload, timeout=20)
            if resp.status_code >= 400:
                continue
            data = resp.json()
        except Exception:
            continue

        for item in data.get("results") or []:
            url = item.get("detailUrl")
            if url:
                detail_urls.add(url)

    return detail_urls
