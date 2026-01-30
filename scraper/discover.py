from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import deque
from typing import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from utils.http import get

DEFAULT_SEEDS_FILE = "urls.txt"
DEFAULT_OUTPUT_FILE = "input/avature_sites.json"
DEFAULT_DISCOVERED_FILE = "output/avature_discovered.txt"
DEFAULT_MAX_FOLLOW = 60
DEFAULT_DELAY = 0.5

_URL_RE = re.compile(r"https?://[^\s)>\"]+")
_CAREERS_HINTS = ("careers", "jobs", "employment", "work-with-us", "join-our-team", "join-us")


def _extract_urls_from_line(line: str) -> list[str]:
    return _URL_RE.findall(line)


def load_seeds(path: str, *, max_seeds: int | None = None, sample_every: int = 1) -> list[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"seed file not found: {path}")

    seeds: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for idx, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if sample_every > 1 and idx % sample_every != 0:
                continue
            seeds.extend(_extract_urls_from_line(line))
            if max_seeds is not None and len(seeds) >= max_seeds:
                break

    return list(dict.fromkeys(seeds))


def normalize_avature_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None

    host = parsed.netloc.lower()
    if not host.endswith("avature.net"):
        return None

    tenant = host.split(".")[0]
    if not tenant:
        return None

    return f"https://{tenant}.avature.net/careers"


def tenant_from_careers_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    host = parsed.netloc.lower()
    if not host.endswith("avature.net"):
        return None
    return host.split(".")[0] or None


def company_from_tenant(tenant: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", " ", tenant).strip()
    return cleaned.title() if cleaned else tenant


def _extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    for tag in soup.select("a[href], iframe[src], script[src], link[href]"):
        href = tag.get("href") or tag.get("src")
        if not href:
            continue
        full = urljoin(base_url, href)
        full = full.split("#", 1)[0]
        links.append(full)

    return list(dict.fromkeys(links))


def _is_careersish(url: str) -> bool:
    lowered = url.lower()
    return any(hint in lowered for hint in _CAREERS_HINTS)


def _fetch_html(url: str, delay: float) -> str | None:
    try:
        resp = get(url, timeout=20)
    except Exception:
        return None
    if resp.status_code >= 400:
        return None
    if delay > 0:
        time.sleep(delay)
    return resp.text


def discover_avature_urls(
    seeds: Iterable[str],
    *,
    max_follow: int = DEFAULT_MAX_FOLLOW,
    delay: float = DEFAULT_DELAY,
) -> set[str]:
    found: set[str] = set()
    visited: set[str] = set()

    seeds_list = list(seeds)
    non_avature: list[str] = []
    for url in seeds_list:
        avature = normalize_avature_url(url)
        if avature:
            found.add(avature)
        else:
            non_avature.append(url)

    queue: deque[str] = deque(non_avature)

    while queue and len(visited) < max_follow:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        html = _fetch_html(url, delay=delay)
        if not html:
            continue

        for link in _extract_links(html, url):
            av = normalize_avature_url(link)
            if av:
                found.add(av)
                continue

            if _is_careersish(link) and link not in visited and len(visited) + len(queue) < max_follow:
                queue.append(link)

    return found


def collect_raw_avature_urls(urls: Iterable[str]) -> set[str]:
    raw: set[str] = set()
    for url in urls:
        try:
            host = urlparse(url).netloc.lower()
        except ValueError:
            continue
        if host.endswith("avature.net"):
            raw.add(url.split("#", 1)[0])
    return raw


def load_existing_sites(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_sites(existing: list[dict], discovered: set[str], raw_urls: set[str]) -> list[dict]:
    existing_by_tenant: dict[str, dict] = {}
    for site in existing:
        tenant = site.get("tenant") or ""
        if tenant:
            existing_by_tenant[tenant] = site

    tenant_to_company = {
        site.get("tenant", ""): site.get("company", "")
        for site in existing
        if site.get("tenant") and site.get("company")
    }

    raw_by_tenant: dict[str, set[str]] = {}
    for url in raw_urls:
        tenant = tenant_from_careers_url(url)
        if not tenant:
            continue
        raw_by_tenant.setdefault(tenant, set()).add(url)

    for careers_url in sorted(discovered):
        tenant = tenant_from_careers_url(careers_url) or ""
        if not tenant:
            continue

        site = existing_by_tenant.get(tenant)
        if site is None:
            company = tenant_to_company.get(tenant) or company_from_tenant(tenant)
            site = {
                "company": company,
                "careers_url": careers_url,
                "tenant": tenant,
                "seed_urls": [],
            }
            existing.append(site)
            existing_by_tenant[tenant] = site
        else:
            site.setdefault("seed_urls", [])
            if site.get("careers_url") is None:
                site["careers_url"] = careers_url

        if tenant in raw_by_tenant:
            merged = set(site.get("seed_urls") or [])
            merged.update(raw_by_tenant[tenant])
            site["seed_urls"] = sorted(merged)

    return existing


def write_discovered_txt(path: str, urls: Iterable[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for url in sorted(set(urls)):
            f.write(url)
            f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover Avature career sites from seed URLs.")
    parser.add_argument("--seeds", default=DEFAULT_SEEDS_FILE, help="Path to urls.txt")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help="Path to output JSON")
    parser.add_argument("--discovered-out", default=DEFAULT_DISCOVERED_FILE, help="Path to discovered txt")
    parser.add_argument("--max-follow", type=int, default=DEFAULT_MAX_FOLLOW, help="Max pages to fetch")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests (seconds)")
    parser.add_argument("--max-seeds", type=int, default=None, help="Max seed URLs to load")
    parser.add_argument("--sample-every", type=int, default=1, help="Keep every Nth seed line")
    args = parser.parse_args()

    seeds = load_seeds(args.seeds, max_seeds=args.max_seeds, sample_every=args.sample_every)
    raw_avature = collect_raw_avature_urls(seeds)
    discovered = discover_avature_urls(seeds, max_follow=args.max_follow, delay=args.delay)

    existing = load_existing_sites(args.output)
    merged = merge_sites(existing, discovered, raw_avature)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    write_discovered_txt(args.discovered_out, raw_avature)
    print(
        f"[discover] seeds={len(seeds)} found={len(discovered)} total={len(merged)} raw_urls={len(raw_avature)}"
    )


if __name__ == "__main__":
    main()
