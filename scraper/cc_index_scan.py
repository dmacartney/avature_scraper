from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import re
from typing import Iterable

import requests

DEFAULT_BASE_URL = "https://data.commoncrawl.org/"
DEFAULT_OUTPUT = "output/avature_discovered.txt"
DEFAULT_STATE = "output/cc_index_state.txt"
DEFAULT_BUFFER_LINES = 5000

_URL_RE = re.compile(r"https?://[^\s)>\"]+")


def iter_paths(path_file: str) -> Iterable[str]:
    with open(path_file, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            yield line


def load_state(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def append_state(path: str, item: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(item)
        f.write("\n")


def extract_url(line: str) -> str | None:
    # Try JSON first (CDX lines are JSON objects).
    try:
        obj = json.loads(line)
        if isinstance(obj, dict) and "url" in obj:
            return str(obj["url"])
    except Exception:
        pass

    # Fallback: regex search.
    m = _URL_RE.search(line)
    if m:
        return m.group(0)
    return None


def iter_gzip_lines(url: str) -> Iterable[tuple[str, int]]:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for raw in gz:
                decoded = raw.decode("utf-8", errors="ignore")
                yield decoded, len(raw)


def scan_paths(
    paths: Iterable[str],
    *,
    base_url: str,
    match_host: str,
    output_path: str,
    state_path: str,
    max_files: int | None,
    dedupe: bool,
    buffer_lines: int,
    max_bytes: int | None,
) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    processed = load_state(state_path) if state_path else set()
    seen: set[str] = set() if dedupe else set()

    out_mode = "a" if os.path.exists(output_path) else "w"
    with open(output_path, out_mode, encoding="utf-8") as out:
        processed_count = 0
        buffered: list[str] = []
        bytes_read = 0
        for rel in paths:
            if max_files is not None and processed_count >= max_files:
                break
            if rel in processed:
                continue

            url = rel if rel.startswith("http") else base_url.rstrip("/") + "/" + rel.lstrip("/")
            for line, raw_len in iter_gzip_lines(url):
                bytes_read += raw_len
                if max_bytes is not None and bytes_read >= max_bytes:
                    break
                if match_host not in line:
                    continue
                found = extract_url(line)
                if not found:
                    continue
                if match_host not in found:
                    continue
                if dedupe:
                    if found in seen:
                        continue
                    seen.add(found)
                buffered.append(found)
                if len(buffered) >= buffer_lines:
                    out.write("\n".join(buffered))
                    out.write("\n")
                    buffered.clear()

            if state_path:
                append_state(state_path, rel)
            processed_count += 1
            if max_bytes is not None and bytes_read >= max_bytes:
                break

        if buffered:
            out.write("\n".join(buffered))
            out.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan Common Crawl index shards for Avature URLs.")
    parser.add_argument("--paths-file", required=True, help="Local file containing index shard paths")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL for CC data")
    parser.add_argument("--match-host", default="avature.net", help="Host substring to match")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output file for discovered URLs")
    parser.add_argument("--state", default=DEFAULT_STATE, help="Checkpoint file for processed paths")
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Ignore checkpoint file and rescan from start",
    )
    parser.add_argument("--max-files", type=int, default=5, help="Max number of shard files to scan")
    parser.add_argument("--buffer-lines", type=int, default=DEFAULT_BUFFER_LINES, help="Write buffer size")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=None,
        help="Stop after reading this many uncompressed bytes (approx)",
    )
    parser.add_argument("--dedupe", action="store_true", help="Dedupe URLs during scan (memory heavy)")
    args = parser.parse_args()

    state_path = "" if args.ignore_state else args.state
    scan_paths(
        iter_paths(args.paths_file),
        base_url=args.base_url,
        match_host=args.match_host,
        output_path=args.output,
        state_path=state_path,
        max_files=args.max_files,
        dedupe=args.dedupe,
        buffer_lines=args.buffer_lines,
        max_bytes=args.max_bytes,
    )


if __name__ == "__main__":
    main()
