# utils/http.py
from __future__ import annotations

import time
import requests
from requests import Response

DEFAULT_HEADERS = {
    # A normal desktop browser UA prevents some 406 blocks.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(DEFAULT_HEADERS)


def _request(method: str, url: str, **kwargs) -> Response:
    timeout = kwargs.pop("timeout", 20)
    resp = Response()
    for attempt in range(3):
        resp = session.request(method, url, timeout=timeout, **kwargs)

        # Some Avature tenants send 406 unless headers look like a browser.
        # Retry once with a slightly different Accept.
        if resp.status_code == 406 and attempt < 2:
            session.headers["Accept"] = "*/*"
            time.sleep(0.5 * (attempt + 1))
            continue

        return resp

    return resp


def get(url: str, **kwargs) -> Response:
    return _request("GET", url, **kwargs)


def post(url: str, json=None, **kwargs) -> Response:
    return _request("POST", url, json=json, **kwargs)
