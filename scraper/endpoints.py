import re
from utils.http import get


COMMON_ENDPOINTS = [
    "/services/avature/search",
    "/services/avature/jobs",
    "/api/jobs"
]


def discover_job_endpoint(base_url: str) -> str | None:
    html = get(base_url).text

    for ep in COMMON_ENDPOINTS:
        if ep in html:
            return base_url.rstrip("/") + ep

    matches = re.findall(r"(/services/[^\"']+)", html)
    if matches:
        return base_url.rstrip("/") + matches[0]

    return None