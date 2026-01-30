import hashlib
from bs4 import BeautifulSoup


def normalize(job: dict, tenant: str) -> dict:
    description_html = job.get("description") or ""
    soup = BeautifulSoup(description_html, "html.parser")

    return {
        "id": job.get("id") or job.get("jobId"),
        "title": job.get("title"),
        "location": job.get("location"),
        "posted_date": job.get("postedDate"),
        "apply_url": job.get("applyUrl") or job.get("url"),
        "description_html": description_html,
        "description_text": soup.get_text(" ", strip=True),
        "tenant": tenant,
        "fingerprint": hashlib.sha256(
        (job.get("title", "") + description_html).encode()
        ).hexdigest()
    }