# scraper/parse_jobdetail.py
from __future__ import annotations

import re
from bs4 import BeautifulSoup


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def parse_jobdetail(html: str, url: str, tenant: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # --- Job ID from URL: /JobDetail/<slug>/<id> or /JobDetail/<id>
    job_id = None
    m = re.search(r"/JobDetail/.+?/([0-9]+)(?:\b|/|$)", url)
    if m:
        job_id = m.group(1)
    if not job_id:
        m = re.search(r"/JobDetail/([0-9]+)(?:\b|/|$)", url)
        if m:
            job_id = m.group(1)
    if not job_id:
        m = re.search(r"[?&]jobId=([0-9]+)", url)
        if m:
            job_id = m.group(1)

    # --- Title: first details article value (NOT the header logo H1)
    title = ""
    title_el = soup.select_one(
        "article.article--details .article__content__view__field__value"
    )
    if title_el:
        title = _clean_text(title_el.get_text(" ", strip=True))

    # Fallback: try the JobDetail slug, if needed
    if not title:
        # URL slug is usually title-ish; keep as last resort
        title = url.split("/JobDetail/", 1)[-1].split("/", 1)[0].replace("-", " ")

    # --- Metadata: label/value pairs (Location, Business Area, Ref #, etc.)
    metadata: dict[str, str] = {}
    for field in soup.select("article .article__content__view__field"):
        label_el = field.select_one(".article__content__view__field__label")
        value_el = field.select_one(".article__content__view__field__value")
        if not label_el or not value_el:
            continue
        label = _clean_text(label_el.get_text(" ", strip=True)).rstrip(":")
        value = _clean_text(value_el.get_text(" ", strip=True))
        if label and value:
            metadata[label] = value

    # convenience normalized fields
    location = metadata.get("Location") or metadata.get("location")
    ref_num = metadata.get("Ref #") or metadata.get("Ref#") or metadata.get("Requisition") or metadata.get("Req #")

    # --- Description: rich-text field only
    desc_el = soup.select_one(
        ".field--rich-text .article__content__view__field__value"
    )
    if not desc_el:
        # fallback: find the header 'Description & Requirements' then grab following rich-text block
        desc_el = soup.find("div", class_=re.compile(r"field--rich-text"))

    description_html = ""
    description_text = ""
    if desc_el:
        description_html = str(desc_el)
        description_text = _clean_text(desc_el.get_text(" ", strip=True))

    # --- Apply URL: use visible Apply Now button (login link with jobId)
    apply_url = url
    apply_el = soup.select_one("a.button--primary[href*='Login?jobId=']")
    if apply_el and apply_el.get("href"):
        apply_url = apply_el["href"]

    return {
        "id": job_id,
        "title": title,
        "apply_url": apply_url,
        "tenant": tenant,
        "location": location,
        "ref": ref_num,
        "metadata": metadata,
        "description_html": description_html,
        "description_text": description_text,
        "source": {"kind": "jobdetail_html", "url": url},
    }
