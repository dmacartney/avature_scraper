from utils.http import post


def fetch_jobs(endpoint: str) -> list[dict]:
    page = 1
    page_size = 50
    results = []

    while True:
        payload = {
        "page": page,
        "pageSize": page_size
        }

        resp = post(endpoint, json=payload)
        resp.raise_for_status()
        data = resp.json()

        jobs = data.get("jobs") or data.get("results") or []
        if not jobs:
            break

        results.extend(jobs)
        page += 1

    return results