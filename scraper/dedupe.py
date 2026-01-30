def dedupe(jobs: list[dict]) -> list[dict]:
    seen = set()
    unique = []

    for job in jobs:
        key = (job.get("tenant"), job.get("id") or job.get("fingerprint"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(job)

    return unique