"""Personio XML job feed."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def fetch_personio_jobs(
    http: HostRateLimitedHttp, company: str, *, tld: str = "de"
) -> tuple[list[dict[str, Any]], int]:
    url = f"https://{company}.jobs.personio.{tld}/xml"
    r = http.get(url)
    if r.status_code == 404:
        if tld == "de":
            return fetch_personio_jobs(http, company, tld="com")
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError:
        return [], 200
    jobs: list[dict[str, Any]] = []
    for pos in root.findall(".//position"):
        job: dict[str, Any] = {"id": pos.findtext("id"), "tld": tld}
        for child in pos:
            if child.tag:
                job[child.tag] = child.text or ""
        jobs.append(job)
    return jobs, 200
