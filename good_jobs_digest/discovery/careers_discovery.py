"""Find ATS boards by scraping an employer website for careers links."""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from discovery.ats_registry import parse_ats_from_text, probe_board
from discovery.resolve import AtsMatch, careers_url, employer_names_align, slug_aligns_with_company

logger = logging.getLogger(__name__)

_CAREERS_HREF = re.compile(
    r"career|job|hiring|join[\s_-]?us|work[\s_-]?with[\s_-]?us|opportunit|vacanc|open[\s_-]?role",
    re.I,
)
_SKIP_HOSTS = frozenset(
    {
        "facebook.com",
        "twitter.com",
        "x.com",
        "linkedin.com",
        "instagram.com",
        "youtube.com",
        "tiktok.com",
    }
)


def _normalize_website(url: str) -> str:
    raw = url.strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    return raw


def _same_site(base_netloc: str, href: str) -> bool:
    try:
        host = urlparse(href).netloc.lower().replace("www.", "")
    except Exception:  # noqa: BLE001
        return False
    if not host:
        return True
    base = base_netloc.lower().replace("www.", "")
    return host == base or host.endswith(f".{base}")


def careers_link_candidates(html: str, base_url: str) -> list[str]:
    """Return careers-like URLs from a homepage, most promising first."""
    soup = BeautifulSoup(html, "html.parser")
    base_netloc = urlparse(base_url).netloc
    scored: list[tuple[int, str]] = []
    seen: set[str] = set()

    for tag in soup.find_all("a", href=True):
        href = (tag.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        absolute = urljoin(base_url, href)
        key = absolute.lower().split("#")[0].rstrip("/")
        if key in seen:
            continue
        try:
            host = urlparse(absolute).netloc.lower().replace("www.", "")
        except Exception:  # noqa: BLE001
            continue
        if host and any(host == skip or host.endswith(f".{skip}") for skip in _SKIP_HOSTS):
            continue

        text = tag.get_text(" ", strip=True)
        score = 0
        if parse_ats_from_text(absolute):
            score += 100
        if _CAREERS_HREF.search(absolute):
            score += 40
        if _CAREERS_HREF.search(text):
            score += 30
        if _same_site(base_netloc, absolute):
            score += 10
        if score <= 0:
            continue
        seen.add(key)
        scored.append((score, absolute))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [url for _, url in scored[:12]]


def _accept_match(
    *,
    company_name: str,
    slug: str,
    ats: str,
    board_name: str,
    from_parsed_url: bool,
) -> bool:
    if not company_name:
        return True
    slug_ok = slug_aligns_with_company(slug, company_name)
    if ats == "lever":
        return slug_ok
    name_ok = bool(board_name) and employer_names_align(company_name, board_name)
    if from_parsed_url:
        return slug_ok or name_ok
    if board_name and not name_ok:
        return False
    return name_ok or slug_ok


def _match_from_parsed(
    client: httpx.Client,
    ats: str,
    slug: str,
    *,
    company_name: str,
    try_eu_lever: bool,
) -> AtsMatch | None:
    if ats == "lever" and try_eu_lever:
        for region in ("global", "eu"):
            result = probe_board(client, ats, slug, region=region)
            if result.has_jobs and _accept_match(
                company_name=company_name,
                slug=slug,
                ats=ats,
                board_name=result.board_name,
                from_parsed_url=True,
            ):
                return AtsMatch(
                    ats_type="lever",
                    ats_slug=slug,
                    ats_region=region,
                    careers_url=careers_url("lever", slug, region=region),
                    board_display_name=result.board_name,
                )
        return None

    result = probe_board(client, ats, slug)
    if not result.has_jobs:
        return None
    if not _accept_match(
        company_name=company_name,
        slug=slug,
        ats=ats,
        board_name=result.board_name,
        from_parsed_url=True,
    ):
        return None
    return AtsMatch(
        ats_type=ats,
        ats_slug=slug,
        careers_url=careers_url(ats, slug),
        board_display_name=result.board_name,
    )


def discover_ats_from_website(
    client: httpx.Client,
    website: str,
    *,
    company_name: str = "",
    try_eu_lever: bool = False,
) -> AtsMatch | None:
    """Fetch homepage, follow careers links, parse ATS URLs before slug guessing."""
    start_url = _normalize_website(website)
    if not start_url:
        return None
    try:
        resp = client.get(start_url, timeout=12.0)
    except httpx.RequestError as exc:
        logger.debug("Website fetch failed for %s: %s", start_url, exc)
        return None
    if resp.status_code >= 400 or not resp.text:
        return None

    parsed_from_html = parse_ats_from_text(resp.text)
    if parsed_from_html:
        ats, slug = parsed_from_html
        hit = _match_from_parsed(
            client, ats, slug, company_name=company_name, try_eu_lever=try_eu_lever
        )
        if hit:
            return hit

    for link in careers_link_candidates(resp.text, str(resp.url)):
        parsed = parse_ats_from_text(link)
        if not parsed:
            try:
                page = client.get(link, timeout=12.0)
            except httpx.RequestError:
                continue
            if page.status_code >= 400 or not page.text:
                continue
            parsed = parse_ats_from_text(page.text) or parse_ats_from_text(str(page.url))
        if not parsed:
            continue
        ats, slug = parsed
        hit = _match_from_parsed(
            client, ats, slug, company_name=company_name, try_eu_lever=try_eu_lever
        )
        if hit:
            return hit
    return None
