"""Tests for website-first careers discovery."""

from __future__ import annotations

from discovery.careers_discovery import careers_link_candidates


def test_careers_link_candidates_prefers_ats_urls():
    html = """
    <html><body>
      <a href="/about">About</a>
      <a href="https://boards.greenhouse.io/acmeimpact">Careers</a>
      <a href="/jobs">Open roles</a>
    </body></html>
    """
    links = careers_link_candidates(html, "https://acmeimpact.org")
    assert links[0] == "https://boards.greenhouse.io/acmeimpact"
    assert any("/jobs" in link for link in links)


def test_careers_link_candidates_skips_social():
    html = """
    <html><body>
      <a href="https://facebook.com/acme">Facebook</a>
      <a href="/careers">Careers</a>
    </body></html>
    """
    links = careers_link_candidates(html, "https://acme.org")
    assert links == ["https://acme.org/careers"]
