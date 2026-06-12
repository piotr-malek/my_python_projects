"""HTML cleanup and plain-text extraction."""

from __future__ import annotations

import html as html_lib
import re
from typing import Any

import html2text
from bs4 import BeautifulSoup

_h2t = html2text.HTML2Text()
_h2t.ignore_links = False
_h2t.body_width = 0


def strip_html_to_text(raw: str | None) -> str:
    if not raw:
        return ""
    unescaped = html_lib.unescape(raw)
    soup = BeautifulSoup(unescaped, "html.parser")
    text = soup.get_text("\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def html_to_markdownish(raw: str | None) -> str:
    if not raw:
        return ""
    unescaped = html_lib.unescape(raw or "")
    return _h2t.handle(unescaped).strip()


def collapse_ws(s: str) -> str:
    return re.sub(r"[ \t]+", " ", s.replace("\r\n", "\n")).strip()
