"""Tiny Markdown-to-HTML converter for digest emails."""

import html
import re

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_RE = re.compile(r"(?<![A-Za-z0-9])_([^_\n]+?)_(?![A-Za-z0-9])")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def markdown_to_html(text):
    lines = text.splitlines()
    out = []
    bullets = []
    paragraph = []

    def _flush_bullets():
        if bullets:
            out.append("<ul>" + "".join(f"<li>{b}</li>" for b in bullets) + "</ul>")
            bullets.clear()

    def _flush_paragraph():
        if paragraph:
            out.append("<p>" + " ".join(paragraph) + "</p>")
            paragraph.clear()

    def _flush_all():
        _flush_bullets()
        _flush_paragraph()

    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            _flush_all()
            continue
        if line.startswith("# "):
            _flush_all()
            out.append(f"<h1>{_inline(line[2:])}</h1>")
        elif line.startswith("## "):
            _flush_all()
            out.append(f"<h2>{_inline(line[3:])}</h2>")
        elif line.startswith("### "):
            _flush_all()
            out.append(f"<h3>{_inline(line[4:])}</h3>")
        elif line.startswith("> "):
            _flush_all()
            out.append(f"<blockquote>{_inline(line[2:])}</blockquote>")
        elif line.lstrip().startswith("- "):
            _flush_paragraph()
            bullets.append(_inline(line.lstrip()[2:]))
        else:
            _flush_bullets()
            paragraph.append(_inline(line.rstrip("  ")))

    _flush_all()
    return "\n".join(out)


def _inline_inner(text: str) -> str:
    escaped = html.escape(text)
    escaped = _BOLD_RE.sub(r"<strong>\1</strong>", escaped)
    return _ITALIC_RE.sub(r"<em>\1</em>", escaped)


def _inline(text: str) -> str:
    parts: list[str] = []
    pos = 0
    for match in _LINK_RE.finditer(text):
        if match.start() > pos:
            parts.append(_inline_inner(text[pos : match.start()]))
        url = html.escape(match.group(2), quote=True)
        parts.append(f'<a href="{url}">{_inline_inner(match.group(1))}</a>')
        pos = match.end()
    if pos < len(text):
        parts.append(_inline_inner(text[pos:]))
    return "".join(parts) if parts else _inline_inner(text)
