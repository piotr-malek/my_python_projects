"""Tiny Markdown-to-HTML converter for the constrained vocabulary the digest emits.

Supports: # h1, ## h2, **bold**, _italic_, > blockquote, - bullets, blank-line paragraphs.
Outputs clean, mobile-friendly HTML fragments. Not a general-purpose parser.
"""
import html
import re

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
# Italic: _text_ where text has no underscores. Avoids matching identifiers like evidence_field.
_ITALIC_RE = re.compile(r"(?<![A-Za-z0-9])_([^_\n]+?)_(?![A-Za-z0-9])")


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


def _inline(text):
    escaped = html.escape(text)
    escaped = _BOLD_RE.sub(r"<strong>\1</strong>", escaped)
    return _ITALIC_RE.sub(r"<em>\1</em>", escaped)
