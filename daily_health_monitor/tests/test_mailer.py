from datetime import date

from mail.mailer import DigestMailer
from mail.markdown_html import markdown_to_html


def test_headings_and_bold_render():
    md = "# Title\n\n**bold**\n\n## Section\n\n> note\n\n- one\n- two\n"
    html = markdown_to_html(md)
    assert "<h1>Title</h1>" in html
    assert "<h2>Section</h2>" in html
    assert "<strong>bold</strong>" in html
    assert "<blockquote>note</blockquote>" in html
    assert "<ul><li>one</li><li>two</li></ul>" in html


def test_html_escapes_special_chars():
    md = "## A & B\n\n- 1 < 2\n"
    html = markdown_to_html(md)
    assert "&amp;" in html
    assert "&lt;" in html


def test_paragraph_break_on_blank_line():
    md = "para one\n\npara two\n"
    html = markdown_to_html(md)
    assert "<p>para one</p>" in html
    assert "<p>para two</p>" in html


def test_italic_renders_as_em():
    md = "- **Bed earlier** — _recover sleep deficit._\n"
    html = markdown_to_html(md)
    assert "<em>recover sleep deficit.</em>" in html


def test_italic_does_not_match_inside_identifiers():
    md = "Field name evidence_field stays literal.\n"
    html = markdown_to_html(md)
    assert "evidence_field" in html
    assert "<em>" not in html


def _mailer():
    # Bypass __init__ — we only need _subject, which doesn't touch SMTP config.
    return DigestMailer.__new__(DigestMailer)


def test_subject_reflects_health_state_yellow():
    subject = _mailer()._subject(
        {"digest_payload": {"health_state": "yellow"}}, date(2026, 5, 25)
    )
    assert subject == "Physiology Digest \u2014 2026-05-25 \u2014 easy day"


def test_subject_reflects_health_state_red():
    subject = _mailer()._subject(
        {"digest_payload": {"health_state": "red"}}, date(2026, 5, 25)
    )
    assert "rest day" in subject
    assert "Recovery" not in subject


def test_subject_falls_back_when_no_state():
    subject = _mailer()._subject({}, date(2026, 5, 25))
    assert subject == "Physiology Digest \u2014 2026-05-25"
