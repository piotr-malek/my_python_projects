import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Template

from mail.markdown_html import markdown_to_html

_HEADLINE_RE = re.compile(r"^[^\n]*\*\*(.+?)\*\*", re.MULTILINE)
_SUBJECT_MAX_LEN = 72


class DigestMailer:
    def __init__(self, settings):
        self._settings = settings
        self._html_template = Template(
            (Path(__file__).parent / "templates" / "digest.html").read_text()
        )

    def send(self, digest_text, analysis, target):
        msg = self._build_message(digest_text, analysis, target)
        try:
            with smtplib.SMTP(self._settings.SMTP_HOST, self._settings.SMTP_PORT) as server:
                server.starttls()
                server.login(self._settings.SMTP_USER, self._settings.SMTP_PASSWORD)
                server.sendmail(self._settings.SMTP_USER, [self._settings.EMAIL_TO], msg.as_string())
        except Exception as e:
            self._settings.FALLBACK_DIGEST_DIR.mkdir(parents=True, exist_ok=True)
            path = self._settings.FALLBACK_DIGEST_DIR / f"digest_{target.isoformat()}.txt"
            path.write_text(digest_text)
            raise RuntimeError(f"SMTP failed; saved to {path}") from e

    def _build_message(self, digest_text, analysis, target):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = self._subject(analysis, target, digest_text)
        msg["From"] = self._settings.SMTP_USER
        msg["To"] = self._settings.EMAIL_TO
        msg.attach(MIMEText(digest_text, "plain", "utf-8"))
        msg.attach(MIMEText(self._render_html(digest_text, target), "html", "utf-8"))
        return msg

    @staticmethod
    def _truncate(text, limit=_SUBJECT_MAX_LEN):
        text = " ".join(str(text).split())
        if len(text) <= limit:
            return text
        return text[: limit - 1].rstrip() + "…"

    @classmethod
    def _extract_headline(cls, digest_text):
        if not digest_text:
            return None
        match = _HEADLINE_RE.search(digest_text)
        if match:
            return match.group(1).strip()
        fallback = re.search(r"\*\*(.+?)\*\*", digest_text)
        return fallback.group(1).strip() if fallback else None

    @classmethod
    def _subject_suffix(cls, analysis, digest_text=None):
        headline = cls._extract_headline(digest_text)
        if headline:
            return cls._truncate(headline)

        payload = analysis.get("digest_payload") or {}
        insights = payload.get("insights") or []
        if insights and isinstance(insights[0], dict):
            summary = insights[0].get("summary")
            if summary:
                return cls._truncate(summary)

        ef = (payload.get("training_load_context") or {}).get("expected_fatigue_today") or {}
        if ef.get("level") in ("moderate", "high"):
            return "expected training recovery"
        if ef.get("level") == "mild":
            return "light training recovery"

        state = payload.get("health_state")
        return {
            "green": "all clear",
            "yellow": "easy day",
            "red": "recovery focus",
        }.get(state)

    def _subject(self, analysis, target, digest_text=None):
        suffix = self._subject_suffix(analysis, digest_text)
        if suffix:
            return f"Physiology Digest \u2014 {target.isoformat()} \u2014 {suffix}"
        return f"Physiology Digest \u2014 {target.isoformat()}"

    def _render_html(self, digest_text, target):
        return self._html_template.render(
            date=target.isoformat(),
            body_html=markdown_to_html(digest_text),
        )
