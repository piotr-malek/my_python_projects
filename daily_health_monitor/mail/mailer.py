import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Template

from mail.markdown_html import markdown_to_html


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
        msg["Subject"] = self._subject(analysis, target)
        msg["From"] = self._settings.SMTP_USER
        msg["To"] = self._settings.EMAIL_TO
        msg.attach(MIMEText(digest_text, "plain", "utf-8"))
        msg.attach(MIMEText(self._render_html(digest_text, target), "html", "utf-8"))
        return msg

    _STATE_LABELS = {
        "red": "rest day",
        "yellow": "easy day",
        "green": "normal day",
    }

    def _subject(self, analysis, target):
        state = (analysis.get("digest_payload") or {}).get("health_state")
        suffix = f" \u2014 {self._STATE_LABELS[state]}" if state in self._STATE_LABELS else ""
        return f"Physiology Digest \u2014 {target.isoformat()}{suffix}"

    def _render_html(self, digest_text, target):
        return self._html_template.render(
            date=target.isoformat(),
            body_html=markdown_to_html(digest_text),
        )
