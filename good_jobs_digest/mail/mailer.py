"""SMTP digest mailer with HTML + plain parts."""

from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date
from pathlib import Path

from jinja2 import Template
from markupsafe import Markup

from mail.markdown_html import markdown_to_html


class JobDigestMailer:
    def __init__(self, settings):
        self._settings = settings
        tpl_path = Path(__file__).parent / "templates" / "digest.html"
        self._html_template = Template(tpl_path.read_text(encoding="utf-8"))

    def send(self, digest_text: str, *, digest_date: date, n_jobs: int) -> None:
        msg = self._build_message(digest_text, digest_date=digest_date, n_jobs=n_jobs)
        with smtplib.SMTP(self._settings.SMTP_HOST, self._settings.SMTP_PORT) as server:
            server.starttls()
            server.login(self._settings.SMTP_USER, self._settings.SMTP_PASSWORD)
            server.sendmail(self._settings.SMTP_USER, [self._settings.EMAIL_TO], msg.as_string())

    def _build_message(self, digest_text: str, *, digest_date: date, n_jobs: int):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Job digest — {digest_date.isoformat()} — {n_jobs} matches"
        msg["From"] = self._settings.SMTP_USER
        msg["To"] = self._settings.EMAIL_TO
        msg.attach(MIMEText(digest_text, "plain", "utf-8"))
        body_html = self._html_template.render(
            date=digest_date.isoformat(),
            body_html=Markup(markdown_to_html(digest_text)),
        )
        msg.attach(MIMEText(body_html, "html", "utf-8"))
        return msg

    def write_fallback(self, digest_text: str, *, digest_date: date) -> Path:
        self._settings.FALLBACK_DIGEST_DIR.mkdir(parents=True, exist_ok=True)
        path = self._settings.FALLBACK_DIGEST_DIR / f"digest_{digest_date.isoformat()}.txt"
        path.write_text(digest_text, encoding="utf-8")
        return path
