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

    def verify_login(self) -> None:
        """Authenticate without sending. Raises with an actionable message.

        Called before the pipeline does its work so bad credentials surface in
        seconds rather than after a full ingest+score cycle.
        """
        user = self._settings.SMTP_USER
        password = self._settings.SMTP_PASSWORD
        if not user or not password or not self._settings.EMAIL_TO:
            raise RuntimeError("SMTP_USER, SMTP_PASSWORD and EMAIL_TO must all be set")
        if password != password.strip() or password[:1] in {'"', "'"}:
            raise RuntimeError(
                "SMTP_PASSWORD is wrapped in quotes or padded with whitespace — "
                "store the raw app password, without the quotes used in .env"
            )
        # Gmail app passwords are exactly 16 characters (spaces are cosmetic). A
        # truncated paste otherwise surfaces only as an opaque 535.
        if "gmail" in (self._settings.SMTP_HOST or "") and len(password.replace(" ", "")) != 16:
            raise RuntimeError(
                f"SMTP_PASSWORD has {len(password.replace(' ', ''))} characters "
                "(ignoring spaces); a Gmail app password has exactly 16 — it looks "
                "truncated or mistyped"
            )
        timeout = float(getattr(self._settings, "SMTP_TIMEOUT_SECONDS", 30) or 30)
        with smtplib.SMTP(self._settings.SMTP_HOST, self._settings.SMTP_PORT, timeout=timeout) as server:
            server.starttls()
            try:
                server.login(user, password)
            except smtplib.SMTPAuthenticationError as exc:
                raise RuntimeError(
                    f"SMTP login rejected for {user} ({exc.smtp_code}). For Gmail this usually means "
                    "the app password is wrong, has been revoked, or 2FA/app passwords are not enabled."
                ) from exc

    def send(self, digest_text: str, *, digest_date: date, n_jobs: int) -> None:
        msg = self._build_message(digest_text, digest_date=digest_date, n_jobs=n_jobs)
        timeout = float(getattr(self._settings, "SMTP_TIMEOUT_SECONDS", 30) or 30)
        # Without a timeout a wedged connection hangs the scheduled run forever.
        with smtplib.SMTP(self._settings.SMTP_HOST, self._settings.SMTP_PORT, timeout=timeout) as server:
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
