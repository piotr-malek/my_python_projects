import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Template

from mail.markdown_html import markdown_to_html

_STATE_MARKERS = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
_STATE_LABELS = {
    "green": "all clear",
    "yellow": "easy day",
    "red": "rest day",
}
# Only used when yellow/red — names the health angle, not the workout.
_HEALTH_CATEGORY_LABELS = {
    "stress": "stress up",
    "sleep": "sleep off",
    "recovery": "recovery low",
    "circadian": "rhythm off",
    "illness_watch": "health watch",
    "cognitive": "focus low",
}


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

    @classmethod
    def _subject_suffix(cls, analysis):
        """Short health status — headline stays in the email body."""
        payload = analysis.get("digest_payload") or {}
        state = payload.get("health_state")
        marker = _STATE_MARKERS.get(state, "")
        label = _STATE_LABELS.get(state)

        if state in ("yellow", "red"):
            insights = payload.get("insights") or []
            category = (
                insights[0].get("category") if insights and isinstance(insights[0], dict) else None
            )
            if category in _HEALTH_CATEGORY_LABELS:
                label = _HEALTH_CATEGORY_LABELS[category]
            elif category == "training_response":
                label = "recovery soft" if state == "yellow" else "rest day"

        if not label:
            return None
        return f"{marker} {label}".strip() if marker else label

    def _subject(self, analysis, target, digest_text=None):
        suffix = self._subject_suffix(analysis)
        date_short = target.strftime("%d %b")
        if suffix:
            return f"Physiology \u2014 {date_short} \u2014 {suffix}"
        return f"Physiology \u2014 {date_short}"

    def _render_html(self, digest_text, target):
        return self._html_template.render(
            date=target.isoformat(),
            body_html=markdown_to_html(digest_text),
        )
