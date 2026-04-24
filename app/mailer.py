"""
Transactional mailer — bilingual (AR+EN) emails.

Uses Resend's HTTPS API (works on Railway/Fly/Vercel where outbound SMTP
is blocked at the network level). Falls back to SMTP via smtplib when
RESEND_API_KEY is not set but SMTP_USER/SMTP_PASSWORD are.

Safe-by-default: if neither is configured, send_mail logs a warning and
returns False instead of raising, so local dev keeps working.
"""
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from typing import Optional

import requests

from config import Config

log = logging.getLogger(__name__)


# ─── Backend detection ──────────────────────────────────────────────────

def _resend_is_configured() -> bool:
    return bool(getattr(Config, "RESEND_API_KEY", "") and Config.MAIL_FROM)


def _smtp_is_configured() -> bool:
    return bool(Config.SMTP_HOST and Config.SMTP_USER and Config.SMTP_PASSWORD)


def mailer_is_configured() -> bool:
    return _resend_is_configured() or _smtp_is_configured()


# ─── Public API ─────────────────────────────────────────────────────────

def send_mail(
    to: str,
    subject: str,
    text_body: str,
    html_body: Optional[str] = None,
) -> bool:
    """Send a transactional email. Returns True on success, False otherwise."""
    if not to:
        return False

    if _resend_is_configured():
        return _send_resend(to, subject, text_body, html_body)
    if _smtp_is_configured():
        return _send_smtp(to, subject, text_body, html_body)

    log.warning("Mailer not configured — would have sent to %s: %s", to, subject)
    return False


# ─── Resend HTTPS API backend ──────────────────────────────────────────

def _send_resend(to, subject, text_body, html_body) -> bool:
    from_field = (
        formataddr((Config.MAIL_FROM_NAME, Config.MAIL_FROM))
        if Config.MAIL_FROM_NAME
        else Config.MAIL_FROM
    )
    payload = {
        "from": from_field,
        "to": [to],
        "subject": subject,
        "text": text_body,
    }
    if html_body:
        payload["html"] = html_body
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {Config.RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )
        if r.status_code >= 400:
            log.error("❌ Resend send failed for %s: %s %s", to, r.status_code, r.text[:200])
            return False
        log.info("✅ Email sent via Resend to %s", to)
        return True
    except Exception as e:
        log.error("❌ Resend request failed for %s: %s", to, e)
        return False


# ─── SMTP fallback backend ──────────────────────────────────────────────

def _send_smtp(to, subject, text_body, html_body) -> bool:
    msg = EmailMessage()
    msg["From"] = formataddr((Config.MAIL_FROM_NAME, Config.MAIL_FROM))
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(text_body, charset="utf-8")
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    try:
        if Config.SMTP_PORT == 465:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(Config.SMTP_HOST, Config.SMTP_PORT, context=ctx, timeout=20) as s:
                s.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
                s.send_message(msg)
        else:
            with smtplib.SMTP(Config.SMTP_HOST, Config.SMTP_PORT, timeout=20) as s:
                s.ehlo()
                if Config.SMTP_USE_TLS:
                    ctx = ssl.create_default_context()
                    s.starttls(context=ctx)
                    s.ehlo()
                s.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
                s.send_message(msg)
        log.info("✅ Email sent via SMTP to %s", to)
        return True
    except Exception as e:
        log.error("❌ SMTP send failed for %s: %s", to, e)
        return False


# ─── Templates ──────────────────────────────────────────────────────────

def password_reset_email(full_name: str, reset_url: str, ttl_minutes: int) -> tuple:
    """Returns (subject, text, html) — bilingual (AR + EN) in one email."""
    name = full_name or ""
    subject = "Ain Real Estate — Password reset / إعادة تعيين كلمة المرور"

    text = f"""Hi {name},

We received a request to reset your Ain Real Estate password.

Use this link to choose a new password (valid for {ttl_minutes} minutes):

{reset_url}

If you didn't request this, you can safely ignore this email.

— Ain Real Estate team


مرحباً {name}،

تلقّينا طلباً لإعادة تعيين كلمة المرور الخاصة بحسابك في Ain Real Estate.

استخدم الرابط التالي لاختيار كلمة مرور جديدة (صالح لمدة {ttl_minutes} دقيقة):

{reset_url}

إذا لم تطلب ذلك، يمكنك تجاهل هذه الرسالة بأمان.

— فريق Ain Real Estate
"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Password reset</title></head>
<body style="margin:0;padding:0;background:#0d0e11;font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#e3e2e6">
  <div style="max-width:560px;margin:40px auto;background:#121316;border-radius:16px;padding:32px;border:1px solid rgba(186,202,193,0.12)">
    <div style="text-align:center;margin-bottom:28px">
      <div style="display:inline-flex;align-items:center;gap:10px">
        <div style="width:44px;height:44px;border-radius:12px;background:linear-gradient(135deg,#45f1bb,#c6bfff);display:inline-flex;align-items:center;justify-content:center;color:#0d0e11;font-weight:700;font-size:20px">A</div>
        <span style="font-size:20px;font-weight:700;color:#e3e2e6">Ain Real Estate</span>
      </div>
    </div>

    <h2 style="color:#e3e2e6;font-size:18px;margin-bottom:12px">Reset your password</h2>
    <p style="color:#bacac1;font-size:14px;line-height:1.7">
      Hi <strong>{name}</strong>, we received a request to reset your Ain Real Estate password.
      Click the button below to set a new one — this link is valid for <strong>{ttl_minutes} minutes</strong>.
    </p>
    <p style="text-align:center;margin:28px 0">
      <a href="{reset_url}" style="display:inline-block;padding:12px 28px;background:linear-gradient(135deg,#45f1bb,#c6bfff);color:#0d0e11;text-decoration:none;border-radius:10px;font-weight:600;font-size:14px">Reset password</a>
    </p>

    <hr style="border:none;border-top:1px solid rgba(186,202,193,0.12);margin:28px 0">

    <h2 dir="rtl" style="color:#e3e2e6;font-size:18px;margin-bottom:12px;text-align:right">إعادة تعيين كلمة المرور</h2>
    <p dir="rtl" style="color:#bacac1;font-size:14px;line-height:1.7;text-align:right">
      مرحباً <strong>{name}</strong>، تلقّينا طلباً لإعادة تعيين كلمة المرور لحسابك.
      اضغط الزر بالأعلى لاختيار كلمة مرور جديدة — الرابط صالح لمدة <strong>{ttl_minutes} دقيقة</strong>.
      إذا لم تطلب ذلك يمكنك تجاهل هذه الرسالة بأمان.
    </p>

    <p style="color:#6e7178;font-size:11px;text-align:center;margin-top:32px;word-break:break-all">
      {reset_url}
    </p>
  </div>
</body>
</html>"""

    return subject, text, html
