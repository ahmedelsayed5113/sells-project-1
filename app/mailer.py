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

# ─── Signup approval lifecycle templates ────────────────────────────────

_BRAND_HEADER = """
    <div style="text-align:center;margin-bottom:28px">
      <div style="display:inline-flex;align-items:center;gap:10px">
        <div style="width:44px;height:44px;border-radius:12px;background:linear-gradient(135deg,#45f1bb,#c6bfff);display:inline-flex;align-items:center;justify-content:center;color:#0d0e11;font-weight:700;font-size:20px">A</div>
        <span style="font-size:20px;font-weight:700;color:#e3e2e6">Ain Real Estate</span>
      </div>
    </div>
"""


def _wrap_html(inner: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#0d0e11;font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#e3e2e6">
  <div style="max-width:560px;margin:40px auto;background:#121316;border-radius:16px;padding:32px;border:1px solid rgba(186,202,193,0.12)">
    {_BRAND_HEADER}
    {inner}
  </div>
</body></html>"""


def signup_pending_email(full_name: str) -> tuple:
    name = full_name or ""
    subject = "Ain Real Estate — Signup received / تم استلام طلب التسجيل"
    text = f"""Hi {name},

Thanks for signing up to Ain Real Estate. Your request has been queued for admin approval.
You'll receive another email once your account is approved (or if it's declined).

— Ain Real Estate team


مرحباً {name}،

شكراً لتسجيلك في Ain Real Estate. تم استلام طلبك وسيقوم الأدمين بمراجعته قريباً.
سنرسل لك بريداً آخر فور الموافقة على حسابك (أو إذا تم رفض الطلب).

— فريق Ain Real Estate
"""
    inner = f"""
    <h2 style="color:#e3e2e6;font-size:18px;margin-bottom:12px">Signup received</h2>
    <p style="color:#bacac1;font-size:14px;line-height:1.7">
      Hi <strong>{name}</strong>, your registration is in the queue for admin approval. You'll get another
      email once your account is approved.
    </p>
    <hr style="border:none;border-top:1px solid rgba(186,202,193,0.12);margin:28px 0">
    <h2 dir="rtl" style="color:#e3e2e6;font-size:18px;margin-bottom:12px;text-align:right">تم استلام طلب التسجيل</h2>
    <p dir="rtl" style="color:#bacac1;font-size:14px;line-height:1.7;text-align:right">
      مرحباً <strong>{name}</strong>، طلبك في انتظار موافقة الأدمين. سنرسل لك بريداً آخر فور الموافقة على حسابك.
    </p>
    """
    return subject, text, _wrap_html(inner)


def signup_approved_email(full_name: str) -> tuple:
    name = full_name or ""
    subject = "Ain Real Estate — Account approved / تم تفعيل الحساب"
    text = f"""Hi {name},

Good news — your Ain Real Estate account has been approved. You can now sign in
with the username and password you chose at registration.

— Ain Real Estate team


مرحباً {name}،

تمت الموافقة على حسابك في Ain Real Estate. يمكنك تسجيل الدخول الآن باستخدام اسم المستخدم
وكلمة المرور التي اخترتها عند التسجيل.

— فريق Ain Real Estate
"""
    inner = f"""
    <h2 style="color:#e3e2e6;font-size:18px;margin-bottom:12px">Account approved</h2>
    <p style="color:#bacac1;font-size:14px;line-height:1.7">
      Hi <strong>{name}</strong>, your account has been approved. You can sign in now using your
      registered username and password.
    </p>
    <hr style="border:none;border-top:1px solid rgba(186,202,193,0.12);margin:28px 0">
    <h2 dir="rtl" style="color:#e3e2e6;font-size:18px;margin-bottom:12px;text-align:right">تم تفعيل الحساب</h2>
    <p dir="rtl" style="color:#bacac1;font-size:14px;line-height:1.7;text-align:right">
      مرحباً <strong>{name}</strong>، تمت الموافقة على حسابك. يمكنك الآن تسجيل الدخول باستخدام اسم المستخدم
      وكلمة المرور.
    </p>
    """
    return subject, text, _wrap_html(inner)


def signup_rejected_email(full_name: str) -> tuple:
    name = full_name or ""
    subject = "Ain Real Estate — Signup request update / تحديث طلب التسجيل"
    text = f"""Hi {name},

We're sorry — your Ain Real Estate signup request was not approved at this time.
If you believe this was a mistake, please contact your administrator.

— Ain Real Estate team


مرحباً {name}،

نأسف، لم تتم الموافقة على طلب التسجيل في Ain Real Estate. إذا كنت تعتقد أن هذا خطأ،
يرجى التواصل مع الإدارة.

— فريق Ain Real Estate
"""
    inner = f"""
    <h2 style="color:#e3e2e6;font-size:18px;margin-bottom:12px">Signup request update</h2>
    <p style="color:#bacac1;font-size:14px;line-height:1.7">
      Hi <strong>{name}</strong>, your signup request was not approved at this time. Please contact
      your administrator if you believe this was a mistake.
    </p>
    <hr style="border:none;border-top:1px solid rgba(186,202,193,0.12);margin:28px 0">
    <h2 dir="rtl" style="color:#e3e2e6;font-size:18px;margin-bottom:12px;text-align:right">تحديث طلب التسجيل</h2>
    <p dir="rtl" style="color:#bacac1;font-size:14px;line-height:1.7;text-align:right">
      مرحباً <strong>{name}</strong>، لم تتم الموافقة على طلب التسجيل. يرجى التواصل مع الإدارة إذا كنت
      تعتقد أن هذا خطأ.
    </p>
    """
    return subject, text, _wrap_html(inner)


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
