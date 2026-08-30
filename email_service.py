import os
import smtplib
import secrets
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger("kambus_email")

def generate_otp_code(length: int = 6) -> str:
    """Generate a cryptographically secure 6-digit numeric OTP."""
    digits = "0123456789"
    return "".join(secrets.choice(digits) for _ in range(length))


def send_student_verification_email(recipient_email: str, student_name: str, otp_code: str) -> tuple[bool, str | None]:
    """
    Sends a verification email with a 6-digit OTP to the student.
    Returns (success: bool, error: str | None).
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER") or os.getenv("EMAIL_USER")
    smtp_password = os.getenv("SMTP_PASSWORD") or os.getenv("EMAIL_PASSWORD") or os.getenv("SMTP_PASS")
    smtp_from = os.getenv("SMTP_FROM") or smtp_user or "noreply@kambus.app"

    subject = "KAMBUS — Verify your student email"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; margin: 0; padding: 20px; }}
            .container {{ max-width: 480px; margin: 0 auto; background-color: #ffffff; border-radius: 16px; border: 1px solid #e2e8f0; padding: 28px 24px; }}
            .header {{ text-align: center; margin-bottom: 20px; }}
            .brand {{ color: #4F46E5; font-size: 24px; font-weight: 800; letter-spacing: 1px; margin: 0; }}
            .subtitle {{ color: #64748B; font-size: 12px; font-weight: 600; margin-top: 4px; }}
            .content-box {{ background-color: #F8FAFC; border-radius: 12px; padding: 20px; text-align: center; margin: 20px 0; border: 1px solid #EEF2FF; }}
            .greeting {{ color: #1E293B; font-size: 14px; font-weight: 600; margin-bottom: 8px; }}
            .instruction {{ color: #475569; font-size: 13px; margin: 0 0 16px 0; line-height: 1.5; }}
            .otp-code {{ font-size: 32px; font-weight: 800; letter-spacing: 6px; color: #1E1B4B; background: #EEF2FF; padding: 12px 24px; border-radius: 10px; display: inline-block; border: 1px dashed #6366F1; }}
            .expiry-note {{ color: #94A3B8; font-size: 11px; font-weight: 500; margin-top: 14px; }}
            .footer {{ text-align: center; color: #94A3B8; font-size: 11px; margin-top: 20px; line-height: 1.4; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 class="brand">KAMBUS</h1>
                <p class="subtitle">Campus Transport Operating System</p>
            </div>
            <div class="content-box">
                <div class="greeting">Hello {student_name or 'Student'},</div>
                <p class="instruction">Enter the 6-digit verification code below to verify your email address and activate your KAMBUS account:</p>
                <div class="otp-code">{otp_code}</div>
                <p class="expiry-note">⏱️ This code expires in 10 minutes. For your security, do not share this code with anyone.</p>
            </div>
            <div class="footer">
                If you did not request this verification code, please ignore this email.<br>
                &copy; KAMBUS Transport Intelligence
            </div>
        </div>
    </body>
    </html>
    """

    # If SMTP is configured, attempt sending real email
    if smtp_host and smtp_user and smtp_password:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"KAMBUS <{smtp_from}>"
            msg["To"] = recipient_email

            part = MIMEText(html_content, "html")
            msg.attach(part)

            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10) as server:
                    server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_from, [recipient_email], msg.as_string())
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                    server.ehlo()
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_from, [recipient_email], msg.as_string())

            logger.info(f"Verification email successfully dispatched to {recipient_email}")
            return True, None
        except Exception as e:
            logger.error(f"Failed to dispatch verification email via SMTP: {e}")
            # Fall back to simulated email dispatch in dev environments
            return False, str(e)
    else:
        # Development / Simulation mode
        logger.info(f"[SIMULATED EMAIL] Verification code dispatched for {recipient_email}")
        return True, None
