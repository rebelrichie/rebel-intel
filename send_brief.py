"""
Rebel Talent Systems — Daily Brief Email Sender v4.0
Sends PDF brief to subscriber list via Gmail SMTP
"""

import os
import smtplib
import json
import time
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formatdate

# ─── CONFIG ──────────────────────────────────────────────────────────────────

GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
SEND_TO = os.environ.get("SEND_TO", "")
BRIEF_CC = os.environ.get("BRIEF_CC", "")
SITE_URL = os.environ.get("SITE_URL", "https://rebelrichie.github.io/rebel-intel")
SHEET_ID = os.environ.get("SHEET_ID", "")
SHEETS_API_KEY = os.environ.get("SHEETS_API_KEY", "")
PDF_PATH = "docs/daily_brief.pdf"
TODAY = datetime.date.today().strftime("%B %d, %Y")
TODAY_SLUG = datetime.date.today().strftime("%Y-%m-%d")


# ─── SUBSCRIBER LIST ─────────────────────────────────────────────────────────

def get_subscribers():
    """Pull subscriber list from Google Sheet or fall back to SEND_TO."""
    emails = []

    if SHEET_ID and SHEETS_API_KEY:
        try:
            import requests
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/Sheet1!A:A?key={SHEETS_API_KEY}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            values = resp.json().get("values", [])
            emails = [row[0].strip() for row in values[1:] if row and "@" in row[0]]
            print(f"  {len(emails)} subscribers from Google Sheet")
        except Exception as e:
            print(f"  [WARN] Sheet pull failed: {e} - falling back to SEND_TO")

    if not emails and SEND_TO:
        emails = [e.strip() for e in SEND_TO.split(",") if e.strip() and "@" in e]
        print(f"  {len(emails)} recipients from SEND_TO secret")

    return emails


# ─── EMAIL BUILDER ───────────────────────────────────────────────────────────

def build_html_body():
    """Build the HTML email body with summary from data.json."""
    moves_html = ""
    try:
        with open("docs/data.json") as f:
            data = json.load(f)
        moves = data.get("moves_today", [])
        top3 = data.get("top_3", [])

        moves_items = "".join(f"<li style='margin-bottom:8px;'>{m}</li>" for m in moves)
        top3_items = "".join(f"<li style='margin-bottom:8px; color:#7CCBC3;'>{t}</li>" for t in top3)

        moves_html = f"""
        <h3 style="color:#E2C46B; font-family:sans-serif; margin-top:24px;">Your Moves Today</h3>
        <ul style="color:#e0e8f0; font-family:sans-serif; line-height:1.6;">{moves_items}</ul>
        <h3 style="color:#7CCBC3; font-family:sans-serif; margin-top:24px;">Top 3 Must-Chase</h3>
        <ul style="color:#e0e8f0; font-family:sans-serif; line-height:1.6;">{top3_items}</ul>
        """
    except Exception:
        moves_html = "<p style='color:#e0e8f0; font-family:sans-serif;'>See attached PDF for full brief.</p>"

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="background:#050d1b; margin:0; padding:32px;">
  <div style="max-width:600px; margin:0 auto;">

    <div style="border-bottom:2px solid #7CCBC3; padding-bottom:16px; margin-bottom:24px;">
      <h1 style="color:#7CCBC3; font-family:sans-serif; font-size:22px; margin:0;">
        REBEL TALENT SYSTEMS
      </h1>
      <p style="color:#E2C46B; font-family:monospace; font-size:12px; margin:4px 0 0;">
        BD INTEL BRIEF &mdash; {TODAY}
      </p>
    </div>

    <p style="color:#a0b4c8; font-family:sans-serif; font-size:14px; line-height:1.6;">
      Your daily fractional recruiting intelligence is attached.
      Today's brief covers signals from {len(data.get('source_stats', {}))} data sources,
      including funding rounds, YC companies, hiring signals, and executive moves
      across Series A-C startups.
    </p>

    {moves_html}

    <div style="margin-top:32px; padding-top:16px; border-top:1px solid #1a2a3a;">
      <a href="{SITE_URL}"
         style="display:inline-block; background:#7CCBC3; color:#050d1b;
                font-family:sans-serif; font-weight:bold; font-size:13px;
                padding:10px 20px; text-decoration:none; border-radius:3px;">
        View Live Dashboard
      </a>
      <p style="color:#4a6070; font-family:monospace; font-size:11px; margin-top:16px;">
        Rebel Talent Systems &middot; Fractional Head of Talent &middot; Series A-C Startups<br>
        $8K-$14K/month &middot; 350% average ROI
      </p>
    </div>

  </div>
</body>
</html>
"""


def build_plain_text():
    """Build plain-text fallback for email clients that don't render HTML."""
    try:
        with open("docs/data.json") as f:
            data = json.load(f)
        moves = data.get("moves_today", [])
        top3 = data.get("top_3", [])

        lines = [
            f"REBEL TALENT SYSTEMS - BD Intel Brief - {TODAY}",
            "=" * 50,
            "",
            "YOUR MOVES TODAY:",
        ]
        for m in moves:
            lines.append(f"  - {m}")
        lines.append("")
        lines.append("TOP 3 MUST-CHASE:")
        for t in top3:
            lines.append(f"  - {t}")
        lines.append("")
        if data.get("cross_references"):
            lines.append("")
            lines.append("CROSS-REFERENCES (multi-source):")
            for xr in data["cross_references"][:5]:
                lines.append(f"  - {xr['company']} ({xr['source_count']} sources: {', '.join(xr['sources'])})")

        lines.append("")
        lines.append(f"Full brief attached. Dashboard: {SITE_URL}")
        lines.append("")
        lines.append("---")
        lines.append("Rebel Talent Systems | Fractional Head of Talent | $8K-$14K/mo")
        return "\n".join(lines)
    except Exception:
        return f"Rebel Talent BD Intel Brief - {TODAY}\n\nSee attached PDF for full brief.\n\nDashboard: {SITE_URL}"


# ─── SEND ────────────────────────────────────────────────────────────────────

def send_brief():
    print(f"\n{'='*60}")
    print(f"REBEL TALENT - Sending Daily Brief")
    print(f"  {TODAY}")
    print(f"{'='*60}\n")

    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print("  [ERROR] GMAIL_USER or GMAIL_APP_PASSWORD not set - aborting")
        return

    recipients = get_subscribers()
    if not recipients:
        print("  [ERROR] No recipients found - aborting")
        return

    cc_list = [e.strip() for e in BRIEF_CC.split(",") if e.strip() and "@" in e] if BRIEF_CC else []

    # Build email with both HTML and plain-text
    msg = MIMEMultipart("mixed")
    msg["From"] = f"Rebel Talent Intel <{GMAIL_USER}>"
    msg["To"] = ", ".join(recipients)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = f"Rebel Talent BD Brief - {TODAY}"
    msg["Date"] = formatdate(localtime=True)

    # Create alternative part for HTML + plain text
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(build_plain_text(), "plain"))
    alt_part.attach(MIMEText(build_html_body(), "html"))
    msg.attach(alt_part)

    # Attach PDF
    if os.path.exists(PDF_PATH):
        with open(PDF_PATH, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="Rebel_Talent_Brief_{TODAY_SLUG}.pdf"',
            )
            msg.attach(part)
        print(f"  PDF attached: {PDF_PATH}")
    else:
        print(f"  [WARN] PDF not found at {PDF_PATH} - sending without attachment")

    # Send with retry
    all_recipients = recipients + cc_list
    for attempt in range(2):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
                server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
                server.sendmail(GMAIL_USER, all_recipients, msg.as_string())
            print(f"  Sent to {len(all_recipients)} recipients")
            return
        except Exception as e:
            print(f"  [WARN] Send failed (attempt {attempt + 1}): {e}")
            if attempt < 1:
                time.sleep(5)

    print("  [ERROR] All send attempts failed")


if __name__ == "__main__":
    send_brief()
