"""
Rebel Talent Systems — Daily Brief Email Sender v5.0
Sends PDF brief to hardcoded recipients + optional subscriber list via Gmail SMTP
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

GMAIL_USER        = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
SEND_TO           = os.environ.get("SEND_TO", "")
BRIEF_CC          = os.environ.get("BRIEF_CC", "")
SITE_URL          = os.environ.get("SITE_URL", "https://rebelrichie.github.io/rebel-intel")
SHEET_ID          = os.environ.get("SHEET_ID", "")
SHEETS_API_KEY    = os.environ.get("SHEETS_API_KEY", "")
PDF_PATH          = "docs/daily_brief.pdf"
TODAY             = datetime.date.today().strftime("%B %d, %Y")
TODAY_SLUG        = datetime.date.today().strftime("%Y-%m-%d")

# Hardcoded required recipients — always send here regardless of subscriber list
REQUIRED_RECIPIENTS = [
    "richie@rebeltalentsystems.com",
    "shaun@arkhamtalent.com",
]


# ─── SUBSCRIBER LIST ─────────────────────────────────────────────────────────

def get_recipients():
    """
    Build final recipient list:
    1. Always include REQUIRED_RECIPIENTS
    2. Add any Google Sheet subscribers
    3. Add any SEND_TO env var emails
    De-duplicate while preserving order.
    """
    seen   = set()
    emails = []

    def add(email):
        e = email.strip().lower()
        if e and "@" in e and e not in seen:
            seen.add(e)
            emails.append(email.strip())

    # 1. Hardcoded required
    for e in REQUIRED_RECIPIENTS:
        add(e)

    # 2. Google Sheet subscribers
    if SHEET_ID and SHEETS_API_KEY:
        try:
            import requests
            url = (f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
                   f"/values/Sheet1!A:A?key={SHEETS_API_KEY}")
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            for row in resp.json().get("values", [])[1:]:
                if row and "@" in row[0]:
                    add(row[0])
            print(f"  Google Sheet: {len(emails)} total after sheet merge")
        except Exception as e:
            print(f"  [WARN] Sheet pull failed: {e}")

    # 3. SEND_TO env var (comma-separated)
    if SEND_TO:
        for e in SEND_TO.split(","):
            add(e)

    print(f"  Final recipient list: {emails}")
    return emails


# ─── EMAIL BUILDER ───────────────────────────────────────────────────────────

def _load_data():
    try:
        with open("docs/data.json") as f:
            return json.load(f)
    except Exception:
        return {}


def build_html_body():
    data = _load_data()
    moves = data.get("moves_today", [])
    top3  = data.get("top_3", [])
    lead_profiles = data.get("lead_profiles", [])
    market_trends = data.get("market_trends", [])
    apollo_enabled = data.get("apollo_enabled", False)
    leads_with_contacts = data.get("leads_with_contacts", 0)

    # Moves section
    moves_html = "".join(
        f"<li style='margin-bottom:10px; padding:8px 12px; background:#0d1117; "
        f"border-left:2px solid #ffa500;'>"
        f"<span style='color:#ffa500; font-size:10px; letter-spacing:0.15em;'>OBJ {i+1:02d}</span><br>"
        f"<span style='color:#c0d0e0;'>{m}</span></li>"
        for i, m in enumerate(moves)
    )
    top3_html = "".join(
        f"<li style='margin-bottom:10px; padding:8px 12px; background:#0d1117; "
        f"border-left:2px solid #00ffd5;'>"
        f"<span style='color:#00ffd5; font-size:10px; letter-spacing:0.15em;'>TARGET {i+1:02d}</span><br>"
        f"<span style='color:#c0d0e0;'>{t}</span></li>"
        for i, t in enumerate(top3[:3])
    )

    # Lead profiles preview (top 3 with decision makers)
    profiles_html = ""
    for p in lead_profiles[:3]:
        dms = p.get("decision_makers", [])[:2]
        dm_html = ""
        for dm in dms:
            li_link = dm.get("linkedin_url", "")
            email   = dm.get("email", "")
            dm_html += (
                f"<div style='font-size:12px; color:#8899aa; margin-top:4px;'>"
                f"<strong style='color:#c0d0e0;'>{dm['name']}</strong> — {dm['title']}"
                + (f" | <a href='mailto:{email}' style='color:#00ffd5;'>{email}</a>" if email else "")
                + (f" | <a href='{li_link}' style='color:#7CCBC3;'>LinkedIn</a>" if li_link else "")
                + "</div>"
            )
        website = p.get("website", "")
        name_link = (f"<a href='{website}' style='color:#00ffd5; text-decoration:none;'>{p['name']}</a>"
                     if website else f"<span style='color:#00ffd5;'>{p['name']}</span>")
        emp = f" · {p['employee_count']} employees" if p.get("employee_count") else ""
        profiles_html += (
            f"<div style='background:#0d1117; border:1px solid #1a2440; border-top:2px solid #00ffd5; "
            f"padding:12px; margin-bottom:8px;'>"
            f"<div style='font-family:sans-serif;'>{name_link} "
            f"<span style='color:#4a6070; font-size:11px;'>{p.get('vertical','')}{emp}</span></div>"
            + (f"<div style='color:#8899aa; font-size:12px; margin-top:4px;'>{p['funding'].get('latest_amount','')} {p['funding'].get('stage','')}</div>" if p['funding'].get('latest_amount') else "")
            + dm_html
            + f"<div style='color:#7CCBC3; font-size:11px; margin-top:6px;'>→ {p.get('outreach_angle','')}</div>"
            f"</div>"
        )

    # Market trends
    trends_html = "".join(
        f"<li style='margin-bottom:6px; color:#8899aa; font-size:13px;'>{t}</li>"
        for t in market_trends
    )

    apollo_badge = (
        f"<span style='background:rgba(0,255,213,0.08); color:#00ffd5; border:1px solid rgba(0,255,213,0.2); "
        f"font-size:10px; padding:2px 8px; letter-spacing:0.1em;'>APOLLO ENRICHED · {leads_with_contacts} CONTACTS</span>"
        if apollo_enabled and leads_with_contacts else ""
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="background:#050d1b; margin:0; padding:32px; font-family:'JetBrains Mono',monospace;">
<div style="max-width:640px; margin:0 auto;">

  <!-- Header -->
  <div style="border-bottom:2px solid #00ffd5; padding-bottom:16px; margin-bottom:24px;">
    <div style="font-size:8px; letter-spacing:0.25em; color:#ff3366; margin-bottom:8px;">
      // INTERNAL — NOT FOR DISTRIBUTION
    </div>
    <h1 style="color:#00ffd5; font-family:sans-serif; font-size:20px; margin:0; letter-spacing:0.15em;">
      REBEL TALENT SYSTEMS
    </h1>
    <p style="color:#ffa500; font-size:11px; margin:4px 0 8px; letter-spacing:0.15em;">
      BD INTEL BRIEF — {TODAY}
    </p>
    {apollo_badge}
  </div>

  <!-- Summary stats -->
  <div style="background:#0d1117; border:1px solid #1a2440; padding:12px 16px; margin-bottom:20px;">
    <div style="font-size:8px; letter-spacing:0.2em; color:#4a6070; margin-bottom:8px;">
      INTELLIGENCE SUMMARY // {TODAY_SLUG}
    </div>
    <div style="color:#a0b4c8; font-size:13px; line-height:1.6;">
      Today's brief covers signals from multiple data sources including
      funding rounds, fintech/martech signals, SAM.gov SBIR awards,
      YC companies, HN hiring, and executive moves across Series A-C startups
      targeting 20–100 employee US companies.
    </div>
  </div>

  <!-- Moves Today -->
  <h3 style="color:#ffa500; font-size:13px; letter-spacing:0.2em; margin-bottom:8px;">
    // MISSION OBJECTIVES TODAY
  </h3>
  <ul style="list-style:none; padding:0; margin:0 0 20px;">{moves_html}</ul>

  <!-- Top 3 -->
  <h3 style="color:#00ffd5; font-size:13px; letter-spacing:0.2em; margin-bottom:8px;">
    // HIGH-VALUE TARGETS
  </h3>
  <ul style="list-style:none; padding:0; margin:0 0 20px;">{top3_html}</ul>

  <!-- Lead Profiles Preview -->
  {f'<h3 style="color:#b47cff; font-size:13px; letter-spacing:0.2em; margin-bottom:8px;">// APOLLO LEAD PROFILES (top 3)</h3>' + profiles_html if profiles_html else ''}

  <!-- Market Trends -->
  {f'<h3 style="color:#4a6070; font-size:13px; letter-spacing:0.2em; margin-bottom:8px;">// MARKET TRENDS</h3><ul style="list-style:disc; padding-left:20px; margin:0 0 20px;">{trends_html}</ul>' if trends_html else ''}

  <!-- CTA -->
  <div style="margin-top:28px; padding-top:16px; border-top:1px solid #1a2440;">
    <a href="{SITE_URL}"
       style="display:inline-block; background:#00ffd5; color:#050d1b;
              font-family:sans-serif; font-weight:bold; font-size:12px;
              letter-spacing:0.15em; padding:10px 22px; text-decoration:none;">
      → VIEW LIVE DASHBOARD
    </a>
    <span style="display:inline-block; margin-left:12px;">
      <a href="{SITE_URL}/hubspot_leads.csv"
         style="color:#4a6070; font-size:11px; text-decoration:none; letter-spacing:0.1em;">
        Download HubSpot CSV
      </a>
    </span>
  </div>

  <!-- Footer -->
  <div style="margin-top:24px; padding-top:16px; border-top:1px solid #1a2440;
              font-size:10px; color:#3a4858; letter-spacing:0.1em; line-height:1.8;">
    Rebel Talent Systems · Fractional Head of Talent · Series A-C Startups<br>
    $8K–$14K/month · 350% average ROI · EDF: 6 roles, $178K agency fees avoided<br>
    <span style="color:#4a6070;">richie@rebeltalentsystems.com · shaun@arkhamtalent.com</span>
  </div>

</div>
</body>
</html>"""


def build_plain_text():
    data = _load_data()
    moves  = data.get("moves_today", [])
    top3   = data.get("top_3", [])
    trends = data.get("market_trends", [])
    profiles = data.get("lead_profiles", [])

    lines = [
        f"REBEL TALENT SYSTEMS — BD Intel Brief — {TODAY}",
        "=" * 55,
        "",
        "MISSION OBJECTIVES TODAY:",
    ]
    for i, m in enumerate(moves, 1):
        lines.append(f"  OBJ {i:02d}: {m}")
    lines += ["", "HIGH-VALUE TARGETS:"]
    for i, t in enumerate(top3, 1):
        lines.append(f"  TARGET {i:02d}: {t}")
    if profiles:
        lines += ["", "APOLLO LEAD PROFILES:"]
        for p in profiles[:5]:
            lines.append(f"  [{p.get('vertical','')}] {p['name']} — {p['funding'].get('latest_amount','')} {p['funding'].get('stage','')}")
            for dm in p.get("decision_makers", [])[:2]:
                lines.append(f"    Contact: {dm['name']} ({dm['title']}) — {dm.get('email','no email')}")
            lines.append(f"    Angle: {p.get('outreach_angle','')}")
    if trends:
        lines += ["", "MARKET TRENDS:"]
        for t in trends:
            lines.append(f"  - {t}")
    lines += [
        "",
        f"Full brief attached. Dashboard: {SITE_URL}",
        "",
        "---",
        "Rebel Talent Systems | Fractional Head of Talent | $8K-$14K/mo",
        "richie@rebeltalentsystems.com | shaun@arkhamtalent.com",
    ]
    return "\n".join(lines)


# ─── SEND ────────────────────────────────────────────────────────────────────

def send_brief():
    print(f"\n{'='*60}")
    print(f"REBEL TALENT — Sending Daily Brief v5.0")
    print(f"  {TODAY}")
    print(f"{'='*60}\n")

    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print("  [ERROR] GMAIL_USER or GMAIL_APP_PASSWORD not set — aborting")
        return

    recipients = get_recipients()
    if not recipients:
        print("  [ERROR] No recipients found — aborting")
        return

    cc_list = [e.strip() for e in BRIEF_CC.split(",") if e.strip() and "@" in e] if BRIEF_CC else []

    msg = MIMEMultipart("mixed")
    msg["From"]    = f"Rebel Talent Intel <{GMAIL_USER}>"
    msg["To"]      = ", ".join(recipients)
    if cc_list:
        msg["Cc"]  = ", ".join(cc_list)
    msg["Subject"] = f"Rebel Talent BD Brief — {TODAY}"
    msg["Date"]    = formatdate(localtime=True)

    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(build_plain_text(), "plain"))
    alt_part.attach(MIMEText(build_html_body(),  "html"))
    msg.attach(alt_part)

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
        print(f"  [WARN] PDF not found at {PDF_PATH} — sending without attachment")

    all_recipients = recipients + cc_list
    for attempt in range(2):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
                server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
                server.sendmail(GMAIL_USER, all_recipients, msg.as_string())
            print(f"  Sent to {len(all_recipients)} recipients: {', '.join(all_recipients)}")
            return
        except Exception as e:
            print(f"  [WARN] Send failed (attempt {attempt + 1}): {e}")
            if attempt < 1:
                time.sleep(5)

    print("  [ERROR] All send attempts failed")


if __name__ == "__main__":
    send_brief()
