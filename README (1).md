# Rebel Talent Systems — Daily BD Intel Platform

Automated fractional recruiting intelligence for Series A–C startup outreach.  
Runs Mon–Fri at 7am ET. Emails a PDF brief. Deploys a live dashboard.

---

## What It Does

- Pulls RSS from TechCrunch, VentureBeat, Crunchbase, Built In, Google News
- Filters for Series A–C funding, hiring signals, executive moves
- Synthesizes with Groq AI into actionable BD intelligence
- Generates a PDF brief via WeasyPrint
- Exports HubSpot-ready CSV with funding signals
- Emails the brief to your subscriber list
- Deploys a live filterable dashboard to GitHub Pages

---

## Setup — 5 Steps

### 1. Fork or clone this repo

```bash
git clone https://github.com/yourusername/rebel-talent-intel.git
cd rebel-talent-intel
```

### 2. Enable GitHub Pages

GitHub repo → Settings → Pages → Source: `gh-pages` branch → `/root`

### 3. Set GitHub Secrets

Go to: **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Where to get it |
|---|---|
| `GROQ_API_KEY` | [console.groq.com](https://console.groq.com) — free tier works |
| `CRUNCHBASE_API_KEY` | [crunchbase.com/api](https://data.crunchbase.com/docs) — paid account required |
| `GMAIL_USER` | Your Gmail address |
| `GMAIL_APP_PASSWORD` | Google Account → Security → 2FA → App Passwords |
| `SEND_TO` | Comma-separated email list (fallback recipients) |
| `BRIEF_CC` | Optional CC addresses |
| `SITE_URL` | `https://yourusername.github.io/rebel-talent-intel` |
| `SHEET_ID` | Optional: Google Sheet ID for subscriber list |
| `SHEETS_API_KEY` | Optional: Google Cloud API key with Sheets API |
| `SAM_GOV_API_KEY` | Optional: [sam.gov/api](https://open.gsa.gov/api/sam-entity-extracts-api/) — removes rate limits |

### 4. Set up Google Apps Script (Subscribe button)

1. Go to [script.google.com](https://script.google.com) → New project
2. Paste this code:

```javascript
function doPost(e) {
  const data = JSON.parse(e.postData.contents);
  const sheet = SpreadsheetApp.openById('YOUR_SHEET_ID').getSheets()[0];
  sheet.appendRow([data.email, data.name, data.ts, data.source]);
  return ContentService.createTextOutput('OK');
}
```

3. Deploy → New deployment → Web app → Execute as: Me → Access: Anyone
4. Copy the deployment URL into `docs/index.html` at `const WEBHOOK_URL = '...'`

### 5. Trigger first run

GitHub → Actions → **Rebel Talent — Daily BD Intel Brief** → Run workflow

---

## Local Development

```bash
pip install weasyprint jinja2 requests feedparser groq pandas
export GROQ_API_KEY="your-key-here"
python generate_brief.py
```

PDF writes to `docs/daily_brief.pdf`. Dashboard reads `docs/data.json`.

---

## Customization

All business logic lives in `generate_brief.py`:

- **`INCLUDE_KEYWORDS` / `EXCLUDE_KEYWORDS`** — tune signal filtering
- **`FUNDING_FEEDS`** — add/remove RSS sources
- **Groq system prompt** — update pricing, case study, distribution channels
- **`stage_to_arr()`** — map funding stage to ARR estimate

PDF template: `templates/report.html`  
Dashboard: `docs/index.html`

---

## Architecture

```
rebel-talent-intel/
├── .github/workflows/daily-intel.yml   ← runs Mon–Fri 7am ET
├── docs/
│   ├── index.html                      ← GitHub Pages dashboard
│   ├── data.json                       ← generated daily
│   ├── daily_brief.pdf                 ← generated daily
│   └── hubspot_leads.csv               ← generated daily
├── templates/
│   └── report.html                     ← Jinja2 PDF template
├── generate_brief.py                   ← main script
├── send_brief.py                       ← email sender
└── README.md
```

---

*Built on the EDF Oracle architecture. Same stack. Different target market.*  
*Rebel Talent Systems — Richie Lampani — Fractional Head of Talent*
