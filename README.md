# Rebel Talent Systems — Daily BD Intel Platform v3.0

Automated fractional recruiting intelligence for Series A-C startup outreach.
Runs Mon-Fri at 7am ET. Emails a PDF brief. Deploys a live dashboard.

---

## What It Does

- Pulls RSS from TechCrunch, VentureBeat, Crunchbase News, StrictlyVC, ExecutiveBiz, GovExec, Google News
- Smart filtering: rejects large companies, non-US, opinion pieces, layoff news
- Extracts company names, funding amounts, and stages from headlines
- Synthesizes with Groq AI (llama-3.3-70b) into actionable BD intelligence
- Generates a branded PDF brief via WeasyPrint
- Exports HubSpot-ready CSV with funding signals
- Emails the brief to your subscriber list
- Deploys a live filterable dashboard to GitHub Pages

---

## Setup

### 1. Fork or clone this repo

```bash
git clone https://github.com/rebelrichie/rebel-intel.git
cd rebel-intel
```

### 2. Enable GitHub Pages

GitHub repo > Settings > Pages > Source: `gh-pages` branch > `/root`

### 3. Set GitHub Secrets

Go to: **Settings > Secrets and variables > Actions > New repository secret**

| Secret | Where to get it |
|---|---|
| `GROQ_API_KEY` | [console.groq.com](https://console.groq.com) - free tier works |
| `GMAIL_USER` | Your Gmail address |
| `GMAIL_APP_PASSWORD` | Google Account > Security > 2FA > App Passwords |
| `SEND_TO` | Comma-separated email list (fallback recipients) |
| `BRIEF_CC` | Optional CC addresses |
| `SITE_URL` | `https://rebelrichie.github.io/rebel-intel` |
| `SHEET_ID` | Optional: Google Sheet ID for subscriber list |
| `SHEETS_API_KEY` | Optional: Google Cloud API key with Sheets API |
| `SAM_GOV_API_KEY` | Optional: [sam.gov/api](https://open.gsa.gov/api/sam-entity-extracts-api/) - removes rate limits |

### 4. Trigger first run

GitHub > Actions > **Rebel Talent - Daily BD Intel Brief** > Run workflow

---

## Local Development

```bash
pip install -r requirements.txt
export GROQ_API_KEY="your-key-here"
python generate_brief.py
```

PDF writes to `docs/daily_brief.pdf`. Dashboard reads `docs/data.json`.

---

## Customization

All business logic lives in `generate_brief.py`:

- **`LARGE_COMPANY_NAMES`** - companies to auto-reject (too big for ICP)
- **`NON_US_GEOS`** - non-US geographies to filter out
- **`PRIORITY_GEOS` / `PRIORITY_SECTORS`** - boost relevance scoring
- **`FUNDING_FEEDS`** - add/remove RSS sources
- **Groq system prompt** - update pricing, case study, distribution channels
- **`stage_to_arr()`** - map funding stage to ARR estimate

PDF template: `templates/report.html`
Dashboard: `docs/index.html`

---

## Architecture

```
rebel-intel/
├── .github/workflows/daily-intel.yml   <- runs Mon-Fri 7am ET
├── docs/
│   ├── index.html                      <- GitHub Pages dashboard
│   ├── data.json                       <- generated daily
│   ├── daily_brief.pdf                 <- generated daily
│   └── hubspot_leads.csv               <- generated daily
├── templates/
│   └── report.html                     <- Jinja2 PDF template
├── generate_brief.py                   <- main engine (v3.0)
├── send_brief.py                       <- email sender
├── requirements.txt                    <- Python dependencies
└── README.md
```

---

*Rebel Talent Systems - Richie Lampani - Fractional Head of Talent*
