"""
Rebel Talent Systems — Daily BD Intel Brief
Fractional Recruiting Intelligence for Series A-C Startups
"""

import os
import json
import csv
import re
import datetime
import feedparser
import requests
from groq import Groq
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML as WeasyHTML

# ─── CONFIG ──────────────────────────────────────────────────────────────────

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
CRUNCHBASE_API_KEY = os.environ.get("CRUNCHBASE_API_KEY", "")
TODAY = datetime.date.today().strftime("%B %d, %Y")
TODAY_SLUG = datetime.date.today().strftime("%Y-%m-%d")
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# Target geos — used to score/filter results
PRIORITY_GEOS = [
    "new york", "nyc", "brooklyn", "manhattan",
    "chicago", "illinois",
    "atlanta", "georgia",
    "michigan", "detroit", "ann arbor",
    "colorado", "denver", "boulder",
    "remote", "united states", "us-based"
]

# Target sectors
PRIORITY_SECTORS = [
    "saas", "b2b", "software", "enterprise software", "vertical saas",
    "govtech", "government technology", "defense tech", "national security",
    "cybersecurity", "dual-use", "sbir", "dod", "federal", "space tech",
    "data infrastructure", "ai", "machine learning", "devtools", "api"
]

INCLUDE_KEYWORDS = [
    "series a", "series b", "series c", "seed round", "raises", "funding", "million", "venture",
    "head of talent", "vp people", "chief people officer", "chro", "director recruiting",
    "hiring", "headcount", "team growth", "scaling", "workforce", "talent acquisition",
    "fractional hr", "people ops", "startup hiring", "remote hiring", "people operations",
    "talent lead", "vp of talent", "head of people",
    # Defense/govtech signals
    "sbir", "govtech", "defense tech", "dual-use", "national security startup",
    "dod contract", "federal contract award", "government contract", "phase ii",
    "in-q-tel", "shield capital", "lux capital", "paladin capital", "snowpoint",
    # SaaS signals
    "saas", "b2b software", "vertical saas", "api platform", "devtools"
]

EXCLUDE_KEYWORDS = [
    "series d", "series e", "series f", "ipo", "public company", "fortune 500",
    "real estate", "biotech clinical trial", "fda approval", "patent litigation",
    "nasdaq", "nyse", "stock price", "bankruptcy", "layoffs", "restructuring",
    # Biotech/pharma unless it has a software angle
    "clinical stage", "phase 3", "phase iii", "drug approval", "therapeutic",
    # Non-US geos
    "london", "uk startup", "european", "india", "singapore", "canada",
    "australia", "israel", "germany", "france"
]

# ─── RSS SOURCES ─────────────────────────────────────────────────────────────

FUNDING_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://venturebeat.com/feed/",
    "https://news.crunchbase.com/feed/",
    "https://strictlyvc.com/feed/",
    # Defense/GovTech specific
    "https://executivebiz.com/feed/",
    "https://www.govexec.com/rss/technology/",
    "https://news.google.com/rss/search?q=govtech+startup+funding+OR+%22defense+tech%22+funding+OR+SBIR+award&hl=en-US&gl=US&ceid=US:en",
]

HIRING_SIGNAL_FEEDS = [
    # Priority geos only
    "https://www.builtinnyc.com/jobs/feed",
    "https://www.builtinchicago.org/jobs/feed",
    "https://www.builtinatlanta.com/jobs/feed",
    "https://www.builtincolorado.com/jobs/feed",
    # Keyword-targeted Google News
    "https://news.google.com/rss/search?q=%22head+of+talent%22+OR+%22VP+people%22+OR+%22director+of+recruiting%22+startup&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22people+operations%22+%22Series+A%22+OR+%22Series+B%22&hl=en-US&gl=US&ceid=US:en",
]

EXEC_MOVE_FEEDS = [
    "https://news.google.com/rss/search?q=%22appoints+Chief+People+Officer%22+OR+%22hires+VP+Talent%22+OR+%22names+Head+of+People%22+OR+%22appoints+CHRO%22&hl=en-US&gl=US&ceid=US:en",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.businesswire.com/rss/home/?rss=G1",
]

COMPETITOR_FEEDS = [
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22Leap+HR%22+OR+%22Bambee%22+OR+%22fractional+recruiting%22+OR+%22fractional+talent%22&hl=en-US&gl=US&ceid=US:en",
]

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def fetch_feed(url, max_items=20):
    """Fetch and parse an RSS feed, return list of entry dicts."""
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:max_items]:
            title = getattr(entry, "title", "")
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
            link = getattr(entry, "link", "")
            published = getattr(entry, "published", "")
            # strip html tags from summary
            summary = re.sub(r"<[^>]+>", " ", summary)
            summary = re.sub(r"\s+", " ", summary).strip()[:400]
            items.append({
                "title": title.strip(),
                "summary": summary,
                "link": link,
                "published": published,
                "text": f"{title} {summary}".lower()
            })
        return items
    except Exception as e:
        print(f"  [WARN] Feed failed ({url[:60]}...): {e}")
        return []


def passes_filter(item):
    """Return True if item is relevant, False if noise."""
    text = item.get("text", "").lower()
    if any(kw in text for kw in EXCLUDE_KEYWORDS):
        return False
    if any(kw in text for kw in INCLUDE_KEYWORDS):
        return True
    return False


def geo_score(text):
    """Return 1 if text matches a priority geo, 0 otherwise."""
    t = text.lower()
    return 1 if any(g in t for g in PRIORITY_GEOS) else 0


def sector_score(text):
    """Return 1 if text matches a priority sector."""
    t = text.lower()
    return 1 if any(s in t for s in PRIORITY_SECTORS) else 0


def pull_crunchbase_funding():
    """Pull recent Series A-C rounds from Crunchbase API (paid)."""
    print("→ Pulling Crunchbase funding (API)...")
    if not CRUNCHBASE_API_KEY:
        print("  [WARN] No CRUNCHBASE_API_KEY — skipping Crunchbase API")
        return []

    results = []
    url = "https://api.crunchbase.com/api/v4/searches/funding_rounds"
    headers = {"X-cb-user-key": CRUNCHBASE_API_KEY, "Content-Type": "application/json"}

    payload = {
        "field_ids": [
            "identifier", "announced_on", "money_raised", "investment_type",
            "funded_organization_identifier", "funded_organization_location",
            "funded_organization_short_description", "funded_organization_num_employees_enum"
        ],
        "query": [
            {"type": "predicate", "field_id": "announced_on",
             "operator_id": "gte", "values": [YESTERDAY]},
            {"type": "predicate", "field_id": "announced_on",
             "operator_id": "lte", "values": [datetime.date.today().strftime("%Y-%m-%d")]},
            {"type": "predicate", "field_id": "investment_type",
             "operator_id": "includes",
             "values": ["angel", "seed", "series_a", "series_b", "series_c"]},
        ],
        "sort": [{"field_id": "announced_on", "sort_value": "desc"}],
        "limit": 50
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        entities = resp.json().get("entities", [])
        for e in entities:
            p = e.get("properties", {})
            org = p.get("funded_organization_identifier", {})
            loc = p.get("funded_organization_location", [{}])
            loc_str = ", ".join(l.get("value", "") for l in loc) if isinstance(loc, list) else ""
            amount_obj = p.get("money_raised", {})
            amount = f"${amount_obj.get('value_usd', 0) / 1_000_000:.1f}M" if amount_obj else "undisclosed"
            stage_raw = p.get("investment_type", "seed")
            stage = stage_raw.replace("_", " ").title().replace("Series ", "Series ")
            description = p.get("funded_organization_short_description", "")
            emp = p.get("funded_organization_num_employees_enum", "")

            # Score geo and sector relevance
            combined = f"{loc_str} {description} {org.get('value', '')}".lower()
            geo = geo_score(combined)
            sec = sector_score(combined)

            # Skip if employee count signals too large (c_10001_plus, etc.)
            if emp and "10001" in emp:
                continue

            results.append({
                "name": org.get("value", "Unknown")[:80],
                "amount": amount,
                "stage": stage,
                "sector": description[:100],
                "location": loc_str,
                "headline": f"{org.get('value', '')} raised {amount} ({stage})",
                "link": f"https://crunchbase.com/organization/{org.get('permalink', '')}",
                "geo_match": geo,
                "sector_match": sec,
                "score": geo + sec
            })

        # Sort by score (geo + sector matches first)
        results.sort(key=lambda x: x["score"], reverse=True)
        print(f"  {len(results)} Crunchbase rounds found ({sum(1 for r in results if r['geo_match'])} geo matches)")
        return results[:20]

    except Exception as e:
        print(f"  [WARN] Crunchbase API error: {e}")
        return []


def pull_sam_gov():
    """Pull recent SBIR/contract awards from SAM.gov — defense/govtech signal."""
    print("→ Pulling SAM.gov SBIR awards...")
    results = []
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    start_str = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

    # SBIR awards endpoint — requires both start AND end date
    url = "https://api.sam.gov/opportunities/v2/search"
    params = {
        "api_key": "DEMO_KEY",  # Replace with SAM_GOV_API_KEY secret if rate limiting
        "postedFrom": start_str,
        "postedTo": today_str,
        "ptype": "s",  # SBIR
        "limit": 25,
        "offset": 0,
        "status": "active",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            print("  [WARN] SAM.gov rate limited — using DEMO_KEY, add SAM_GOV_API_KEY secret")
            return []
        if not resp.ok:
            print(f"  [WARN] SAM.gov returned {resp.status_code}")
            return []

        data = resp.json()
        for opp in data.get("opportunitiesData", []):
            title = opp.get("title", "")
            agency = opp.get("fullParentPathName", opp.get("organizationName", ""))
            naics = opp.get("naicsCode", "")
            description = opp.get("description", "")[:200]
            posted = opp.get("postedDate", "")
            sol_number = opp.get("solicitationNumber", "")

            # Only surface things that signal a startup hiring wave:
            # SBIR Phase II = company already has traction + federal $$ coming
            is_phase2 = "phase ii" in title.lower() or "phase 2" in title.lower()
            is_tech = any(s in (title + description).lower() for s in ["software", "ai", "cyber", "data", "saas", "platform", "cloud"])

            if is_tech:
                results.append({
                    "title": title[:100],
                    "agency": agency[:60],
                    "type": "SBIR Phase II" if is_phase2 else "SBIR",
                    "posted": posted,
                    "signal": f"{agency} SBIR — {title[:80]}",
                    "why_now": "SBIR award incoming = funded startup, will need to hire fast.",
                    "link": f"https://sam.gov/opp/{opp.get('noticeId', '')}"
                })

        print(f"  {len(results)} SAM.gov SBIR signals found")
        return results[:8]

    except Exception as e:
        print(f"  [WARN] SAM.gov error: {e}")
        return []


def pull_funding_signals():
    """Pull from Crunchbase API first, fall back to RSS."""
    # Try Crunchbase API
    cb_results = pull_crunchbase_funding()

    # Supplement with RSS (always run — catches news Crunchbase misses)
    print("→ Pulling RSS funding signals...")
    rss_results = []
    for url in FUNDING_FEEDS:
        items = fetch_feed(url, max_items=30)
        for item in items:
            if passes_filter(item):
                amount_match = re.search(r'\$(\d+(?:\.\d+)?)\s*(million|billion|M|B)', item["title"] + item["summary"], re.IGNORECASE)
                amount = amount_match.group(0) if amount_match else "undisclosed"
                stage = "seed"
                for s in ["series c", "series b", "series a"]:
                    if s in item["text"]:
                        stage = s.replace("series ", "Series ").strip()
                        break
                combined = item["text"]
                rss_results.append({
                    "name": item["title"][:80],
                    "amount": amount,
                    "stage": stage,
                    "sector": "",
                    "location": "",
                    "headline": item["title"][:120],
                    "link": item["link"],
                    "geo_match": geo_score(combined),
                    "sector_match": sector_score(combined),
                    "score": geo_score(combined) + sector_score(combined)
                })

    rss_results.sort(key=lambda x: x["score"], reverse=True)

    # Merge: CB first (higher quality), RSS fills gaps
    combined = cb_results + [r for r in rss_results if not any(r["name"][:20] in c["name"] for c in cb_results)]
    print(f"  Total funding signals: {len(combined)} ({len(cb_results)} from API, {len(rss_results)} from RSS)")
    return combined[:20]


def pull_hiring_signals():
    print("→ Pulling hiring signals...")
    results = []
    for url in HIRING_SIGNAL_FEEDS:
        items = fetch_feed(url, max_items=30)
        for item in items:
            if passes_filter(item):
                results.append({
                    "company": item["title"][:80],
                    "role": item["title"][:80],
                    "signal": item["summary"][:200],
                    "link": item["link"]
                })
    print(f"  {len(results)} hiring signals found")
    return results[:10]


def pull_exec_moves():
    print("→ Pulling executive moves...")
    results = []
    for url in EXEC_MOVE_FEEDS:
        items = fetch_feed(url, max_items=20)
        for item in items:
            if passes_filter(item) or any(kw in item["text"] for kw in ["chief people", "chro", "vp talent", "head of people"]):
                results.append({
                    "person": "",
                    "title": "",
                    "company": item["title"][:80],
                    "headline": item["title"][:120],
                    "link": item["link"]
                })
    print(f"  {len(results)} exec moves found")
    return results[:8]


def pull_competitor_intel():
    print("→ Pulling competitor intel...")
    results = []
    for url in COMPETITOR_FEEDS:
        items = fetch_feed(url, max_items=15)
        for item in items:
            results.append({
                "headline": item["title"][:120],
                "link": item["link"]
            })
    return results[:6]


def pull_news_headlines():
    print("→ Pulling news headlines...")
    headlines = []
    for url in FUNDING_FEEDS[:2]:
        items = fetch_feed(url, max_items=10)
        headlines += [item["title"] for item in items if passes_filter(item)]
    return headlines[:10]


# ─── GROQ SYNTHESIS ──────────────────────────────────────────────────────────

def synthesize_with_groq(funded, hiring, execs, headlines, sam_signals=None):
    print("→ Calling Groq for synthesis...")

    if not GROQ_API_KEY:
        print("  [WARN] No GROQ_API_KEY — using fallback data")
        return fallback_data(funded, hiring, execs)

    context = json.dumps({
        "funded_companies": funded[:8],
        "hiring_signals": hiring[:6],
        "exec_moves": execs[:5],
        "sam_sbir_signals": (sam_signals or [])[:5],
        "news_headlines": headlines[:8]
    }, indent=2)

    system_prompt = """You are the BD Oracle for Richie Lampani at Rebel Talent Systems, a fractional Head of Talent and Recruiting practice.
You think like a senior talent exec who has built recruiting functions at 30+ Series A-C startups.

ABOUT THIS PRACTICE:
Rebel Talent Systems offers fractional recruiting leadership. Richie engages as interim Head of Talent or Director of Recruiting.
Builds recruiting infrastructure clients own permanently.
Pricing: $8K / $10.6K / $14K per month.
Ideal client: Series A-C startup, 20-150 employees, no dedicated talent lead, needs to hire 5-20 people in next 12 months.
Proof point: EDF engagement — 6 roles filled, $178K in agency fees avoided, 350% ROI.
Distribution: FPP (Fractional People Practitioners), GoFractional.

TARGET SECTORS:
1. SaaS / B2B tech — classic ICP, needs full recruiting build-out post-Series A
2. Defense tech / GovTech — fast-growing niche; SBIR Phase II winners are especially hot because
   they just got federal validation + capital and need to hire engineers + business ops fast.
   Key investors to watch: In-Q-Tel, Shield Capital, Lux Capital, Paladin Capital, DCVC, a16z (national security).

TARGET GEOS (priority order):
NYC / Northeast, Chicago, Atlanta, Michigan (Detroit/Ann Arbor), Colorado (Denver/Boulder), Remote-first US.
Weight geo-matched companies higher in your recommendations.

WHAT TRIGGERS A REAL OPPORTUNITY:
- Series A or B in SaaS/B2B — hiring 10-30 people in 90 days, no people function yet
- SBIR Phase II award or defense tech seed round — need to hire cleared engineers + BD fast
- New CPO/CHRO at a startup under 150 employees — they need infra, not just a recruiter
- GovTech company just announced a gov contract — headcount expansion signal
- Company in priority geo posting for Head of Talent/VP People with no one in seat

WHAT TO IGNORE: Series D+, public companies, enterprise, biotech clinical, >500 employees, non-US.

CRITICAL RULES:
- Only reference companies and facts from the DATA provided. Never invent.
- Every string must be a complete sentence of 10-30 words. Never output just a name or number.
- Use imperative verbs for moves_today (e.g. "Reach out to...", "Connect with...", "Check LinkedIn for...").
- For defense/govtech signals, note if they are SBIR Phase II or have known defense investors.
- Return ONLY valid JSON. No markdown. No code fences. No preamble.

Return ONLY valid JSON with exactly these keys:
{
  "moves_today": ["string 1", "string 2", "string 3"],
  "top_3": ["string 1", "string 2", "string 3"],
  "new_money": [{"company": "", "amount": "", "stage": "", "location": "", "sector_tag": "", "why_now": ""}],
  "hiring_signals": ["string 1", "string 2"],
  "exec_moves": ["string 1", "string 2"],
  "defense_signals": ["string 1", "string 2"],
  "vehicles": ["string 1", "string 2"],
  "competitive": ["string 1", "string 2"]
}"""

    user_prompt = f"Today is {TODAY}. Here is today's raw intelligence data:\n\n{context}\n\nSynthesize this into actionable BD intelligence for Richie. Return ONLY valid JSON."

    try:
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            max_tokens=2000,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"```\s*$", "", raw)
        raw = raw.strip()
        result = json.loads(raw)
        print("  Groq synthesis complete")
        return result
    except json.JSONDecodeError as e:
        print(f"  [WARN] Groq JSON parse error: {e}")
        return fallback_data(funded, hiring, execs)
    except Exception as e:
        print(f"  [WARN] Groq call failed: {e}")
        return fallback_data(funded, hiring, execs)


def fallback_data(funded, hiring, execs):
    """Return structured fallback when Groq fails."""
    moves = [
        "Search LinkedIn for founders at recently-funded Series A SaaS and defense tech startups with no talent function.",
        "Post in GoFractional Slack about availability for Q2 talent builds at seed and Series A companies.",
        "Review FPP community board for warm intro opportunities to funded startups needing recruiting infrastructure."
    ]
    top3 = ["No data available — check RSS feed connectivity.", "Verify GROQ_API_KEY is set in GitHub Secrets.", "Re-run script after confirming feed URLs are live."]
    return {
        "moves_today": moves,
        "top_3": top3,
        "new_money": [{"company": f["name"][:50], "amount": f["amount"], "stage": f["stage"], "location": f.get("location", ""), "sector_tag": "", "why_now": "Recently funded — likely entering a hiring phase."} for f in funded[:3]],
        "hiring_signals": ["Hiring signal data pulled — review raw feed output for details.", "Check built-in city feeds for active talent/people roles."],
        "exec_moves": ["Executive move data pulled — review for new CPO/CHRO appointments.", "New people exec appointments signal incoming hiring wave."],
        "defense_signals": ["SAM.gov SBIR data pulled — review for Phase II defense tech awards.", "Defense tech SBIR Phase II winners are prime fractional talent targets."],
        "vehicles": ["FPP and GoFractional remain primary warm distribution channels.", "In-Q-Tel and Shield Capital portfolio pages are strong signals for defense tech hiring."],
        "competitive": ["Monitor fractional HR market for positioning updates.", "Track Bambee and Leap HR announcements for competitive intelligence."]
    }


# ─── EXPORT FUNCTIONS ─────────────────────────────────────────────────────────

def build_data_json(intel, funded, hiring, execs, competitors, sam_signals=None):
    """Build the data.json consumed by the GitHub Pages dashboard."""
    return {
        "generated": TODAY,
        "generated_slug": TODAY_SLUG,
        "moves_today": intel.get("moves_today", []),
        "top_3": intel.get("top_3", []),
        "new_money": intel.get("new_money", []),
        "hiring_signals": intel.get("hiring_signals", []),
        "exec_moves": intel.get("exec_moves", []),
        "defense_signals": intel.get("defense_signals", []),
        "vehicles": intel.get("vehicles", []),
        "competitive": intel.get("competitive", []),
        "raw_funded": funded,
        "raw_hiring": hiring,
        "raw_execs": execs,
        "raw_competitors": competitors,
        "raw_sam": sam_signals or [],
        "pdf_url": "daily_brief.pdf"
    }


def export_hubspot_csv(funded, intel):
    """Export HubSpot-ready CSV with company signals."""
    path = "docs/hubspot_leads.csv"
    rows = []
    # From Groq-synthesized new_money
    for item in intel.get("new_money", []):
        rows.append({
            "Company Name": item.get("company", ""),
            "Funding Amount": item.get("amount", ""),
            "Stage": item.get("stage", ""),
            "Estimated ARR Stage": stage_to_arr(item.get("stage", "")),
            "Why Contact Now": item.get("why_now", ""),
            "Date": TODAY
        })
    # From raw funded fallback
    for item in funded[:5]:
        if not any(r["Company Name"] == item["name"][:50] for r in rows):
            rows.append({
                "Company Name": item["name"][:50],
                "Funding Amount": item["amount"],
                "Stage": item["stage"],
                "Estimated ARR Stage": stage_to_arr(item["stage"]),
                "Why Contact Now": "Recently funded startup — likely in active hiring mode.",
                "Date": TODAY
            })
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Company Name", "Funding Amount", "Stage", "Estimated ARR Stage", "Why Contact Now", "Date"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  HubSpot CSV written: {path} ({len(rows)} rows)")


def stage_to_arr(stage):
    mapping = {
        "seed": "$0-2M ARR",
        "Series A": "$1-5M ARR",
        "Series B": "$5-20M ARR",
        "Series C": "$20-50M ARR",
    }
    return mapping.get(stage, "Pre-revenue / early stage")


def generate_pdf(data, intel):
    """Render Jinja2 template and generate PDF via WeasyPrint."""
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("report.html")
    html_content = template.render(
        today=TODAY,
        data=data,
        intel=intel,
    )
    out_path = "docs/daily_brief.pdf"
    WeasyHTML(string=html_content, base_url=".").write_pdf(out_path)
    print(f"  PDF written: {out_path}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"REBEL TALENT SYSTEMS — Daily Intel Brief")
    print(f"  {TODAY}")
    print(f"{'='*60}\n")

    os.makedirs("docs", exist_ok=True)

    # Pull data
    funded = pull_funding_signals()
    hiring = pull_hiring_signals()
    execs = pull_exec_moves()
    competitors = pull_competitor_intel()
    headlines = pull_news_headlines()
    sam_signals = pull_sam_gov()

    # Synthesize with Groq
    intel = synthesize_with_groq(funded, hiring, execs, headlines, sam_signals)

    # Build data structures
    data = build_data_json(intel, funded, hiring, execs, competitors, sam_signals)

    # Write data.json (MUST happen before workflow commits)
    json_path = "docs/data.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  data.json written: {json_path}")

    # Export HubSpot CSV
    export_hubspot_csv(funded, intel)

    # Generate PDF
    try:
        generate_pdf(data, intel)
    except Exception as e:
        print(f"  [WARN] PDF generation failed: {e}")

    print(f"\n✓ Brief complete — {TODAY}")


if __name__ == "__main__":
    main()
