"""
Rebel Talent Systems — Daily BD Intel Brief v3.0
Fractional Recruiting Intelligence for Series A-C Startups

Complete rewrite: smarter filtering, better parsing, upgraded AI.
"""

import os
import json
import csv
import re
import time
import datetime
import feedparser
import requests
from groq import Groq
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML as WeasyHTML

# ─── CONFIG ──────────────────────────────────────────────────────────────────

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
TODAY = datetime.date.today().strftime("%B %d, %Y")
TODAY_SLUG = datetime.date.today().strftime("%Y-%m-%d")
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# ─── KNOWN LARGE COMPANIES (auto-reject) ─────────────────────────────────────
# These are too big for the ICP. If a headline mentions them, skip it.
LARGE_COMPANY_NAMES = {
    "zoom", "kpmg", "atlassian", "duolingo", "hitachi", "ibm", "google",
    "microsoft", "amazon", "meta", "apple", "oracle", "salesforce", "sap",
    "adobe", "cisco", "intel", "nvidia", "dell", "hp", "vmware", "paypal",
    "stripe", "shopify", "hubspot", "twilio", "datadog", "snowflake",
    "mongodb", "splunk", "servicenow", "workday", "uber", "airbnb",
    "doordash", "instacart", "robinhood", "coinbase", "block", "square",
    "twitter", "linkedin", "netflix", "spotify", "disney", "walmart",
    "target", "costco", "jpmorgan", "goldman", "morgan stanley", "booz allen",
    "leidos", "raytheon", "lockheed", "northrop", "general dynamics",
    "bae systems", "l3harris", "boeing", "deloitte", "accenture", "mckinsey",
    "pwc", "ernst & young", "ey", "bcg", "bain", "capgemini",
    "masterclass", "excel london", "lrg", "tower insurance", "fortifi",
    "aprio", "greenhouse",
}

# ─── NON-US GEO KEYWORDS (reject if found) ──────────────────────────────────
NON_US_GEOS = {
    "london", "uk startup", "european", "india", "singapore", "canada",
    "australia", "israel", "germany", "france", "japan", "china", "brazil",
    "south korea", "sweden", "netherlands", "spain", "italy", "ireland",
    "switzerland", "middle east", "dubai", "uae", "saudi", "nigeria",
    "south africa", "new zealand", "hong kong", "taiwan",
    "vietnam", "indonesia", "malaysia", "philippines", "pakistan",
    "turkey", "poland", "czech", "romania", "hungary", "portugal",
    "mumbai", "bangalore", "delhi", "chennai", "hyderabad", "pune",
    "kolkata", "shanghai", "beijing", "shenzhen", "tokyo", "osaka",
    "seoul", "berlin", "munich", "paris", "amsterdam", "stockholm",
    "copenhagen", "helsinki", "oslo", "zurich", "geneva", "vienna",
    "brussels", "milan", "madrid", "barcelona", "lisbon", "dublin",
    "edinburgh", "manchester", "birmingham", "leeds", "glasgow",
    "toronto", "vancouver", "montreal", "sydney", "melbourne",
    "tel aviv", "haifa", "sao paulo", "mexico city",
    "buenos aires", "santiago", "bogota", "lima", "nairobi", "lagos",
    "cairo", "cape town", "johannesburg",
}

# ─── PRIORITY GEOS ───────────────────────────────────────────────────────────
PRIORITY_GEOS = [
    "new york", "nyc", "brooklyn", "manhattan",
    "chicago", "illinois",
    "atlanta", "georgia",
    "michigan", "detroit", "ann arbor",
    "colorado", "denver", "boulder",
    "washington dc", "arlington", "virginia",
    "remote", "united states", "us-based",
    "san francisco", "bay area", "silicon valley",
    "austin", "texas", "boston", "seattle",
]

# ─── PRIORITY SECTORS ────────────────────────────────────────────────────────
PRIORITY_SECTORS = [
    "saas", "b2b", "enterprise software", "vertical saas",
    "govtech", "government technology", "defense tech", "national security",
    "cybersecurity", "dual-use", "space tech",
    "data infrastructure", "machine learning", "devtools", "api platform",
    "hr tech", "people ops", "recruiting", "talent",
]

# ─── RSS SOURCES ─────────────────────────────────────────────────────────────

FUNDING_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://venturebeat.com/feed/",
    "https://news.crunchbase.com/feed/",
    "https://strictlyvc.com/feed/",
    "https://executivebiz.com/feed/",
    "https://www.govexec.com/rss/technology/",
    "https://news.google.com/rss/search?q=govtech+startup+funding+OR+%22defense+tech%22+funding+OR+SBIR+award&hl=en-US&gl=US&ceid=US:en",
]

HIRING_SIGNAL_FEEDS = [
    "https://news.google.com/rss/search?q=%22head+of+talent%22+OR+%22VP+people%22+OR+%22director+of+recruiting%22+startup+hiring&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22people+operations%22+%22Series+A%22+OR+%22Series+B%22&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Series+A%22+OR+%22Series+B%22+startup+hiring+headcount+growth&hl=en-US&gl=US&ceid=US:en",
]

EXEC_MOVE_FEEDS = [
    "https://news.google.com/rss/search?q=%22appoints+Chief+People+Officer%22+OR+%22hires+VP+Talent%22+OR+%22names+Head+of+People%22+OR+%22appoints+CHRO%22&hl=en-US&gl=US&ceid=US:en",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.businesswire.com/rss/home/?rss=G1",
]

COMPETITOR_FEEDS = [
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22Bambee%22+OR+%22fractional+recruiting%22+OR+%22fractional+talent%22+OR+%22fractional+head+of+people%22&hl=en-US&gl=US&ceid=US:en",
]


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def fetch_feed(url, max_items=25):
    """Fetch and parse an RSS feed with timeout and error handling."""
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:max_items]:
            title = getattr(entry, "title", "").strip()
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
            link = getattr(entry, "link", "")
            published = getattr(entry, "published", "")

            # Strip HTML tags
            summary = re.sub(r"<[^>]+>", " ", summary)
            summary = re.sub(r"\s+", " ", summary).strip()[:500]

            items.append({
                "title": title,
                "summary": summary,
                "link": link,
                "published": published,
                "text": f"{title} {summary}".lower(),
                "source": url.split("/")[2] if "/" in url else "unknown",
            })
        return items
    except Exception as e:
        print(f"  [WARN] Feed failed ({url[:50]}...): {e}")
        return []


def is_large_company(text):
    """Check if text references a known large company."""
    t = text.lower()
    for name in LARGE_COMPANY_NAMES:
        if re.search(r'\b' + re.escape(name) + r'\b', t):
            return True
    return False


def is_non_us(text):
    """Check if text references a non-US geography."""
    t = text.lower()
    for geo in NON_US_GEOS:
        if geo in t:
            return True
    return False


def geo_score(text):
    """Score 0-2 based on priority geo matches."""
    t = text.lower()
    matches = sum(1 for g in PRIORITY_GEOS if g in t)
    return min(matches, 2)


def sector_score(text):
    """Score 0-2 based on priority sector matches."""
    t = text.lower()
    matches = sum(1 for s in PRIORITY_SECTORS if s in t)
    return min(matches, 2)


def extract_company_name(headline):
    """Extract company name from a funding headline."""
    patterns = [
        r'^(?:Exclusive:\s*)?(.+?)\s+(?:raises?|lands?|closes?|secures?|nabs?|gets?|nets?|bags?|grabs?)\s+\$',
        r'^(?:Exclusive:\s*)?(.+?)\s+(?:raises?|lands?|closes?|secures?)\s+(?:a\s+)?\$',
        r'^(.+?):\s+\$\d+',
        r'^(.+?)\s+Raises?\s+\$',
    ]
    for pattern in patterns:
        m = re.match(pattern, headline, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            name = re.sub(r'^(?:Exclusive|Breaking|Report|Update):\s*', '', name, flags=re.IGNORECASE)
            if len(name) <= 60:
                return name
    return ""


def extract_funding_amount(text):
    """Extract funding amount, only near funding-related words."""
    funding_context = r'(?:rais|fund|secur|land|close|round|invest|capital|series|seed|valuation)'
    amount_pattern = r'\$(\d+(?:\.\d+)?)\s*(million|billion|M|B|m|b)'

    for m in re.finditer(amount_pattern, text, re.IGNORECASE):
        start = max(0, m.start() - 100)
        end = min(len(text), m.end() + 100)
        context = text[start:end].lower()
        if re.search(funding_context, context):
            raw_amount = float(m.group(1))
            unit = m.group(2).lower()
            if unit in ("billion", "b"):
                return f"${raw_amount:.1f}B"
            else:
                if raw_amount >= 1000:
                    return f"${raw_amount / 1000:.1f}B"
                return f"${raw_amount:.0f}M" if raw_amount == int(raw_amount) else f"${raw_amount:.1f}M"
    return ""


def detect_stage(text, amount_str=""):
    """Detect funding stage from text content."""
    t = text.lower()

    if re.search(r'\bseries\s*c\b', t):
        return "Series C"
    if re.search(r'\bseries\s*b\b', t):
        return "Series B"
    if re.search(r'\bseries\s*a\b', t):
        return "Series A"
    if re.search(r'\bseed\s+round\b', t) or re.search(r'\bseed\s+funding\b', t):
        return "Seed"
    if re.search(r'\bpre-seed\b', t):
        return "Pre-Seed"

    if amount_str:
        try:
            num = float(re.search(r'[\d.]+', amount_str).group())
            if "B" in amount_str:
                return "Late Stage"
            elif num >= 50:
                return "Series C"
            elif num >= 15:
                return "Series B"
            elif num >= 3:
                return "Series A"
            else:
                return "Seed"
        except (ValueError, AttributeError):
            pass

    return ""


def is_funding_article(item):
    """Determine if an RSS item is about an actual funding event."""
    title = item.get("title", "").lower()

    # Strong funding signals in the headline
    funding_verbs = [
        r'\braises?\b', r'\blands?\b', r'\bcloses?\b', r'\bsecures?\b',
        r'\bnabs?\b', r'\bgets?\b.*funding', r'\bnets?\b',
        r'\bseries\s+[a-c]\b', r'\bseed\s+round\b', r'\bfunding\s+round\b',
        r'\bvaluation\b', r'\b\$\d+.*(?:million|M|billion|B)\b',
    ]

    headline_match = any(re.search(p, title) for p in funding_verbs)

    # Anti-patterns: opinion pieces, listicles, analysis, layoffs
    noise_patterns = [
        r'^how\s+', r'^why\s+', r'^what\s+', r'^will\s+',
        r'^the\s+(?:future|rise|fall|state|best|top|worst)',
        r'^from\s+hype', r'^before\s+', r'^after\s+', r'^report:',
        r'opinion', r'\bcuts?\s+staff\b', r'\blayoffs?\b',
        r'\brestructur', r'\bbankrupt', r'\bipo\b',
        r'\bpublic\s+offering\b', r'\bstock\s+price\b',
        r'\bnasdaq\b', r'\bnyse\b', r'\bfortune\s+500\b',
        r'^agents?\s+need', r'^the\s+week', r'^biggest\s+funding',
        r'how\s+vcs?\s+', r'\bfda\b', r'\bclinical\s+trial\b',
        r'\bphase\s+(?:3|iii|iv)\b',
    ]

    is_noise = any(re.search(p, title) for p in noise_patterns)

    return headline_match and not is_noise


def is_relevant_signal(item):
    """Check if a hiring/exec item is relevant to the ICP."""
    text = item.get("text", "").lower()
    if is_large_company(text):
        return False
    if is_non_us(text):
        return False
    return True


def extract_exec_info(headline):
    """Extract person name and title from exec move headlines."""
    patterns = [
        r'(?:appoints?|names?|hires?|promotes?)\s+(.+?)\s+(?:as|to)\s+(.+?)(?:\s*[-\u2013\u2014]|\s*$|\s+to\s+(?:drive|lead|steer|accelerate|strengthen))',
        r'^(.+?)\s+(?:appointed|named|hired|promoted)\s+(?:as\s+)?(.+?)(?:\s+at\s+|\s*[-\u2013\u2014])',
        r'Role\s+Call\s*\|\s*(.+?),\s*(.+?)(?:\s*[-\u2013\u2014]|\s*$)',
    ]
    for pattern in patterns:
        m = re.search(pattern, headline, re.IGNORECASE)
        if m:
            person = m.group(1).strip()[:60]
            title = m.group(2).strip()[:80]
            person = re.sub(r'\s+', ' ', person)
            title = re.sub(r'\s+', ' ', title)
            return person, title
    return "", ""


def extract_company_from_headline(headline):
    """Extract company name from exec/hiring headlines."""
    patterns = [
        r'^(.+?)\s+(?:appoints?|names?|hires?|promotes?)',
        r'at\s+(.+?)(?:\s*[-\u2013\u2014]|\s*$)',
    ]
    for pattern in patterns:
        m = re.search(pattern, headline, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Remove trailing source attributions
            name = re.sub(r'\s*[-\u2013\u2014]\s*.*$', '', name)
            if len(name) <= 50:
                return name
    return headline.split(" - ")[0].strip()[:50]


def deduplicate(items, key_field, threshold=25):
    """Remove duplicate items based on first N chars of a field."""
    seen = set()
    unique = []
    for item in items:
        key = item.get(key_field, "").lower()[:threshold]
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
        elif not key:
            unique.append(item)
    return unique


# ─── DATA PULL FUNCTIONS ─────────────────────────────────────────────────────

def pull_funding_signals():
    """Pull and filter funding signals from RSS feeds."""
    print("-> Pulling RSS funding signals...")
    results = []

    for url in FUNDING_FEEDS:
        items = fetch_feed(url, max_items=30)
        for item in items:
            if not is_funding_article(item):
                continue
            if is_large_company(item["text"]):
                continue
            if is_non_us(item["text"]):
                continue

            company = extract_company_name(item["title"])
            amount = extract_funding_amount(item["title"] + " " + item["summary"])
            stage = detect_stage(item["text"], amount)

            if not company and not amount:
                continue

            combined = item["text"]
            g_score = geo_score(combined)
            s_score = sector_score(combined)
            total_score = g_score + s_score
            if company and amount:
                total_score += 2

            results.append({
                "name": company or item["title"][:80],
                "amount": amount or "undisclosed",
                "stage": stage or "Unknown",
                "sector": "",
                "location": "",
                "headline": item["title"][:120],
                "link": item["link"],
                "source": item["source"],
                "geo_match": g_score,
                "sector_match": s_score,
                "score": total_score,
            })

    results = deduplicate(results, "name")
    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"  {len(results)} funding signals found (filtered from RSS)")
    return results[:20]


def pull_hiring_signals():
    """Pull hiring signals from RSS feeds."""
    print("-> Pulling hiring signals...")
    results = []

    for url in HIRING_SIGNAL_FEEDS:
        items = fetch_feed(url, max_items=25)
        for item in items:
            if not is_relevant_signal(item):
                continue

            text = item["text"]
            has_signal = any(kw in text for kw in [
                "head of talent", "vp people", "vp of people", "chief people",
                "chro", "director recruiting", "director of recruiting",
                "hiring", "headcount", "team growth", "scaling team",
                "talent acquisition", "fractional hr", "people ops",
                "series a", "series b", "series c", "startup hiring",
                "workforce", "recruiting", "people operations",
            ])

            if not has_signal:
                continue

            company = extract_company_from_headline(item["title"])
            results.append({
                "company": company,
                "role": item["title"][:100],
                "signal": item["summary"][:250],
                "link": item["link"],
                "source": item["source"],
            })

    results = deduplicate(results, "company")
    print(f"  {len(results)} hiring signals found")
    return results[:12]


def pull_exec_moves():
    """Pull executive moves from RSS feeds."""
    print("-> Pulling executive moves...")
    results = []

    for url in EXEC_MOVE_FEEDS:
        items = fetch_feed(url, max_items=25)
        for item in items:
            if not is_relevant_signal(item):
                continue

            text = item["text"]
            is_people_move = any(kw in text for kw in [
                "chief people", "chro", "vp talent", "vp of talent",
                "head of people", "head of talent", "director of recruiting",
                "chief human resources", "vp people", "vp of people",
                "people officer", "talent acquisition",
            ])

            if not is_people_move:
                continue

            person, title = extract_exec_info(item["title"])
            company = extract_company_from_headline(item["title"])

            results.append({
                "person": person,
                "title": title,
                "company": company,
                "headline": item["title"][:120],
                "link": item["link"],
                "source": item["source"],
            })

    results = deduplicate(results, "company")
    print(f"  {len(results)} exec moves found")
    return results[:10]


def pull_sam_gov():
    """Pull SBIR/contract awards from SAM.gov."""
    print("-> Pulling SAM.gov SBIR awards...")
    results = []
    sam_api_key = os.environ.get("SAM_GOV_API_KEY", "DEMO_KEY")
    today_str = datetime.date.today().strftime("%m/%d/%Y")
    start_str = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%m/%d/%Y")

    url = "https://api.sam.gov/opportunities/v2/search"
    params = {
        "api_key": sam_api_key,
        "postedFrom": start_str,
        "postedTo": today_str,
        "ptype": "s",
        "limit": 25,
        "offset": 0,
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            print("  [WARN] SAM.gov rate limited (DEMO_KEY)")
            return []
        if not resp.ok:
            print(f"  [WARN] SAM.gov returned {resp.status_code}")
            return []

        data = resp.json()
        for opp in data.get("opportunitiesData", []):
            title = opp.get("title", "")
            agency = opp.get("fullParentPathName", opp.get("organizationName", ""))
            description = opp.get("description", "")[:200]

            is_phase2 = "phase ii" in title.lower() or "phase 2" in title.lower()
            is_tech = any(s in (title + description).lower()
                         for s in ["software", "ai", "cyber", "data", "saas",
                                   "platform", "cloud", "machine learning"])

            if is_tech:
                results.append({
                    "title": title[:100],
                    "agency": agency[:60],
                    "type": "SBIR Phase II" if is_phase2 else "SBIR",
                    "posted": opp.get("postedDate", ""),
                    "signal": f"{agency} - {title[:80]}",
                    "why_now": "SBIR award = funded startup, hiring wave incoming.",
                    "link": f"https://sam.gov/opp/{opp.get('noticeId', '')}",
                })

        print(f"  {len(results)} SAM.gov SBIR signals found")
        return results[:8]

    except Exception as e:
        print(f"  [WARN] SAM.gov error: {e}")
        return []


def pull_competitor_intel():
    """Pull competitor intelligence."""
    print("-> Pulling competitor intel...")
    results = []
    for url in COMPETITOR_FEEDS:
        items = fetch_feed(url, max_items=15)
        for item in items:
            results.append({
                "headline": item["title"][:120],
                "link": item["link"],
            })
    return results[:6]


def pull_news_headlines():
    """Pull general news headlines for context."""
    print("-> Pulling news headlines...")
    headlines = []
    for url in FUNDING_FEEDS[:2]:
        items = fetch_feed(url, max_items=10)
        for item in items:
            if is_funding_article(item) and not is_large_company(item["text"]):
                headlines.append(item["title"])
    return headlines[:10]


# ─── GROQ SYNTHESIS ──────────────────────────────────────────────────────────

def synthesize_with_groq(funded, hiring, execs, headlines, sam_signals=None):
    """Call Groq AI to synthesize raw data into actionable intelligence."""
    print("-> Calling Groq for synthesis (llama-3.3-70b)...")

    if not GROQ_API_KEY:
        print("  [WARN] No GROQ_API_KEY - using fallback data")
        return fallback_data(funded, hiring, execs)

    context = json.dumps({
        "funded_companies": funded[:10],
        "hiring_signals": hiring[:8],
        "exec_moves": execs[:6],
        "sam_sbir_signals": (sam_signals or [])[:5],
        "news_headlines": headlines[:8],
    }, indent=2)

    system_prompt = """You are the BD Oracle for Richie Lampani at Rebel Talent Systems.

ABOUT REBEL TALENT SYSTEMS:
- Fractional Head of Talent / Director of Recruiting practice
- Richie engages as interim talent leader at Series A-C startups
- Builds recruiting infrastructure clients own permanently
- Pricing: $8K / $10.6K / $14K per month
- ICP: Series A-C startup, 20-150 employees, no dedicated talent lead, needs 5-20 hires in 12 months
- Proof point: EDF engagement - 6 roles filled, $178K agency fees avoided, 350% ROI
- Distribution: FPP (Fractional People Practitioners), GoFractional

TARGET SECTORS (priority order):
1. SaaS / B2B tech - needs full recruiting build-out post-Series A
2. Defense tech / GovTech - SBIR Phase II winners need to hire fast
3. HR Tech / People platforms - adjacent market, warm intros

TARGET GEOS:
NYC, Chicago, Atlanta, Michigan (Detroit/Ann Arbor), Colorado (Denver/Boulder), DC/Virginia, Remote-first US, SF/Bay Area, Austin, Boston, Seattle.

CRITICAL RULES - FOLLOW EXACTLY:
1. NEVER recommend outreach to public companies, Fortune 500, or companies with more than 500 employees
2. NEVER recommend companies outside the United States
3. ONLY reference companies from the DATA provided - never invent companies
4. Every action item must be specific and actionable (name the company, explain why)
5. For "why_now" - explain the specific trigger (just raised Series A, no talent lead, etc.)
6. If data is thin, say so honestly - do not pad with generic advice
7. Return ONLY valid JSON - no markdown, no code fences, no commentary

WHAT MAKES A REAL OPPORTUNITY:
- Series A or B in SaaS/B2B - hiring 10-30 people, no people function yet
- SBIR Phase II or defense tech seed - need cleared engineers + BD fast
- New CPO/CHRO at startup under 150 employees - they need infra
- GovTech company won a gov contract - headcount expansion signal
- Company posting for Head of Talent/VP People with no one in seat

Return ONLY valid JSON:
{
  "moves_today": ["string - imperative action items, 3 max"],
  "top_3": ["string - top 3 companies to chase with reasoning, 3 max"],
  "new_money": [{"company": "", "amount": "", "stage": "", "location": "", "sector_tag": "", "why_now": ""}],
  "hiring_signals": ["string - 2-4 items"],
  "exec_moves": ["string - 2-4 items"],
  "defense_signals": ["string - 1-3 items, or empty array if no defense data"],
  "vehicles": ["string - distribution channel recommendations, 1-2 items"],
  "competitive": ["string - competitive intel, 1-2 items"]
}"""

    user_prompt = f"Today is {TODAY}. Here is today's raw intelligence data:\n\n{context}\n\nSynthesize into actionable BD intelligence. Return ONLY valid JSON."

    for attempt in range(3):
        try:
            client = Groq(api_key=GROQ_API_KEY)
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                max_tokens=2500,
                temperature=0.3,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"```\s*$", "", raw)
            raw = raw.strip()
            result = json.loads(raw)
            result = post_process_groq(result)
            print("  Groq synthesis complete")
            return result

        except json.JSONDecodeError as e:
            print(f"  [WARN] Groq JSON parse error (attempt {attempt + 1}): {e}")
            if attempt < 2:
                time.sleep(2)
        except Exception as e:
            print(f"  [WARN] Groq call failed (attempt {attempt + 1}): {e}")
            if attempt < 2:
                time.sleep(3)

    print("  [WARN] All Groq attempts failed - using fallback")
    return fallback_data(funded, hiring, execs)


def post_process_groq(result):
    """Filter out any large company recommendations from Groq output."""
    for key in ["moves_today", "top_3", "hiring_signals", "exec_moves",
                "defense_signals", "vehicles", "competitive"]:
        if key in result and isinstance(result[key], list):
            result[key] = [
                item for item in result[key]
                if not is_large_company(str(item))
            ]

    if "new_money" in result and isinstance(result["new_money"], list):
        result["new_money"] = [
            item for item in result["new_money"]
            if not is_large_company(item.get("company", ""))
        ]

    return result


def fallback_data(funded, hiring, execs):
    """Structured fallback when Groq fails."""
    moves = []
    if funded:
        moves.append(f"Research {funded[0]['name']} - recently funded ({funded[0]['amount']}). Check if they have a talent function.")
    if execs:
        moves.append(f"Connect with the new people leader at {execs[0]['company']} - they likely need recruiting infrastructure.")
    moves.append("Post in GoFractional and FPP Slack about Q2 availability for Series A-B talent builds.")

    top3 = []
    for f in funded[:3]:
        top3.append(f"{f['name']} raised {f['amount']} ({f['stage']}) - likely entering active hiring phase.")

    return {
        "moves_today": moves[:3],
        "top_3": top3 or ["No high-confidence targets today - check feed connectivity."],
        "new_money": [
            {
                "company": f["name"][:50],
                "amount": f["amount"],
                "stage": f["stage"],
                "location": f.get("location", ""),
                "sector_tag": "",
                "why_now": "Recently funded - likely entering active hiring phase.",
            }
            for f in funded[:5]
        ],
        "hiring_signals": [h["role"][:120] for h in hiring[:3]] or ["No strong hiring signals today."],
        "exec_moves": [e["headline"][:120] for e in execs[:3]] or ["No relevant exec moves today."],
        "defense_signals": [],
        "vehicles": ["FPP and GoFractional remain primary distribution channels."],
        "competitive": ["Monitor fractional HR market for positioning updates."],
    }


# ─── EXPORT FUNCTIONS ────────────────────────────────────────────────────────

def stage_to_arr(stage):
    """Map funding stage to estimated ARR range."""
    mapping = {
        "Pre-Seed": "$0-500K ARR",
        "Seed": "$0-2M ARR",
        "Series A": "$1-5M ARR",
        "Series B": "$5-20M ARR",
        "Series C": "$20-50M ARR",
        "Late Stage": "$50M+ ARR",
    }
    return mapping.get(stage, "Early stage")


def build_data_json(intel, funded, hiring, execs, competitors, sam_signals=None):
    """Build data.json for the GitHub Pages dashboard."""
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
        "pdf_url": "daily_brief.pdf",
    }


def export_hubspot_csv(funded, intel):
    """Export HubSpot-ready CSV with company signals."""
    path = "docs/hubspot_leads.csv"
    rows = []

    for item in intel.get("new_money", []):
        if is_large_company(item.get("company", "")):
            continue
        rows.append({
            "Company Name": item.get("company", ""),
            "Funding Amount": item.get("amount", ""),
            "Stage": item.get("stage", ""),
            "Estimated ARR Stage": stage_to_arr(item.get("stage", "")),
            "Why Contact Now": item.get("why_now", "Recently funded."),
            "Date": TODAY,
        })

    for item in funded[:8]:
        if is_large_company(item["name"]):
            continue
        if not any(r["Company Name"] == item["name"][:50] for r in rows):
            rows.append({
                "Company Name": item["name"][:50],
                "Funding Amount": item["amount"],
                "Stage": item["stage"],
                "Estimated ARR Stage": stage_to_arr(item["stage"]),
                "Why Contact Now": "Recently funded - likely entering active hiring phase.",
                "Date": TODAY,
            })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Company Name", "Funding Amount", "Stage",
            "Estimated ARR Stage", "Why Contact Now", "Date",
        ])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  HubSpot CSV written: {path} ({len(rows)} rows)")


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


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"REBEL TALENT SYSTEMS - Daily Intel Brief v3.0")
    print(f"  {TODAY}")
    print(f"{'='*60}\n")

    os.makedirs("docs", exist_ok=True)

    funded = pull_funding_signals()
    hiring = pull_hiring_signals()
    execs = pull_exec_moves()
    competitors = pull_competitor_intel()
    headlines = pull_news_headlines()
    sam_signals = pull_sam_gov()

    intel = synthesize_with_groq(funded, hiring, execs, headlines, sam_signals)

    data = build_data_json(intel, funded, hiring, execs, competitors, sam_signals)

    json_path = "docs/data.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  data.json written: {json_path}")

    export_hubspot_csv(funded, intel)

    try:
        generate_pdf(data, intel)
    except Exception as e:
        print(f"  [WARN] PDF generation failed: {e}")

    print(f"\n{'='*60}")
    print(f"  Brief complete - {TODAY}")
    print(f"  Funded: {len(funded)} | Hiring: {len(hiring)} | Execs: {len(execs)} | SBIR: {len(sam_signals)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
