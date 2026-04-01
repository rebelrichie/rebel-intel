"""
Rebel Talent Systems — Daily BD Intel Brief v5.0
Fractional Recruiting Intelligence for Series A-C Startups

v5.0: Apollo API integration for decision maker contact data,
fintech/martech/defense verticals, LinkedIn job signals,
deep per-lead profiles with full funding history + tech stack,
hiring surge detection, Sales/Tech exec tracking.
"""

import os
import sys
import json
import csv
import re
import time
import datetime
import feedparser
import requests

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
from groq import Groq
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML as WeasyHTML

# ─── CONFIG ──────────────────────────────────────────────────────────────────

GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
APOLLO_API_KEY  = os.environ.get("APOLLO_API_KEY", "")
APOLLO_BASE     = "https://api.apollo.io/v1"
TODAY           = datetime.date.today().strftime("%B %d, %Y")
TODAY_SLUG      = datetime.date.today().strftime("%Y-%m-%d")
YESTERDAY       = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# ─── KNOWN LARGE COMPANIES (auto-reject) ─────────────────────────────────────
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
    "aprio", "greenhouse", "plaid", "brex", "rippling", "gusto",
    "adp", "paychex", "bamboohr", "lattice", "culture amp", "deel",
}

# Consulting/agency signals — companies with no product (not ICP)
CONSULTING_SIGNALS = [
    "consulting group", "consulting llc", "consulting inc", "advisory group",
    "staffing solutions", "talent agency", "recruiting agency", "staffing inc",
    "managed services", "professional services firm", "outsourcing",
    "contract staffing", "temp agency", "hr consulting", "staffing firm",
]

# ─── NON-US GEO KEYWORDS (reject) ────────────────────────────────────────────
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

# ─── PRIORITY SECTORS (v5.0 — fintech + martech + defense) ───────────────────
PRIORITY_SECTORS = [
    # SaaS / B2B core
    "saas", "b2b", "enterprise software", "vertical saas",
    # Defense / cleared / gov
    "govtech", "government technology", "defense tech", "national security",
    "cybersecurity", "dual-use", "space tech", "cleared", "federal",
    "government contract", "defense contractor", "dod",
    # Fintech (v5.0)
    "fintech", "financial technology", "payments", "insurtech", "lendtech",
    "regtech", "banking tech", "wealth tech", "wealthtech", "embedded finance",
    "neobank", "crypto infrastructure",
    # Martech (v5.0)
    "martech", "marketing technology", "adtech", "ad tech",
    "sales enablement", "revenue intelligence", "go-to-market", "gtm",
    "customer data platform", "cdp", "marketing automation",
    # Data / AI / Dev
    "data infrastructure", "machine learning", "devtools", "api platform",
    "ai", "artificial intelligence", "mlops", "data pipeline",
    # HR-adjacent
    "hr tech", "people ops", "recruiting", "talent", "workforce",
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

FINTECH_FEEDS = [
    "https://news.google.com/rss/search?q=fintech+startup+%22series+a%22+OR+%22series+b%22+OR+%22series+c%22+funding+million&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=fintech+%22raised%22+OR+%22secures%22+%22million%22+startup&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=insurtech+OR+wealthtech+OR+regtech+startup+funding+%22series+a%22+OR+%22series+b%22&hl=en-US&gl=US&ceid=US:en",
]

MARTECH_FEEDS = [
    "https://news.google.com/rss/search?q=martech+startup+%22series+a%22+OR+%22series+b%22+funding+million&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22marketing+technology%22+OR+%22sales+enablement%22+startup+raises+million&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22revenue+intelligence%22+OR+%22go-to-market%22+startup+funding+%22series+a%22&hl=en-US&gl=US&ceid=US:en",
]

HIRING_SIGNAL_FEEDS = [
    "https://news.google.com/rss/search?q=%22head+of+talent%22+OR+%22VP+people%22+OR+%22director+of+recruiting%22+startup+hiring&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22people+operations%22+%22Series+A%22+OR+%22Series+B%22&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Series+A%22+OR+%22Series+B%22+startup+hiring+headcount+growth&hl=en-US&gl=US&ceid=US:en",
    # v5.0: Sales + Tech hiring signals
    "https://news.google.com/rss/search?q=startup+%22VP+Sales%22+OR+%22CTO%22+OR+%22VP+Engineering%22+hiring+%22Series+A%22+OR+%22Series+B%22&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=startup+%22head+of+sales%22+OR+%22vp+of+engineering%22+%22series+a%22+hiring&hl=en-US&gl=US&ceid=US:en",
]

EXEC_MOVE_FEEDS = [
    # People/HR function (original)
    "https://news.google.com/rss/search?q=%22appoints+Chief+People+Officer%22+OR+%22hires+VP+Talent%22+OR+%22names+Head+of+People%22+OR+%22appoints+CHRO%22&hl=en-US&gl=US&ceid=US:en",
    # Sales leaders (v5.0)
    "https://news.google.com/rss/search?q=%22appoints+VP+Sales%22+OR+%22hires+VP+of+Sales%22+OR+%22names+Chief+Revenue+Officer%22+OR+%22appoints+CRO%22+startup&hl=en-US&gl=US&ceid=US:en",
    # Tech leaders (v5.0)
    "https://news.google.com/rss/search?q=%22appoints+CTO%22+OR+%22hires+VP+Engineering%22+OR+%22names+Chief+Technology+Officer%22+startup&hl=en-US&gl=US&ceid=US:en",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.businesswire.com/rss/home/?rss=G1",
]

COMPETITOR_FEEDS = [
    "https://news.google.com/rss/search?q=%22fractional+HR%22+OR+%22Bambee%22+OR+%22fractional+recruiting%22+OR+%22fractional+talent%22+OR+%22fractional+head+of+people%22&hl=en-US&gl=US&ceid=US:en",
]

GOV_CONTRACT_FEEDS = [
    "https://news.google.com/rss/search?q=%22government+contract%22+OR+%22federal+contract%22+startup+%22million%22+defense+OR+cybersecurity&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=SBIR+OR+STTR+%22phase+ii%22+startup+award+technology&hl=en-US&gl=US&ceid=US:en",
]


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def fetch_feed(url, max_items=25):
    """Fetch and parse an RSS feed with strict timeout."""
    try:
        resp = requests.get(url, timeout=12, headers={
            "User-Agent": "RebelTalentIntel/5.0 (RSS Reader)"
        })
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
        items = []
        for entry in feed.entries[:max_items]:
            title   = getattr(entry, "title", "").strip()
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
            link    = getattr(entry, "link", "")
            summary = re.sub(r"<[^>]+>", " ", summary)
            summary = re.sub(r"\s+", " ", summary).strip()[:500]
            items.append({
                "title":   title,
                "summary": summary,
                "link":    link,
                "text":    f"{title} {summary}".lower(),
                "source":  url.split("/")[2] if "/" in url else "unknown",
            })
        return items
    except requests.Timeout:
        print(f"  [WARN] Feed timed out ({url[:50]}...)")
        return []
    except Exception as e:
        print(f"  [WARN] Feed failed ({url[:50]}...): {e}")
        return []


def is_large_company(text):
    t = text.lower()
    for name in LARGE_COMPANY_NAMES:
        if re.search(r"\b" + re.escape(name) + r"\b", t):
            return True
    return False


def is_non_us(text):
    t = text.lower()
    return any(geo in t for geo in NON_US_GEOS)


def is_consulting_firm(text):
    """Reject pure consulting/agency firms — no product = not ICP."""
    t = text.lower()
    return any(s in t for s in CONSULTING_SIGNALS)


def geo_score(text):
    t = text.lower()
    return min(sum(1 for g in PRIORITY_GEOS if g in t), 2)


def sector_score(text):
    t = text.lower()
    return min(sum(1 for s in PRIORITY_SECTORS if s in t), 2)


def detect_vertical(text):
    """Classify company into one of the ICP verticals."""
    t = text.lower()
    if any(kw in t for kw in ["fintech", "financial tech", "payments", "insurtech",
                               "lendtech", "regtech", "wealthtech", "neobank",
                               "embedded finance", "banking tech"]):
        return "Fintech"
    if any(kw in t for kw in ["martech", "marketing tech", "adtech", "ad tech",
                               "sales enablement", "revenue intelligence",
                               "go-to-market", "gtm", "marketing automation", "cdp"]):
        return "Martech"
    if any(kw in t for kw in ["defense", "cleared", "govtech", "government tech",
                               "national security", "sbir", "federal", "dod"]):
        return "Defense/GovTech"
    if any(kw in t for kw in ["cybersecurity", "cyber security", "infosec",
                               "zero trust", "soc ", "siem"]):
        return "Cybersecurity"
    if any(kw in t for kw in ["saas", "b2b", "enterprise software", "vertical saas"]):
        return "SaaS/B2B"
    return "Tech"


def extract_company_name(headline):
    patterns = [
        r"^(?:Exclusive:\s*)?(.+?)\s+(?:raises?|lands?|closes?|secures?|nabs?|gets?|nets?|bags?|grabs?)\s+\$",
        r"^(?:Exclusive:\s*)?(.+?)\s+(?:raises?|lands?|closes?|secures?)\s+(?:a\s+)?\$",
        r"^(.+?):\s+\$\d+",
        r"^(.+?)\s+Raises?\s+\$",
    ]
    for pattern in patterns:
        m = re.match(pattern, headline, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            name = re.sub(r"^(?:Exclusive|Breaking|Report|Update):\s*", "", name, flags=re.IGNORECASE)
            if len(name) <= 60:
                return name
    return ""


def extract_funding_amount(text):
    funding_context = r"(?:rais|fund|secur|land|close|round|invest|capital|series|seed|valuation)"
    amount_pattern  = r"\$(\d+(?:\.\d+)?)\s*(million|billion|M|B|m|b)"
    for m in re.finditer(amount_pattern, text, re.IGNORECASE):
        start = max(0, m.start() - 100)
        end   = min(len(text), m.end() + 100)
        if re.search(funding_context, text[start:end].lower()):
            raw = float(m.group(1))
            unit = m.group(2).lower()
            if unit in ("billion", "b"):
                return f"${raw:.1f}B"
            if raw >= 1000:
                return f"${raw / 1000:.1f}B"
            return f"${raw:.0f}M" if raw == int(raw) else f"${raw:.1f}M"
    return ""


def detect_stage(text, amount_str=""):
    t = text.lower()
    if re.search(r"\bseries\s*c\b", t): return "Series C"
    if re.search(r"\bseries\s*b\b", t): return "Series B"
    if re.search(r"\bseries\s*a\b", t): return "Series A"
    if re.search(r"\bseed\s+round\b", t) or re.search(r"\bseed\s+funding\b", t): return "Seed"
    if re.search(r"\bpre-seed\b", t): return "Pre-Seed"
    if amount_str:
        try:
            num = float(re.search(r"[\d.]+", amount_str).group())
            if "B" in amount_str: return "Late Stage"
            elif num >= 50: return "Series C"
            elif num >= 15: return "Series B"
            elif num >= 3:  return "Series A"
            else:           return "Seed"
        except (ValueError, AttributeError):
            pass
    return ""


def is_funding_article(item):
    title = item.get("title", "").lower()
    funding_verbs = [
        r"\braises?\b", r"\blands?\b", r"\bcloses?\b", r"\bsecures?\b",
        r"\bnabs?\b", r"\bgets?\b.*funding", r"\bnets?\b",
        r"\bseries\s+[a-c]\b", r"\bseed\s+round\b", r"\bfunding\s+round\b",
        r"\bvaluation\b", r"\b\$\d+.*(?:million|M|billion|B)\b",
    ]
    noise_patterns = [
        r"^how\s+", r"^why\s+", r"^what\s+", r"^will\s+",
        r"^the\s+(?:future|rise|fall|state|best|top|worst)",
        r"^from\s+hype", r"^before\s+", r"^after\s+", r"^report:",
        r"opinion", r"\bcuts?\s+staff\b", r"\blayoffs?\b",
        r"\brestructur", r"\bbankrupt", r"\bipo\b",
        r"\bpublic\s+offering\b", r"\bstock\s+price\b",
        r"\bnasdaq\b", r"\bnyse\b", r"\bfortune\s+500\b",
        r"^agents?\s+need", r"^the\s+week", r"^biggest\s+funding",
        r"how\s+vcs?\s+", r"\bfda\b", r"\bclinical\s+trial\b",
    ]
    return (
        any(re.search(p, title) for p in funding_verbs)
        and not any(re.search(p, title) for p in noise_patterns)
    )


def is_relevant_signal(item):
    text = item.get("text", "").lower()
    return not is_large_company(text) and not is_non_us(text)


def extract_exec_info(headline):
    patterns = [
        r"(?:appoints?|names?|hires?|promotes?)\s+(.+?)\s+(?:as|to)\s+(.+?)(?:\s*[-\u2013\u2014]|\s*$|\s+to\s+(?:drive|lead|steer|accelerate|strengthen))",
        r"^(.+?)\s+(?:appointed|named|hired|promoted)\s+(?:as\s+)?(.+?)(?:\s+at\s+|\s*[-\u2013\u2014])",
        r"Role\s+Call\s*\|\s*(.+?),\s*(.+?)(?:\s*[-\u2013\u2014]|\s*$)",
    ]
    for pattern in patterns:
        m = re.search(pattern, headline, re.IGNORECASE)
        if m:
            person = re.sub(r"\s+", " ", m.group(1).strip())[:60]
            title  = re.sub(r"\s+", " ", m.group(2).strip())[:80]
            return person, title
    return "", ""


def extract_company_from_headline(headline):
    patterns = [
        r"^(.+?)\s+(?:appoints?|names?|hires?|promotes?)",
        r"at\s+(.+?)(?:\s*[-\u2013\u2014]|\s*$)",
    ]
    for pattern in patterns:
        m = re.search(pattern, headline, re.IGNORECASE)
        if m:
            name = re.sub(r"\s*[-\u2013\u2014]\s*.*$", "", m.group(1).strip())
            if len(name) <= 50:
                return name
    return headline.split(" - ")[0].strip()[:50]


def deduplicate(items, key_field, threshold=25):
    seen, unique = set(), []
    for item in items:
        key = item.get(key_field, "").lower()[:threshold]
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
        elif not key:
            unique.append(item)
    return unique


# ─── SIGNAL PULL FUNCTIONS ───────────────────────────────────────────────────

def pull_funding_signals():
    """Pull and filter funding signals from RSS feeds."""
    print("-> Pulling RSS funding signals...")
    results = []
    for url in FUNDING_FEEDS:
        items = fetch_feed(url, max_items=30)
        for item in items:
            if not is_funding_article(item): continue
            if is_large_company(item["text"]): continue
            if is_non_us(item["text"]): continue
            if is_consulting_firm(item["text"]): continue

            company = extract_company_name(item["title"])
            amount  = extract_funding_amount(item["title"] + " " + item["summary"])
            stage   = detect_stage(item["text"], amount)
            if not company and not amount: continue

            g_score = geo_score(item["text"])
            s_score = sector_score(item["text"])
            score   = g_score + s_score + (2 if company and amount else 0)
            if stage in ("Series A", "Series B", "Series C"):
                score += 2  # ICP sweet spot

            results.append({
                "name":    company or item["title"][:80],
                "amount":  amount or "undisclosed",
                "stage":   stage or "Unknown",
                "sector":  detect_vertical(item["text"]),
                "location": "",
                "headline": item["title"][:120],
                "link":    item["link"],
                "source":  item["source"],
                "score":   score,
            })
    results = deduplicate(results, "name")
    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"  {len(results)} funding signals found")
    return results[:20]


def pull_fintech_signals():
    """Pull fintech-specific funding signals."""
    print("-> Pulling fintech signals...")
    results = []
    for url in FINTECH_FEEDS:
        items = fetch_feed(url, max_items=20)
        for item in items:
            if not is_funding_article(item): continue
            if is_large_company(item["text"]): continue
            if is_non_us(item["text"]): continue
            company = extract_company_name(item["title"])
            amount  = extract_funding_amount(item["title"] + " " + item["summary"])
            results.append({
                "name":    company or item["title"][:80],
                "amount":  amount or "undisclosed",
                "stage":   detect_stage(item["text"], amount) or "Unknown",
                "sector":  "Fintech",
                "headline": item["title"][:120],
                "link":    item["link"],
                "source":  item["source"],
                "score":   geo_score(item["text"]) + 3,
            })
    results = deduplicate(results, "name")
    print(f"  {len(results)} fintech signals found")
    return results[:10]


def pull_martech_signals():
    """Pull martech-specific funding signals."""
    print("-> Pulling martech signals...")
    results = []
    for url in MARTECH_FEEDS:
        items = fetch_feed(url, max_items=20)
        for item in items:
            if not is_funding_article(item): continue
            if is_large_company(item["text"]): continue
            if is_non_us(item["text"]): continue
            company = extract_company_name(item["title"])
            amount  = extract_funding_amount(item["title"] + " " + item["summary"])
            results.append({
                "name":    company or item["title"][:80],
                "amount":  amount or "undisclosed",
                "stage":   detect_stage(item["text"], amount) or "Unknown",
                "sector":  "Martech",
                "headline": item["title"][:120],
                "link":    item["link"],
                "source":  item["source"],
                "score":   geo_score(item["text"]) + 3,
            })
    results = deduplicate(results, "name")
    print(f"  {len(results)} martech signals found")
    return results[:10]


def pull_hiring_signals():
    """Pull hiring signals — including Sales and Tech roles."""
    print("-> Pulling hiring signals...")
    results = []
    for url in HIRING_SIGNAL_FEEDS:
        items = fetch_feed(url, max_items=25)
        for item in items:
            if not is_relevant_signal(item): continue
            text = item["text"]
            has_signal = any(kw in text for kw in [
                "head of talent", "vp people", "vp of people", "chief people",
                "chro", "director recruiting", "director of recruiting",
                "hiring", "headcount", "team growth", "scaling team",
                "talent acquisition", "fractional hr", "people ops",
                "series a", "series b", "series c", "startup hiring",
                "workforce", "recruiting", "people operations",
                "vp sales", "vp of sales", "cto", "vp engineering",
                "head of sales", "chief revenue officer",
            ])
            if not has_signal: continue

            is_sales_hire = any(kw in text for kw in [
                "vp sales", "vp of sales", "head of sales",
                "chief revenue officer", "cro", "account executive", "sales"
            ])
            is_tech_hire = any(kw in text for kw in [
                "cto", "vp engineering", "vp of engineering",
                "head of engineering", "software engineer", "engineering"
            ])

            company = extract_company_from_headline(item["title"])
            results.append({
                "company":       company,
                "role":          item["title"][:100],
                "signal":        item["summary"][:250],
                "link":          item["link"],
                "source":        item["source"],
                "is_sales_hire": is_sales_hire,
                "is_tech_hire":  is_tech_hire,
            })
    results = deduplicate(results, "company")
    print(f"  {len(results)} hiring signals found")
    return results[:12]


def pull_exec_moves():
    """Pull executive moves — People, Sales, and Tech leaders."""
    print("-> Pulling executive moves...")
    results = []
    for url in EXEC_MOVE_FEEDS:
        items = fetch_feed(url, max_items=25)
        for item in items:
            if not is_relevant_signal(item): continue
            text = item["text"]

            is_people_move = any(kw in text for kw in [
                "chief people", "chro", "vp talent", "vp of talent",
                "head of people", "head of talent", "director of recruiting",
                "chief human resources", "vp people", "vp of people",
                "people officer", "talent acquisition",
            ])
            is_sales_move = any(kw in text for kw in [
                "vp sales", "vp of sales", "chief revenue officer", "cro",
                "head of sales", "director of sales", "vp business development",
            ])
            is_tech_move = any(kw in text for kw in [
                "chief technology officer", "cto ", " cto",
                "vp engineering", "vp of engineering", "head of engineering",
                "chief product officer", "vp product",
            ])

            if not (is_people_move or is_sales_move or is_tech_move):
                continue

            person, title = extract_exec_info(item["title"])
            company = extract_company_from_headline(item["title"])
            move_type = "people" if is_people_move else ("sales" if is_sales_move else "tech")

            results.append({
                "person":    person,
                "title":     title,
                "company":   company,
                "headline":  item["title"][:120],
                "link":      item["link"],
                "source":    item["source"],
                "move_type": move_type,
            })
    results = deduplicate(results, "company")
    print(f"  {len(results)} exec moves found ({sum(1 for r in results if r['move_type']=='sales')} sales, {sum(1 for r in results if r['move_type']=='tech')} tech)")
    return results[:10]


def pull_sam_gov():
    """Pull SBIR/contract awards from SAM.gov."""
    print("-> Pulling SAM.gov SBIR awards...")
    results = []
    sam_api_key = os.environ.get("SAM_GOV_API_KEY", "DEMO_KEY")
    today_str = datetime.date.today().strftime("%m/%d/%Y")
    start_str = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%m/%d/%Y")

    try:
        resp = requests.get(
            "https://api.sam.gov/opportunities/v2/search",
            params={"api_key": sam_api_key, "postedFrom": start_str,
                    "postedTo": today_str, "ptype": "s", "limit": 25, "offset": 0},
            timeout=15,
        )
        if resp.status_code == 429:
            print("  [WARN] SAM.gov rate limited (DEMO_KEY)")
            return []
        if not resp.ok:
            print(f"  [WARN] SAM.gov returned {resp.status_code}")
            return []

        for opp in resp.json().get("opportunitiesData", []):
            title = opp.get("title", "")
            agency = opp.get("fullParentPathName", opp.get("organizationName", ""))
            desc = opp.get("description", "")[:200]
            is_phase2 = "phase ii" in title.lower() or "phase 2" in title.lower()
            is_tech = any(s in (title + desc).lower() for s in
                         ["software", "ai", "cyber", "data", "saas", "platform", "cloud", "machine learning"])
            if is_tech:
                notice_id = opp.get("noticeId", "")
                results.append({
                    "title":    title[:100],
                    "agency":   agency[:60],
                    "type":     "SBIR Phase II" if is_phase2 else "SBIR",
                    "posted":   opp.get("postedDate", ""),
                    "signal":   f"{agency} — {title[:80]}",
                    "why_now":  "SBIR award = funded startup, hiring wave incoming.",
                    "link":     f"https://sam.gov/opp/{notice_id}" if notice_id else "https://sam.gov",
                })
        print(f"  {len(results)} SAM.gov SBIR signals found")
        return results[:8]
    except Exception as e:
        print(f"  [WARN] SAM.gov error: {e}")
        return []


def pull_gov_contract_signals():
    """Pull government contract award signals from news RSS."""
    print("-> Pulling gov contract signals...")
    results = []
    for url in GOV_CONTRACT_FEEDS:
        items = fetch_feed(url, max_items=15)
        for item in items:
            if is_large_company(item["text"]): continue
            if is_non_us(item["text"]): continue
            company = extract_company_from_headline(item["title"])
            amount  = extract_funding_amount(item["title"] + " " + item["summary"])
            results.append({
                "company":  company,
                "headline": item["title"][:120],
                "amount":   amount,
                "link":     item["link"],
                "signal":   "Government contract award — cleared/tech hiring incoming.",
            })
    results = deduplicate(results, "company")
    print(f"  {len(results)} gov contract signals found")
    return results[:6]


def pull_competitor_intel():
    print("-> Pulling competitor intel...")
    results = []
    for url in COMPETITOR_FEEDS:
        items = fetch_feed(url, max_items=15)
        for item in items:
            results.append({"headline": item["title"][:120], "link": item["link"]})
    return results[:6]


def pull_news_headlines():
    print("-> Pulling news headlines...")
    headlines = []
    for url in FUNDING_FEEDS[:2]:
        items = fetch_feed(url, max_items=10)
        for item in items:
            if is_funding_article(item) and not is_large_company(item["text"]):
                headlines.append(item["title"])
    return headlines[:10]


# ─── v4.0 DATA SOURCES ───────────────────────────────────────────────────────

def pull_yc_companies():
    print("-> Pulling Y Combinator batch data...")
    results = []
    for batch in ["s2025", "w2025", "s2024"]:
        try:
            resp = requests.get(
                f"https://yc-oss.github.io/api/batches/{batch}.json",
                timeout=12, headers={"User-Agent": "RebelTalentIntel/5.0"}
            )
            if not resp.ok: continue
            for co in resp.json():
                name   = co.get("name", "")
                status = (co.get("status") or "").lower()
                if not name or is_large_company(name.lower()): continue
                if status in ("inactive", "acquired", "dead"): continue
                results.append({
                    "name":       name,
                    "batch":      co.get("batch", batch).upper(),
                    "description":(co.get("one_liner") or co.get("long_description") or "")[:200],
                    "url":        co.get("url", ""),
                    "industries": co.get("tags") or co.get("industries") or [],
                    "status":     status or "active",
                })
        except Exception as e:
            print(f"  [WARN] YC batch {batch} failed: {e}")
    print(f"  {len(results)} YC companies loaded")
    return results


def pull_hn_hiring():
    print("-> Pulling Hacker News hiring threads...")
    results = []
    try:
        resp = requests.get(
            "https://hn.algolia.com/api/v1/search_by_date",
            params={"query": "Ask HN: Who is hiring", "tags": "story,author_whoishiring", "hitsPerPage": 1},
            timeout=12,
        )
        resp.raise_for_status()
        hits = resp.json().get("hits", [])
        if not hits:
            print("  [WARN] No HN hiring thread found")
            return []

        thread_id = hits[0]["objectID"]
        print(f"  Found thread: {hits[0].get('title', '')}")

        resp2 = requests.get(f"https://hn.algolia.com/api/v1/items/{thread_id}", timeout=15)
        resp2.raise_for_status()
        thread = resp2.json()

        for child in (thread.get("children") or [])[:80]:
            text  = child.get("text", "") or ""
            clean = re.sub(r"<[^>]+>", " ", text)
            clean = re.sub(r"\s+", " ", clean).strip()
            if not clean or len(clean) < 30: continue

            first_line = clean.split("|")[0].strip() if "|" in clean else clean[:60]
            company    = first_line.strip()[:80]
            is_remote  = any(kw in clean.lower() for kw in ["remote", "fully remote", "remote ok"])

            location = ""
            loc_match = re.search(
                r"(?:located?\s+in|based\s+in|offices?\s+in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
                clean
            )
            if loc_match:
                location = loc_match.group(1)

            roles = re.findall(r"(?:hiring|looking for|seeking)\s+(?:a\s+)?([^,.]+)", clean.lower())

            has_sales_role = any(kw in clean.lower() for kw in
                ["sales", "account executive", "ae ", "business development", "bd "])
            has_tech_role  = any(kw in clean.lower() for kw in
                ["engineer", "developer", "software", "backend", "frontend", "devops", "data scientist"])

            if is_non_us(clean.lower()) and not is_remote: continue
            if is_large_company(clean.lower()): continue

            results.append({
                "company":        company,
                "location":       location,
                "remote":         is_remote,
                "roles":          [r.strip()[:60] for r in roles[:4]],
                "has_sales_role": has_sales_role,
                "has_tech_role":  has_tech_role,
                "text":           clean[:300],
            })
    except Exception as e:
        print(f"  [WARN] HN hiring pull failed: {e}")
    print(f"  {len(results)} HN hiring signals found")
    return results[:30]


def pull_github_trending():
    print("-> Pulling GitHub trending repos...")
    results = []
    try:
        resp = requests.get(
            "https://github.com/trending?since=daily",
            timeout=12, headers={"User-Agent": "RebelTalentIntel/5.0"}
        )
        resp.raise_for_status()
        html = resp.text

        repos      = re.findall(r'<h2[^>]*>\s*<a[^>]*href="(/[^"]+)"[^>]*>', html)
        descs      = re.findall(r'<p class="col-9[^"]*">(.*?)</p>', html, re.DOTALL)
        langs      = re.findall(r'itemprop="programmingLanguage">([^<]+)</span>', html)
        stars_list = re.findall(r"(\d[\d,]*)\s*stars?\s*today", html)

        for i, repo_path in enumerate(repos[:25]):
            parts = repo_path.strip("/").split("/")
            if len(parts) != 2: continue
            owner, repo_name = parts
            if is_large_company(owner.lower()): continue

            desc  = re.sub(r"<[^>]+>", "", descs[i]).strip()[:200] if i < len(descs) else ""
            lang  = langs[i].strip() if i < len(langs) else ""
            stars = stars_list[i].replace(",", "") if i < len(stars_list) else "0"

            results.append({
                "repo":        repo_name,
                "owner":       owner,
                "stars_today": int(stars) if stars.isdigit() else 0,
                "language":    lang,
                "description": desc,
                "url":         f"https://github.com{repo_path}",
            })
    except Exception as e:
        print(f"  [WARN] GitHub trending failed: {e}")
    print(f"  {len(results)} trending repos found")
    return results[:15]


def pull_sec_edgar():
    print("-> Pulling SEC EDGAR Form D filings...")
    results = []
    today = datetime.date.today()
    start = today - datetime.timedelta(days=3)
    for query in ['"series a"', '"series b"', '"series c" technology']:
        try:
            resp = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={"q": query, "dateRange": "custom",
                        "startdt": start.strftime("%Y-%m-%d"),
                        "enddt":   today.strftime("%Y-%m-%d"), "forms": "D"},
                timeout=12,
                headers={"User-Agent": "RebelTalentIntel/5.0 rebel-talent@pm.me", "Accept": "application/json"},
            )
            if not resp.ok: continue
            for hit in resp.json().get("hits", {}).get("hits", [])[:15]:
                src = hit.get("_source", {})
                company = src.get("entity_name", "")
                if isinstance(src.get("display_names"), list) and src["display_names"]:
                    company = src["display_names"][0]
                if not company or is_large_company(company.lower()): continue
                is_tech = any(kw in json.dumps(src).lower() for kw in
                    ["software", "saas", "technology", "ai", "machine learning", "platform", "data", "cloud", "cyber"])
                if not is_tech: continue
                results.append({
                    "company":      company[:80],
                    "filing_date":  src.get("file_date", ""),
                    "form_type":    "Form D",
                    "description":  src.get("display_description", "")[:200],
                    "link": f"https://www.sec.gov/cgi-bin/browse-edgar?company={company}&type=D&action=getcompany",
                })
        except Exception as e:
            print(f"  [WARN] SEC EDGAR query failed: {e}")
    results = deduplicate(results, "company")
    print(f"  {len(results)} SEC Form D filings found")
    return results[:10]


def pull_producthunt():
    print("-> Pulling ProductHunt launches...")
    results = []
    try:
        items = fetch_feed("https://www.producthunt.com/feed", max_items=20)
        for item in items:
            if is_large_company(item["text"]): continue
            product = re.split(r"\s*[-\u2013\u2014]\s*", item.get("title", ""))[0].strip()
            results.append({
                "product": product[:80],
                "tagline": item.get("summary", "")[:200],
                "link":    item.get("link", ""),
                "source":  "producthunt.com",
            })
    except Exception as e:
        print(f"  [WARN] ProductHunt pull failed: {e}")
    print(f"  {len(results)} ProductHunt launches found")
    return results[:10]


# ─── APOLLO API INTEGRATION (v5.0) ──────────────────────────────────────────

def apollo_get_domain(company_name):
    """Find company domain via Apollo org search."""
    if not APOLLO_API_KEY:
        return ""
    try:
        resp = requests.post(
            f"{APOLLO_BASE}/organizations/search",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
            json={"api_key": APOLLO_API_KEY, "q_organization_name": company_name,
                  "page": 1, "per_page": 1},
            timeout=12,
        )
        if resp.ok:
            orgs = resp.json().get("organizations", []) or []
            if orgs and isinstance(orgs[0], dict):
                return orgs[0].get("primary_domain", "")
    except Exception:
        pass
    return ""


def apollo_enrich_org(domain):
    """Get org details from Apollo organizations/enrich."""
    if not APOLLO_API_KEY or not domain:
        return {}
    try:
        resp = requests.post(
            f"{APOLLO_BASE}/organizations/enrich",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
            json={"api_key": APOLLO_API_KEY, "domain": domain},
            timeout=15,
        )
        if not resp.ok:
            return {}
        org = resp.json().get("organization", {}) or {}
        events = org.get("funding_events") or []
        total_usd = sum((e.get("amount") or 0) for e in events if isinstance(e, dict))
        return {
            "website":    org.get("website_url", domain),
            "linkedin_url": org.get("linkedin_url", ""),
            "employee_count": org.get("estimated_num_employees", 0) or 0,
            "industry":   org.get("industry", ""),
            "description": org.get("short_description", ""),
            "funding_history": [
                {
                    "date":      e.get("date", ""),
                    "type":      e.get("type", ""),
                    "amount":    f"${e['amount']/1_000_000:.0f}M" if e.get("amount") else "",
                    "investors": (e.get("investors") or [])[:3],
                }
                for e in events[:5] if isinstance(e, dict)
            ],
            "total_funding": f"${total_usd/1_000_000:.0f}M" if total_usd else "",
            "annual_revenue": org.get("annual_revenue_printed", ""),
            "tech_stack":  [
                (t.get("name", "") if isinstance(t, dict) else str(t))
                for t in (org.get("technology_names") or [])[:10]
            ],
        }
    except Exception as e:
        print(f"  [WARN] Apollo org enrich failed for {domain}: {e}")
        return {}


def apollo_find_contacts(company_name, domain=""):
    """Find decision makers at a company via Apollo people/search."""
    if not APOLLO_API_KEY:
        return []

    payload = {
        "api_key": APOLLO_API_KEY,
        "page": 1,
        "per_page": 5,
        "person_titles": [
            "VP of People", "VP People", "Head of People", "Head of Talent",
            "Chief People Officer", "CHRO", "Director of People",
            "VP of HR", "VP HR", "Director of HR", "Director of Talent",
            "VP Sales", "VP of Sales", "Chief Revenue Officer", "CRO",
            "Head of Sales", "Director of Sales",
            "CTO", "Chief Technology Officer", "VP Engineering", "VP of Engineering",
            "Head of Engineering",
            "CEO", "Co-Founder", "Founder",
        ],
        "organization_num_employees_ranges": ["1,100"],
    }
    if domain:
        payload["organization_domains"] = [domain]
    else:
        payload["q_organization_name"] = company_name

    try:
        resp = requests.post(
            f"{APOLLO_BASE}/mixed_people/search",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
            json=payload,
            timeout=15,
        )
        if not resp.ok:
            print(f"  [WARN] Apollo people search {resp.status_code} for {company_name}")
            return []

        results = []
        for p in resp.json().get("people", [])[:5]:
            phones = p.get("phone_numbers", []) or []
            phone  = ""
            if phones and isinstance(phones, list):
                first = phones[0]
                phone = first.get("sanitized_number", "") if isinstance(first, dict) else str(first)

            results.append({
                "name":        p.get("name", ""),
                "title":       p.get("title", ""),
                "email":       p.get("email", ""),
                "phone":       phone,
                "linkedin_url": p.get("linkedin_url", ""),
                "apollo_url":  f"https://app.apollo.io/#/people/{p.get('id', '')}",
                "seniority":   p.get("seniority", ""),
            })
        return results
    except Exception as e:
        print(f"  [WARN] Apollo contacts failed for {company_name}: {e}")
        return []


def enrich_targets_with_apollo(targets):
    """Enrich top target companies with Apollo data (contacts + org info)."""
    if not APOLLO_API_KEY:
        print("  [WARN] No APOLLO_API_KEY — skipping Apollo enrichment")
        return {}

    print(f"-> Apollo enrichment for up to {min(len(targets), 15)} targets...")
    enriched = {}

    for target in targets[:15]:
        name = target.get("company") or target.get("name", "")
        if not name:
            continue
        print(f"  Enriching: {name}")
        try:
            domain   = apollo_get_domain(name);            time.sleep(0.3)
            org_data = apollo_enrich_org(domain) if domain else {}; time.sleep(0.3)
            contacts = apollo_find_contacts(name, domain);           time.sleep(0.3)

            emp_count = org_data.get("employee_count", 0)
            icp_fit   = True
            if emp_count and (emp_count < 20 or emp_count > 100):
                icp_fit = False
                print(f"    [FILTER] {name}: {emp_count} employees — outside 20-100 ICP")

            enriched[name.lower()] = {
                "domain":   domain,
                "icp_fit":  icp_fit,
                "contacts": contacts,
                **org_data,
            }
        except Exception as e:
            print(f"  [WARN] Apollo enrichment failed for {name}: {e}")

    print(f"  Apollo enrichment complete: {len(enriched)} companies, "
          f"{sum(len(v.get('contacts',[])) for v in enriched.values())} contacts found")
    return enriched


# ─── LEAD PROFILE BUILDER (v5.0) ─────────────────────────────────────────────

def build_outreach_angle(target, apollo_data):
    """Build a specific, actionable outreach angle for each lead."""
    angles = []
    stage     = target.get("stage", "")
    amount    = target.get("amount", "")
    emp_count = apollo_data.get("employee_count", 0)
    contacts  = apollo_data.get("contacts", [])
    vertical  = detect_vertical(
        str(target) + " " + apollo_data.get("industry", "") + " " + apollo_data.get("description", "")
    )

    if stage in ("Series A", "Series B", "Series C"):
        angles.append(f"Just raised {amount} ({stage}) — peak hiring window. "
                      "High urgency to build team without a dedicated talent function.")

    if emp_count and 20 <= emp_count <= 60:
        angles.append(f"At {emp_count} employees, fractional Head of Talent delivers max ROI. "
                      "Too small for a full-time hire, too big to wing it.")
    elif emp_count and 60 < emp_count <= 100:
        angles.append(f"At {emp_count} employees, hitting the inflection point. "
                      "Recruiting infrastructure becomes mission-critical.")

    dm_titles = [c.get("title", "").lower() for c in contacts]
    has_people_lead = any(kw in t for t in dm_titles for kw in ["people", "hr", "talent", "recruiting"])
    if not has_people_lead and contacts:
        ceo_or_founder = next(
            (c["name"] for c in contacts if any(kw in c.get("title","").lower() for kw in ["ceo", "founder"])),
            None
        )
        if ceo_or_founder:
            angles.append(f"No People/HR lead found — {ceo_or_founder} (CEO/Founder) is likely doing recruiting. "
                          "Direct pitch opportunity.")
        else:
            angles.append("No visible People/HR lead — CEO or CTO likely owns recruiting. Direct pitch opportunity.")

    if "defense" in vertical.lower() or "govtech" in vertical.lower():
        angles.append("Defense/cleared hiring = specialized need. Rebel has cleared tech recruiting expertise.")
    elif "fintech" in vertical.lower():
        angles.append("Fintech = high compliance + technical bar for hires. Need a recruiter who understands the domain.")
    elif "martech" in vertical.lower():
        angles.append("Martech = simultaneous Sales + Engineering scale. Fractional model covers both tracks.")

    return " ".join(angles[:2]) if angles else "Recently funded startup entering active hiring phase."


def build_lead_profiles(funded, cross_references, apollo_enriched,
                         hn_hiring, execs, fintech, martech):
    """Build comprehensive per-company lead profiles combining all intelligence."""
    profiles = []
    seen = set()

    # Merge all targets — cross-refs first (highest conviction)
    all_targets = []
    for xr in cross_references[:8]:
        all_targets.append({
            "name":         xr.get("company", ""),
            "amount":       xr.get("details", {}).get("funding", ""),
            "stage":        "",
            "sector":       xr.get("details", {}).get("sector", ""),
            "link":         "",
            "headline":     "",
            "sources":      xr.get("sources", []),
            "source_count": xr.get("source_count", 1),
        })

    for item_list in [funded, fintech, martech]:
        for f in item_list:
            all_targets.append({**f, "sources": ["rss"], "source_count": 1})

    for target in all_targets:
        name = target.get("name", "")
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())

        apollo = apollo_enriched.get(name.lower(), {})

        # ICP filter: skip if employee count confirmed outside 20-100
        if not apollo.get("icp_fit", True):
            continue

        # Find related HN hiring signals
        hn_signals = [h for h in hn_hiring
                      if name.lower()[:10] in h.get("company", "").lower()]

        # Find related exec moves
        exec_signals = [e for e in execs
                        if name.lower()[:10] in e.get("company", "").lower()]

        vertical = detect_vertical(
            str(target) + " "
            + apollo.get("industry", "") + " "
            + apollo.get("description", "")
        )

        profiles.append({
            "name":             name,
            "website":          apollo.get("website", "") or target.get("link", ""),
            "linkedin_company": apollo.get("linkedin_url", ""),
            "what_they_do":     apollo.get("description") or target.get("headline", ""),
            "vertical":         vertical,
            "industry":         apollo.get("industry", target.get("sector", "")),
            "funding": {
                "latest_amount": target.get("amount", ""),
                "stage":         target.get("stage", ""),
                "total":         apollo.get("total_funding", ""),
                "history":       apollo.get("funding_history", []),
                "news_link":     target.get("link", ""),
                "headline":      target.get("headline", ""),
            },
            "employee_count":   apollo.get("employee_count", 0),
            "annual_revenue":   apollo.get("annual_revenue", ""),
            "tech_stack":       apollo.get("tech_stack", []),
            "decision_makers":  apollo.get("contacts", []),
            "open_roles":       hn_signals[0].get("roles", []) if hn_signals else [],
            "has_sales_role":   any(h.get("has_sales_role") for h in hn_signals),
            "has_tech_role":    any(h.get("has_tech_role") for h in hn_signals),
            "recent_exec_move": exec_signals[0].get("headline", "") if exec_signals else "",
            "exec_move_link":   exec_signals[0].get("link", "") if exec_signals else "",
            "exec_move_type":   exec_signals[0].get("move_type", "") if exec_signals else "",
            "data_sources":     target.get("sources", ["rss"]),
            "source_count":     target.get("source_count", 1),
            "outreach_angle":   build_outreach_angle(target, apollo),
        })

        if len(profiles) >= 12:
            break

    return profiles


# ─── CROSS-REFERENCE ENGINE ───────────────────────────────────────────────────

def build_cross_references(funded, yc_companies, hn_hiring, github_trending,
                             sec_filings, ph_launches, fintech=None, martech=None):
    print("-> Building cross-references...")
    company_sources = {}

    def normalize(name):
        return re.sub(r"[^a-z0-9]", "", name.lower().strip())

    def add(key_name, source_name, detail_key="", detail_val=""):
        key = normalize(key_name)
        if key and len(key) > 2:
            company_sources.setdefault(key, {"name": key_name, "sources": set(), "details": {}})
            company_sources[key]["sources"].add(source_name)
            if detail_key and detail_val:
                company_sources[key]["details"][detail_key] = detail_val

    for item in funded:
        add(item.get("name",""), "rss_funding", "funding", item.get("amount",""))
    for item in (fintech or []):
        add(item.get("name",""), "fintech", "sector", "Fintech")
    for item in (martech or []):
        add(item.get("name",""), "martech", "sector", "Martech")
    for item in yc_companies:
        add(item.get("name",""), "yc", "yc_batch", item.get("batch",""))
    for item in hn_hiring:
        add(item.get("company","").split("|")[0].strip(), "hn_hiring")
    for item in github_trending:
        add(item.get("owner",""), "github", "trending_repo", item.get("repo",""))
    for item in sec_filings:
        add(item.get("company",""), "sec_edgar")
    for item in ph_launches:
        add(item.get("product",""), "producthunt")

    cross_refs = [
        {
            "company":      info["name"],
            "sources":      sorted(list(info["sources"])),
            "source_count": len(info["sources"]),
            "details":      {k: v for k, v in info["details"].items() if v},
        }
        for info in company_sources.values() if len(info["sources"]) >= 2
    ]
    cross_refs.sort(key=lambda x: x["source_count"], reverse=True)
    print(f"  {len(cross_refs)} companies appear in 2+ sources")
    return cross_refs[:15]


def enrich_funded_with_yc(funded, yc_companies):
    yc_lookup = {co["name"].lower(): co for co in yc_companies}
    for item in funded:
        if item["name"].lower() in yc_lookup:
            item["yc_batch"] = yc_lookup[item["name"].lower()].get("batch", "")
            item["yc_match"] = True
            item["score"]    = item.get("score", 0) + 2
    return funded


# ─── ANALYTICS BUILDERS ───────────────────────────────────────────────────────

def build_source_stats(funded, hiring, execs, sam_signals, yc_companies,
                        hn_hiring, github_trending, sec_filings, ph_launches,
                        fintech=None, martech=None, gov_contracts=None):
    return {
        "rss_funding":     len(funded),
        "rss_hiring":      len(hiring),
        "rss_execs":       len(execs),
        "sam_gov":         len(sam_signals),
        "yc":              len(yc_companies),
        "hn_hiring":       len(hn_hiring),
        "github":          len(github_trending),
        "sec_edgar":       len(sec_filings),
        "producthunt":     len(ph_launches),
        "fintech_signals": len(fintech or []),
        "martech_signals": len(martech or []),
        "gov_contracts":   len(gov_contracts or []),
    }


def build_stage_breakdown(funded, intel, fintech=None, martech=None):
    breakdown = {}
    for item in (funded + (fintech or []) + (martech or [])):
        stage = item.get("stage", "Unknown")
        if stage:
            breakdown[stage] = breakdown.get(stage, 0) + 1
    for item in intel.get("new_money", []):
        stage = item.get("stage", "Unknown")
        if stage:
            breakdown[stage] = breakdown.get(stage, 0) + 1
    return breakdown


def build_sector_breakdown(funded, yc_companies, fintech=None, martech=None):
    breakdown = {}
    for item in funded:
        s = item.get("sector", "")
        if s: breakdown[s] = breakdown.get(s, 0) + 1
    breakdown["Fintech"] = breakdown.get("Fintech", 0) + len(fintech or [])
    breakdown["Martech"] = breakdown.get("Martech", 0) + len(martech or [])
    for item in yc_companies:
        for ind in (item.get("industries") or [])[:2]:
            if ind: breakdown[ind] = breakdown.get(ind, 0) + 1
    return dict(sorted(breakdown.items(), key=lambda x: x[1], reverse=True)[:10])


# ─── MATPLOTLIB CHART GENERATION ─────────────────────────────────────────────

def generate_pdf_charts(data):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        bg     = "#050d1b"
        txt    = "#6a7a8a"
        colors = ["#00ffd5","#ffa500","#ff006e","#b47cff","#00ff41","#ff3366","#4fc3f7","#81c784"]
        charts = []

        stages = data.get("stage_breakdown", {})
        if stages:
            fig, ax = plt.subplots(figsize=(3.5, 2.5), facecolor=bg)
            ax.set_facecolor(bg)
            wedges, texts, autotexts = ax.pie(
                stages.values(), labels=stages.keys(),
                colors=colors[:len(stages)], autopct="%1.0f%%", pctdistance=0.75,
                wedgeprops={"width": 0.45, "edgecolor": bg, "linewidth": 1},
            )
            for t in texts + autotexts:
                t.set_color(txt); t.set_fontsize(7)
            ax.set_title("FUNDING BY STAGE", color=txt, fontsize=8, fontfamily="monospace", pad=10)
            fig.savefig("docs/chart_stage.png", dpi=150, bbox_inches="tight", facecolor=bg)
            plt.close(fig)
            charts.append("chart_stage.png")

        stats = {k: v for k, v in data.get("source_stats", {}).items() if v > 0}
        if stats:
            fig, ax = plt.subplots(figsize=(3.5, 2.5), facecolor=bg)
            ax.set_facecolor(bg)
            labels = [k.replace("_", " ").upper() for k in stats]
            vals   = list(stats.values())
            bar_colors = ["#00ffd5" if v > 5 else "#ffa500" if v > 2 else "#ff006e" for v in vals]
            bars = ax.barh(labels, vals, color=bar_colors, height=0.6)
            ax.tick_params(colors=txt, labelsize=6)
            ax.set_title("DATA SOURCE COVERAGE", color=txt, fontsize=8, fontfamily="monospace", pad=10)
            for spine in ax.spines.values(): spine.set_color("#1a2440")
            for bar in bars:
                ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                        f"{int(bar.get_width())}", va="center", color=txt, fontsize=7)
            fig.savefig("docs/chart_sources.png", dpi=150, bbox_inches="tight", facecolor=bg)
            plt.close(fig)
            charts.append("chart_sources.png")

        print(f"  {len(charts)} PDF charts generated")
        return charts
    except ImportError:
        print("  [WARN] matplotlib not available — skipping PDF charts")
        return []
    except Exception as e:
        print(f"  [WARN] Chart generation failed: {e}")
        return []


# ─── GROQ SYNTHESIS (v5.0) ───────────────────────────────────────────────────

def synthesize_with_groq(funded, hiring, execs, headlines, sam_signals=None,
                          yc_companies=None, cross_references=None,
                          fintech=None, martech=None, lead_profiles=None):
    print("-> Calling Groq for synthesis (llama-3.3-70b)...")

    if not GROQ_API_KEY:
        print("  [WARN] No GROQ_API_KEY — using fallback data")
        return fallback_data(funded, hiring, execs)

    apollo_summary = [
        {
            "company":         p["name"],
            "vertical":        p.get("vertical", ""),
            "employees":       p.get("employee_count", "unknown"),
            "funding":         p["funding"].get("latest_amount", ""),
            "stage":           p["funding"].get("stage", ""),
            "decision_makers": [{"name": dm["name"], "title": dm["title"]}
                                 for dm in p.get("decision_makers", [])[:3]],
            "outreach_angle":  p.get("outreach_angle", ""),
            "tech_stack":      p.get("tech_stack", [])[:5],
        }
        for p in (lead_profiles or [])[:5]
    ]

    context = json.dumps({
        "funded_companies":    funded[:8],
        "fintech_signals":     (fintech or [])[:5],
        "martech_signals":     (martech or [])[:5],
        "hiring_signals":      hiring[:8],
        "exec_moves":          execs[:6],
        "sam_sbir_signals":    (sam_signals or [])[:5],
        "news_headlines":      headlines[:8],
        "yc_batch_companies":  (yc_companies or [])[:10],
        "cross_references":    (cross_references or [])[:5],
        "apollo_enriched_leads": apollo_summary,
    }, indent=2)

    system_prompt = """You are the BD Oracle for Richie Lampani at Rebel Talent Systems.

ABOUT REBEL TALENT SYSTEMS:
- Fractional Head of Talent / Director of Recruiting practice
- Richie engages as interim talent leader at Series A-C startups
- Builds recruiting infrastructure clients own permanently
- Pricing: $8K / $10.6K / $14K per month
- ICP v5.0: Series A-C startup, 20-100 employees, US-based, no dedicated talent lead, needs 5-20 hires in 12 months
- Proof point: EDF engagement — 6 roles filled, $178K agency fees avoided, 350% ROI
- ALSO sends to Shaun at Arkham Talent — he does similar fractional talent work

TARGET VERTICALS (v5.0):
1. SaaS / B2B tech — needs full recruiting build-out post-Series A
2. Defense tech / GovTech / Cleared — SBIR Phase II winners, cleared hiring
3. Fintech — high compliance + technical hiring bar, need domain expert
4. Martech — simultaneous Sales + Engineering growth, time-sensitive
5. HR Tech / People platforms — adjacent market, warm intros

ICP v5.0 FILTERS:
- Company size: STRICTLY 20-100 employees
- US-based ONLY
- Must have active or likely hiring in Sales OR Tech roles
- Reject: consulting firms, agencies, companies with no product
- Score HIGHEST: recent funding + active Sales/Tech hiring + right vertical + no people function

IF apollo_enriched_leads has decision makers: name them specifically in action items.
IF a company appears in cross_references: flag it HIGH CONVICTION.

Return ONLY valid JSON — no markdown fences:
{
  "moves_today": ["3 max — imperative, specific, name decision makers from Apollo if known"],
  "top_3": ["3 max — top companies with detailed reasoning"],
  "new_money": [{"company":"","amount":"","stage":"","location":"","sector_tag":"","why_now":""}],
  "hiring_signals": ["2-4 items"],
  "exec_moves": ["2-4 items"],
  "defense_signals": ["1-3 items or empty array"],
  "fintech_signals": ["1-3 items or empty array"],
  "martech_signals": ["1-3 items or empty array"],
  "vehicles": ["1-2 items"],
  "competitive": ["1-2 items"],
  "market_trends": ["2-3 macro trends relevant to fractional recruiting right now"]
}"""

    total_sources = sum(1 for x in [funded, hiring, execs, sam_signals, yc_companies,
                                     cross_references, fintech, martech, lead_profiles] if x)

    for attempt in range(3):
        try:
            client = Groq(api_key=GROQ_API_KEY)
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                max_tokens=3000,
                temperature=0.3,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content":
                     f"Today is {TODAY}. Here is intelligence from {total_sources} sources:\n\n{context}\n\n"
                     "Synthesize into actionable BD intel. Return ONLY valid JSON."},
                ],
            )
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"```\s*$", "", raw)
            result = json.loads(raw.strip())
            result = post_process_groq(result)
            print("  Groq synthesis complete")
            return result
        except json.JSONDecodeError as e:
            print(f"  [WARN] Groq JSON parse error (attempt {attempt + 1}): {e}")
            if attempt < 2: time.sleep(2)
        except Exception as e:
            print(f"  [WARN] Groq call failed (attempt {attempt + 1}): {e}")
            if attempt < 2: time.sleep(3)

    print("  [WARN] All Groq attempts failed — using fallback")
    return fallback_data(funded, hiring, execs)


def post_process_groq(result):
    for key in ["moves_today", "top_3", "hiring_signals", "exec_moves",
                "defense_signals", "fintech_signals", "martech_signals",
                "vehicles", "competitive", "market_trends"]:
        if key in result and isinstance(result[key], list):
            result[key] = [item for item in result[key] if not is_large_company(str(item))]
    if "new_money" in result and isinstance(result["new_money"], list):
        result["new_money"] = [item for item in result["new_money"]
                               if not is_large_company(item.get("company", ""))]
    return result


def fallback_data(funded, hiring, execs):
    moves = []
    if funded:
        moves.append(f"Research {funded[0]['name']} — recently funded ({funded[0]['amount']}). Check for talent function.")
    if execs:
        moves.append(f"Connect with new leader at {execs[0]['company']} — they need recruiting infrastructure.")
    moves.append("Post in GoFractional and FPP Slack about Q2 availability for Series A-B talent builds.")
    return {
        "moves_today":    moves[:3],
        "top_3":          [f"{f['name']} raised {f['amount']} ({f['stage']}) — entering active hiring phase." for f in funded[:3]]
                          or ["No high-confidence targets today — check feed connectivity."],
        "new_money":      [{"company": f["name"][:50], "amount": f["amount"], "stage": f["stage"],
                            "location": "", "sector_tag": f.get("sector",""), "why_now": "Recently funded."}
                           for f in funded[:5]],
        "hiring_signals": [h["role"][:120] for h in hiring[:3]] or ["No strong hiring signals today."],
        "exec_moves":     [e["headline"][:120] for e in execs[:3]] or ["No relevant exec moves today."],
        "defense_signals": [],
        "fintech_signals": [],
        "martech_signals": [],
        "vehicles":        ["FPP and GoFractional remain primary distribution channels."],
        "competitive":     ["Monitor fractional HR market for positioning updates."],
        "market_trends":   ["Series A/B hiring remains elevated; fractional talent demand is up."],
    }


# ─── EXPORT FUNCTIONS ────────────────────────────────────────────────────────

def stage_to_arr(stage):
    return {
        "Pre-Seed": "$0-500K ARR", "Seed": "$0-2M ARR", "Series A": "$1-5M ARR",
        "Series B": "$5-20M ARR", "Series C": "$20-50M ARR", "Late Stage": "$50M+ ARR",
    }.get(stage, "Early stage")


def export_hubspot_csv(funded, intel, lead_profiles=None):
    """Export HubSpot-ready CSV — one row per contact (Apollo-enriched)."""
    path = "docs/hubspot_leads.csv"
    rows = []
    fieldnames = [
        "First Name", "Last Name", "Title", "Email", "Phone", "LinkedIn URL",
        "Company Name", "Company Website", "Company LinkedIn",
        "Funding Amount", "Stage", "Estimated ARR", "Employee Count",
        "Vertical", "Signal Type", "Why Contact Now", "Date",
    ]

    if lead_profiles:
        for profile in lead_profiles:
            company_base = {
                "Company Name":    profile.get("name", ""),
                "Company Website": profile.get("website", ""),
                "Company LinkedIn": profile.get("linkedin_company", ""),
                "Funding Amount":  profile["funding"].get("latest_amount", ""),
                "Stage":           profile["funding"].get("stage", ""),
                "Estimated ARR":   stage_to_arr(profile["funding"].get("stage", "")),
                "Employee Count":  profile.get("employee_count", ""),
                "Vertical":        profile.get("vertical", ""),
                "Why Contact Now": profile.get("outreach_angle", "Recently funded."),
                "Date":            TODAY,
            }
            dms = profile.get("decision_makers", [])
            if dms:
                for dm in dms:
                    name_parts = dm.get("name", "").split(" ", 1)
                    rows.append({
                        "First Name":  name_parts[0] if name_parts else "",
                        "Last Name":   name_parts[1] if len(name_parts) > 1 else "",
                        "Title":       dm.get("title", ""),
                        "Email":       dm.get("email", ""),
                        "Phone":       dm.get("phone", ""),
                        "LinkedIn URL": dm.get("linkedin_url", ""),
                        "Signal Type": "Apollo Contact",
                        **company_base,
                    })
            else:
                rows.append({
                    "First Name": "", "Last Name": "", "Title": "",
                    "Email": "", "Phone": "", "LinkedIn URL": "",
                    "Signal Type": "Funded Target (no contacts yet)",
                    **company_base,
                })

    if not rows:
        for item in intel.get("new_money", []):
            if is_large_company(item.get("company", "")): continue
            rows.append({
                "First Name": "", "Last Name": "", "Title": "",
                "Email": "", "Phone": "", "LinkedIn URL": "",
                "Company Name":    item.get("company", ""),
                "Company Website": "", "Company LinkedIn": "",
                "Funding Amount":  item.get("amount", ""),
                "Stage":           item.get("stage", ""),
                "Estimated ARR":   stage_to_arr(item.get("stage", "")),
                "Employee Count":  "",
                "Vertical":        item.get("sector_tag", ""),
                "Signal Type":     "Funded Target",
                "Why Contact Now": item.get("why_now", "Recently funded."),
                "Date":            TODAY,
            })

    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        csv.DictWriter(f, fieldnames=fieldnames).writerows(rows)

    contacts_with_email = sum(1 for r in rows if r.get("Email"))
    print(f"  HubSpot CSV: {path} ({len(rows)} rows, {contacts_with_email} with Apollo emails)")


def build_data_json(intel, funded, hiring, execs, competitors, sam_signals=None,
                     yc_companies=None, hn_hiring=None, github_trending=None,
                     sec_filings=None, ph_launches=None, cross_references=None,
                     source_stats=None, stage_breakdown=None, sector_breakdown=None,
                     fintech=None, martech=None, gov_contracts=None, lead_profiles=None):
    return {
        "generated":      TODAY,
        "generated_slug": TODAY_SLUG,
        # Synthesized intel
        "moves_today":    intel.get("moves_today", []),
        "top_3":          intel.get("top_3", []),
        "new_money":      intel.get("new_money", []),
        "hiring_signals": intel.get("hiring_signals", []),
        "exec_moves":     intel.get("exec_moves", []),
        "defense_signals": intel.get("defense_signals", []),
        "fintech_signals": intel.get("fintech_signals", []),
        "martech_signals": intel.get("martech_signals", []),
        "vehicles":       intel.get("vehicles", []),
        "competitive":    intel.get("competitive", []),
        "market_trends":  intel.get("market_trends", []),
        # Raw data
        "raw_funded":     funded,
        "raw_hiring":     hiring,
        "raw_execs":      execs,
        "raw_competitors": competitors,
        "raw_sam":        sam_signals or [],
        "raw_fintech":    fintech or [],
        "raw_martech":    martech or [],
        "raw_gov_contracts": gov_contracts or [],
        # v4.0 sources
        "yc_companies":   yc_companies or [],
        "hn_hiring":      hn_hiring or [],
        "github_trending": github_trending or [],
        "sec_filings":    sec_filings or [],
        "ph_launches":    ph_launches or [],
        # v5.0 enriched leads
        "lead_profiles":  lead_profiles or [],
        # Analytics
        "cross_references": cross_references or [],
        "source_stats":   source_stats or {},
        "stage_breakdown": stage_breakdown or {},
        "sector_breakdown": sector_breakdown or {},
        # Meta
        "pdf_url":        "daily_brief.pdf",
        "apollo_enabled": bool(APOLLO_API_KEY),
        "leads_with_contacts": sum(1 for p in (lead_profiles or []) if p.get("decision_makers")),
    }


def generate_pdf(data, intel, charts=None, lead_profiles=None):
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("report.html")
    html_content = template.render(
        today=TODAY,
        data=data,
        intel=intel,
        charts=charts or [],
        lead_profiles=lead_profiles or [],
    )
    out_path = "docs/daily_brief.pdf"
    WeasyHTML(string=html_content, base_url=".").write_pdf(out_path)
    print(f"  PDF written: {out_path}")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"REBEL TALENT SYSTEMS - Daily Intel Brief v5.0")
    print(f"  {TODAY}")
    print(f"{'='*60}\n")

    os.makedirs("docs", exist_ok=True)

    # ── SIGNAL COLLECTION ──────────────────────────────────────────────────
    funded       = pull_funding_signals()
    fintech      = pull_fintech_signals()
    martech      = pull_martech_signals()
    hiring       = pull_hiring_signals()
    execs        = pull_exec_moves()
    competitors  = pull_competitor_intel()
    headlines    = pull_news_headlines()
    sam_signals  = pull_sam_gov()
    gov_contracts = pull_gov_contract_signals()

    # ── v4.0 SOURCES ───────────────────────────────────────────────────────
    yc_companies    = pull_yc_companies()
    hn_hiring       = pull_hn_hiring()
    github_trending = pull_github_trending()
    sec_filings     = pull_sec_edgar()
    ph_launches     = pull_producthunt()

    # ── CROSS-REFERENCE ENGINE ─────────────────────────────────────────────
    cross_references = build_cross_references(
        funded, yc_companies, hn_hiring, github_trending,
        sec_filings, ph_launches, fintech, martech
    )

    # ── YC ENRICHMENT ─────────────────────────────────────────────────────
    funded = enrich_funded_with_yc(funded, yc_companies)

    # ── APOLLO ENRICHMENT (v5.0) ───────────────────────────────────────────
    apollo_targets = []
    seen_names = set()
    for xr in cross_references[:5]:
        n = xr["company"]
        apollo_targets.append({"name": n, "company": n})
        seen_names.add(n.lower())
    for f in (funded + fintech + martech)[:10]:
        n = f["name"]
        if n.lower() not in seen_names:
            apollo_targets.append({"name": n, "company": n})
            seen_names.add(n.lower())

    apollo_enriched = enrich_targets_with_apollo(apollo_targets)

    # ── LEAD PROFILES (v5.0) ───────────────────────────────────────────────
    lead_profiles = build_lead_profiles(
        funded, cross_references, apollo_enriched,
        hn_hiring, execs, fintech, martech
    )

    # ── GROQ SYNTHESIS ─────────────────────────────────────────────────────
    intel = synthesize_with_groq(
        funded, hiring, execs, headlines, sam_signals,
        yc_companies, cross_references, fintech, martech, lead_profiles
    )

    # ── ANALYTICS ──────────────────────────────────────────────────────────
    source_stats    = build_source_stats(funded, hiring, execs, sam_signals,
                                          yc_companies, hn_hiring, github_trending,
                                          sec_filings, ph_launches, fintech, martech, gov_contracts)
    stage_breakdown = build_stage_breakdown(funded, intel, fintech, martech)
    sector_breakdown = build_sector_breakdown(funded, yc_companies, fintech, martech)

    # ── BUILD DATA JSON ────────────────────────────────────────────────────
    data = build_data_json(
        intel, funded, hiring, execs, competitors, sam_signals,
        yc_companies, hn_hiring, github_trending,
        sec_filings, ph_launches, cross_references,
        source_stats, stage_breakdown, sector_breakdown,
        fintech, martech, gov_contracts, lead_profiles,
    )

    with open("docs/data.json", "w") as f:
        json.dump(data, f, indent=2)
    print("  data.json written: docs/data.json")

    export_hubspot_csv(funded, intel, lead_profiles)

    # ── PDF GENERATION ─────────────────────────────────────────────────────
    charts = []
    try:
        charts = generate_pdf_charts(data)
    except Exception as e:
        print(f"  [WARN] Chart generation failed: {e}")

    try:
        generate_pdf(data, intel, charts, lead_profiles)
    except Exception as e:
        print(f"  [WARN] PDF generation failed: {e}")

    # ── SUMMARY ────────────────────────────────────────────────────────────
    total_signals   = sum(source_stats.values())
    active_sources  = sum(1 for v in source_stats.values() if v > 0)
    contacts_found  = sum(len(p.get("decision_makers", [])) for p in lead_profiles)

    print(f"\n{'='*60}")
    print(f"  Brief complete — {TODAY} — v5.0")
    print(f"  {active_sources} active sources | {total_signals} total signals")
    print(f"  Funded: {len(funded)} | Fintech: {len(fintech)} | Martech: {len(martech)}")
    print(f"  Hiring: {len(hiring)} | Execs: {len(execs)} | SBIR: {len(sam_signals)}")
    print(f"  YC: {len(yc_companies)} | HN: {len(hn_hiring)} | GitHub: {len(github_trending)}")
    print(f"  Cross-referenced: {len(cross_references)} companies in 2+ sources")
    print(f"  Lead profiles: {len(lead_profiles)} | Apollo contacts: {contacts_found}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
