#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║  FORTRESS SNIPER — Cloud Fetcher v2.3                              ║
║  Runs headless on GitHub Actions (no GUI, no Tkinter)              ║
║                                                                      ║
║  All config via GitHub Secrets (environment variables):             ║
║    GOOGLE_CREDS_JSON   — full contents of credentials.json         ║
║    GOOGLE_SHEET_ID     — your spreadsheet ID                       ║
║    TELEGRAM_BOT_TOKEN  — from @BotFather                           ║
║    TELEGRAM_CHAT_ID    — your chat/group ID                        ║
║    GITHUB_REPOSITORY   — set automatically by GitHub Actions       ║
║    GITHUB_RUN_ID       — set automatically by GitHub Actions       ║
║                                                                      ║
║  v2.5 changes vs v2.4 (Akamai-confirmed, scoped Playwright fix):    ║
║    - CONFIRMED (not inferred): corporates-pit sets bm_sz/bm_sv —    ║
║      Akamai Bot Manager cookies — on its stub/timeout responses.    ║
║      corporate-announcements now succeeds under the same WAF, so    ║
║      only corporates-pit gets the browser-based fix below.          ║
║    - ADDED: harvest_akamai_cookies() launches a stealth-patched     ║
║      headless Chromium via Playwright ONCE per run, before the      ║
║      Insider chunk loop, to let Akamai's sensor JS actually run     ║
║      and earn legitimate bm_sz/bm_sv/ak_bmsc cookies                ║
║    - Harvested cookies are injected into the existing curl_cffi     ║
║      session; the real API call still goes through curl_cffi        ║
║      (fast) — Playwright is a cookie source, not a scraper          ║
║    - Every other endpoint (Bhavcopy/FII-DII/Filings/Earnings)       ║
║      is untouched — curl_cffi-only, since curl_cffi already works   ║
║      for all of them, including Filings as of the v2.4 fix          ║
║    - Fully optional dependency: if Playwright/Chromium isn't        ║
║      installed, or the harvest fails for any reason, this is        ║
║      logged and the script falls back to the plain curl_cffi        ║
║      attempt exactly as it worked in v2.4 — never fatal             ║
║                                                                      ║
║  v2.4 changes vs v2.3 (Insider/Filings timeout hybrid fix):        ║
║    - Insider + Filings (corporates-pit / corporate-announcements)  ║
║      now run FIRST in main(), right after shared warmup, so they   ║
║      get the freshest session and least prior traffic in the run  ║
║    - Added per-path throttle (6s) for these two endpoints only,    ║
║      independent of the global 2.5s gap — full-duration hangs      ║
║      that clear on retry look like a path-specific tarpit, not a   ║
║      client-fingerprint block                                      ║
║    - Added section-page warmup (the actual companies-listing page ║
║      a browser would visit) before calling these two endpoints,    ║
║      not just the generic homepage/market-data warmup              ║
║    - Every NSE response now logs diagnostic headers (Server,       ║
║      cf-ray, X-RateLimit-*, Retry-After, Set-Cookie, etc.) so a    ║
║      future timeout/stub carries evidence of what's actually       ║
║      fronting the endpoint, instead of just a guess                ║
║    - BUGFIX: fetch_filings used UTC datetime.today() instead of    ║
║      IST — same class of bug already fixed in fetch_insider         ║
║                                                                      ║
║  v2.3 changes vs v2.2:                                             ║
║    - BUGFIX: sell filter was "sell" only — "market sale", "sale"   ║
║      slipped through. Now SKIP_TYPES whitelist-rejects any type    ║
║      not in the genuine-buy list (market purchase, esop exercise,  ║
║      preferential allotment, rights, open offer)                   ║
║    - ADDED: TYPE_LABEL maps raw acqMode to clean display label     ║
║    - ADDED: KEEP_TYPES allowlist — only genuine buy signals kept:  ║
║      market purchase · esop (exercise only) · preferential offer   ║
║      rights · open offer · employee benefit · esos                 ║
║    - All other types (sale, pledge, gift, off market, transfer,    ║
║      amalgamation, inter-se) are skipped and logged                ║
║  v2.2 changes vs v2.1:                                             ║
║    - BUGFIX: intimDt used for date filter, not acqfromDt           ║
║    - BUGFIX: IST timezone used, not UTC datetime.today()           ║
║    - BUGFIX: timezone-safe cutoff comparison                       ║
║  v2.1 changes vs v2.0:                                             ║
║    - Full traceback logged for every exception, everywhere         ║
║    - No silent except:/continue blocks — every skip is logged      ║
║    - NSE session warmup failures are logged with status codes      ║
║    - _nse_json HTML check fixed (was wrapped in broken try/except) ║
║    - Telegram error messages include GitHub Actions run URL        ║
║    - Error strings no longer truncated to 60 chars                 ║
║    - push_df logs quota/auth errors with full detail               ║
║    - Config validation at startup with clear missing-secret hints  ║
║    - Bhavcopy file attempt log shows full URL and failure reason   ║
║    - FII/DII field resolution logs what key was actually found     ║
║    - Insider skipped rows logged individually at DEBUG level       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os, sys, io, time, json, zlib, traceback, html as _html
from datetime import datetime, timedelta, date
from collections import defaultdict

try:
    import requests
    import pandas as pd
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError as e:
    print(f"[FATAL] Missing dependency: {e}", flush=True)
    print("Run: pip install requests pandas gspread google-auth", flush=True)
    sys.exit(1)

# ── Optional: curl_cffi gives us a real browser TLS/JA3 fingerprint.
# NSE's bot-management started tarpitting/blocking plain `requests` sessions
# (distinguishable purely at the TLS handshake level, regardless of headers).
# If curl_cffi isn't installed we transparently fall back to plain `requests`
# with rotated header sets — degraded, but the script still runs.
try:
    from curl_cffi import requests as cf_requests
    from curl_cffi.requests.exceptions import (
        Timeout as _CfTimeout,
        ConnectionError as _CfConnectionError,
        RequestException as _CfRequestException,
    )
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    cf_requests = None
    _CfTimeout = _CfConnectionError = _CfRequestException = ()

# ── Optional: Playwright, used ONLY to harvest Akamai Bot Manager cookies
# (bm_sz/bm_sv/ak_bmsc) for the one endpoint (corporates-pit) that curl_cffi
# cannot get past regardless of TLS fingerprint — confirmed by the bm_sz/bm_sv
# cookies Akamai sets on the stubbed/timed-out responses. A real Chromium runs
# Akamai's sensor JS and earns legitimate cookies; those get handed to the
# existing curl_cffi session, which then makes the actual API call as normal.
# Every other endpoint keeps using curl_cffi only — this is deliberately not
# a wholesale migration. If Playwright/Chromium isn't installed (or the
# harvest fails for any reason), the script logs it and falls back to the
# plain curl_cffi attempt exactly as before — never fatal.
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    sync_playwright = None

# Unified exception tuples so every except-clause below catches failures from
# EITHER backend (plain requests or curl_cffi), whichever is active.
_TIMEOUT_EXCS = (requests.exceptions.Timeout,) + ((_CfTimeout,) if HAS_CURL_CFFI else ())
_CONN_EXCS    = (requests.exceptions.ConnectionError,) + ((_CfConnectionError,) if HAS_CURL_CFFI else ())
_REQ_EXCS     = (requests.exceptions.RequestException,) + ((_CfRequestException,) if HAS_CURL_CFFI else ())

# Rotation pool of browser fingerprints to impersonate (curl_cffi) — cycled
# across session creations so a block/tarpit on one fingerprint doesn't sink
# every fetch in the run. Mix of Chrome/Edge/Firefox/Safari, all recent.
NSE_IMPERSONATE_PROFILES = [
    "chrome146", "chrome142", "chrome136", "edge101", "safari184", "firefox147",
]

# Rotation pool of plain-requests header sets, used only when curl_cffi is
# unavailable. Varies UA, sec-ch-ua, and Accept-Encoding together so each
# session at least looks like a *consistent* browser rather than a mix.
_PLAIN_HEADER_SETS = [
    {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Accept-Encoding": "gzip, deflate, br",
    },
    {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                       "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"),
        "sec-ch-ua-platform": '"macOS"',
        "Accept-Encoding": "gzip, deflate, br",
    },
    {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"),
        "sec-ch-ua": '"Microsoft Edge";v="126", "Chromium";v="126", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Accept-Encoding": "gzip, deflate, br",
    },
    {
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0"),
        "Accept-Encoding": "gzip, deflate, br",
    },
]

_session_rotation = {"n": 0}

def _next_fingerprint():
    """Returns (backend_label, profile_or_headers) and advances the rotation counter."""
    i = _session_rotation["n"]
    _session_rotation["n"] += 1
    if HAS_CURL_CFFI:
        return "curl_cffi", NSE_IMPERSONATE_PROFILES[i % len(NSE_IMPERSONATE_PROFILES)]
    return "requests", _PLAIN_HEADER_SETS[i % len(_PLAIN_HEADER_SETS)]

# ── Global cross-call throttle ───────────────────────────────────────
# NSE appears to rate-limit / tarpit based on request VOLUME from an IP over
# a short window, not just per-endpoint. Observed across two runs: whichever
# endpoint happened to fire during a burst is the one that stalls (Insider
# one run, Filings the next) — not always the same one. Every fetch_*()
# creating its own session (2 warmup hits each) plus per-chunk rotations
# adds up to a burst of 10+ requests in a couple of minutes. This enforces a
# minimum gap between ANY request this script sends to nseindia.com,
# regardless of which function is calling.
_MIN_REQUEST_GAP_SEC = 2.5
_last_request_ts = {"t": 0.0}

# ── Per-path throttle ─────────────────────────────────────────────────
# corporates-pit and corporate-announcements consistently hang for the
# FULL timeout duration (not a fast 403/redirect) while every other NSE
# endpoint hit in the same run, same session, responds fast. A fast
# JS-challenge/bot-check normally answers quickly (small HTML/JS payload
# or an instant 403) — it doesn't sit on the connection. A slow/silent
# hang that clears on a *different* attempt is the signature of a
# path-specific rate-limiter/tarpit sitting in front of just these two
# endpoints. So in addition to the global gap above, these paths get
# their own longer minimum spacing, independent of what else the script
# has been doing.
_PATH_THROTTLE_GAP_SEC = {
    "/api/corporates-pit": 6.0,
    "/api/corporate-announcements": 6.0,
}
_last_path_request_ts: dict = {}

def _throttle(path_key: str = None):
    now = time.time()
    wait = _last_request_ts["t"] + _MIN_REQUEST_GAP_SEC - now
    if path_key and path_key in _PATH_THROTTLE_GAP_SEC:
        gap = _PATH_THROTTLE_GAP_SEC[path_key]
        last = _last_path_request_ts.get(path_key, 0.0)
        path_wait = last + gap - now
        wait = max(wait, path_wait)
    if wait > 0:
        time.sleep(wait)
    now2 = time.time()
    _last_request_ts["t"] = now2
    if path_key:
        _last_path_request_ts[path_key] = now2


def _diag_headers(resp) -> str:
    """Pull out the headers that would reveal a WAF/bot-management product
    fronting this endpoint (Akamai, Cloudflare, custom rate-limiter, etc.),
    so a timeout/small-payload log line carries actual diagnostic evidence
    instead of just a guess. Logged at DBG level on every response."""
    try:
        h = resp.headers
    except Exception:
        return "(no headers available)"
    interesting = [
        "Server", "Via", "X-Cache", "X-Akamai-Transformed", "X-Akamai-Request-ID",
        "cf-ray", "cf-cache-status", "X-RateLimit-Limit", "X-RateLimit-Remaining",
        "X-RateLimit-Reset", "Retry-After", "X-Request-ID", "X-Frame-Options",
    ]
    found = {k: h[k] for k in interesting if k in h}
    cookies = list(getattr(resp, "cookies", {}) or {})
    return f"headers={found or '(none of interest)'} cookies_set={cookies}"


# ══════════════════════════════════════════════════════════════════════
# CONFIG — all from environment variables / GitHub Secrets
# ══════════════════════════════════════════════════════════════════════

GOOGLE_CREDS_JSON  = os.environ.get("GOOGLE_CREDS_JSON",  "")
GOOGLE_SHEET_ID    = os.environ.get("GOOGLE_SHEET_ID",    "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# Auto-set by GitHub Actions — used to build the run log URL in Telegram messages
GH_REPO   = os.environ.get("GITHUB_REPOSITORY", "")
GH_RUN_ID = os.environ.get("GITHUB_RUN_ID", "")

SHEET_BHAVCOPY = "BHAVCOPY"
SHEET_FII_DII  = "FII_DII"
SHEET_INSIDER  = "INSIDER"
SHEET_FILINGS  = "FILINGS"
SHEET_EARNINGS = "EARNINGS"

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ══════════════════════════════════════════════════════════════════════
# LOGGING — every function uses these, never bare print()
# ══════════════════════════════════════════════════════════════════════

def log(msg: str, level: str = "INFO"):
    ts     = datetime.utcnow().strftime("%H:%M:%S")
    prefix = {"INFO":"   ", "WARN":"⚠️ ", "ERR":"❌ ", "OK":"✅ ", "DBG":"🔍 "}.get(level, "   ")
    print(f"[{ts}] {prefix}{msg}", flush=True)

def log_tb(label: str, exc: Exception):
    """Log exception type, message, AND full traceback. Called in every except block."""
    log(f"{label}: {type(exc).__name__}: {exc}", "ERR")
    for line in traceback.format_exc().strip().splitlines():
        print(f"           {line}", flush=True)


# ══════════════════════════════════════════════════════════════════════
# STARTUP CONFIG VALIDATION
# ══════════════════════════════════════════════════════════════════════

def validate_config():
    """Check all required secrets are present and the credentials JSON is valid."""
    missing = []
    if not GOOGLE_CREDS_JSON:
        missing.append("GOOGLE_CREDS_JSON  ← paste full credentials.json text")
    if not GOOGLE_SHEET_ID:
        missing.append("GOOGLE_SHEET_ID    ← your spreadsheet ID")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN ← from @BotFather")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID   ← use @userinfobot to find yours")
    if missing:
        log("Missing required GitHub Secrets — add them at:", "ERR")
        log("  Repo → Settings → Secrets and variables → Actions → New repository secret", "ERR")
        for m in missing:
            log(f"  • {m}", "ERR")
        sys.exit(1)

    try:
        creds = json.loads(GOOGLE_CREDS_JSON)
    except json.JSONDecodeError as exc:
        log_tb("GOOGLE_CREDS_JSON is not valid JSON — paste the full file, not just part of it", exc)
        sys.exit(1)

    required_keys = ["type", "project_id", "private_key", "client_email"]
    missing_keys  = [k for k in required_keys if k not in creds]
    if missing_keys:
        log(f"GOOGLE_CREDS_JSON is missing fields: {missing_keys}", "ERR")
        log("Make sure you pasted the ENTIRE contents of credentials.json", "ERR")
        sys.exit(1)

    log(f"Config OK — service account: {creds.get('client_email', '?')}", "OK")
    log(f"Sheet ID: {GOOGLE_SHEET_ID}", "DBG")
    log(f"Telegram chat: {TELEGRAM_CHAT_ID}", "DBG")


# ══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════════

def _actions_url() -> str:
    if GH_REPO and GH_RUN_ID:
        return f"https://github.com/{GH_REPO}/actions/runs/{GH_RUN_ID}"
    return ""

def _esc(s) -> str:
    """HTML-escape a plain-text value for safe embedding in Telegram HTML mode."""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

def _tg_link(url: str, label: str) -> str:
    """Return a Telegram HTML anchor, or empty string if url is empty."""
    if not url:
        return ""
    return f'<a href="{url}">{_esc(label)}</a>'

def _tg_bold(s) -> str:
    return f"<b>{_esc(s)}</b>"

def _tg_code(s) -> str:
    return f"<code>{_esc(s)}</code>"

def send_telegram(text: str):
    """Send a Telegram message.  Never raises — logs failures instead.

    Callers MUST use _tg_bold(), _tg_code(), _tg_link(), and _esc() for any
    user/exception content embedded in the message.  send_telegram() sends text
    verbatim with parse_mode=HTML — it does NOT escape anything itself.
    Every raw string from NSE/exceptions must be wrapped in _esc() at the call
    site before being inserted into the f-string.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured — skipping", "WARN")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=20
        )
        if resp.status_code == 200:
            log("Telegram notification sent", "OK")
        else:
            log(f"Telegram HTTP {resp.status_code}: {resp.text[:300]}", "WARN")
    except Exception as exc:
        log_tb("Telegram send error (notification lost)", exc)


# ══════════════════════════════════════════════════════════════════════
# NSE SESSION
# ══════════════════════════════════════════════════════════════════════

def nse_session(timeout: int = 30):
    """
    Warmed-up session for NSE — curl_cffi (real browser TLS/JA3 fingerprint)
    when available, else plain requests with rotated browser-like headers.

    Each call advances the fingerprint rotation, so successive fetch_*()
    calls in the same run each get a different "browser" — if NSE's bot
    management is tarpitting one fingerprint, the next call gets a fresh one
    instead of repeating the same failure all run.

    Warmup failures are LOGGED (with status code) but never abort the run —
    the session is still returned so the actual API calls can try.
    """
    backend, fp = _next_fingerprint()

    common_headers = {
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "DNT":             "1",
        "Connection":      "keep-alive",
    }

    if backend == "curl_cffi":
        s = cf_requests.Session(impersonate=fp)
        s.headers.update(common_headers)
        log(f"NSE session: curl_cffi impersonate={fp}", "DBG")
    else:
        s = requests.Session()
        s.headers.update(common_headers)
        s.headers.update(fp)
        log("NSE session: plain requests fallback (pip install curl_cffi for a real "
            "browser TLS fingerprint — recommended, NSE bot-detection tightened)", "WARN")

    for url in ["https://www.nseindia.com",
                "https://www.nseindia.com/market-data/live-equity-market"]:
        try:
            _throttle()
            r = s.get(url, timeout=timeout)
            log(f"NSE warmup {url[-35:]} → HTTP {r.status_code} ({len(r.content)}B)", "DBG")
            time.sleep(1.5)
        except _TIMEOUT_EXCS:
            log(f"NSE warmup TIMEOUT for {url} — continuing anyway", "WARN")
        except _CONN_EXCS as exc:
            log(f"NSE warmup CONNECTION ERROR for {url}: {exc} — continuing anyway", "WARN")
        except Exception as exc:
            log_tb(f"NSE warmup unexpected error for {url}", exc)
    return s


# ── Akamai cookie harvester (Playwright) ────────────────────────────────
# Scoped narrowly to corporates-pit: everything else in this script keeps
# using curl_cffi only, because everything else works with curl_cffi only.
_AKAMAI_COOKIE_NAMES = {"bm_sz", "bm_sv", "ak_bmsc", "bm_mi", "_abck"}

def harvest_akamai_cookies(page_url: str, wait_seconds: float = 12.0):
    """
    Load `page_url` in a real (stealth-patched) headless Chromium so Akamai
    Bot Manager's sensor JS actually runs and issues legitimate bm_sz/bm_sv/
    ak_bmsc cookies, then return those cookies as a dict.

    Returns None (never raises) if Playwright isn't installed or the harvest
    fails for any reason — callers must treat that as "couldn't get a boost,
    fall back to the plain curl_cffi attempt", not as a fatal error.
    """
    if not HAS_PLAYWRIGHT:
        log("Playwright not installed — skipping Akamai cookie harvest, "
            "falling back to curl_cffi-only for this endpoint", "WARN")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="Asia/Kolkata",
            )
            # Patch the most common headless tells Akamai's sensor script
            # checks for. Not bulletproof, but removes the cheapest signals.
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                window.chrome = { runtime: {} };
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (params) => (
                    params.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(params)
                );
            """)
            page = context.new_page()
            log(f"Playwright: loading {page_url} to earn Akamai cookies...", "DBG")
            page.goto(page_url, timeout=int(wait_seconds * 1000) + 15000, wait_until="domcontentloaded")
            # Akamai's sensor payload posts asynchronously after load — give
            # it real wall-clock time rather than trusting networkidle, which
            # can fire before the sensor beacon completes.
            page.wait_for_timeout(int(wait_seconds * 1000))

            cookies = context.cookies()
            browser.close()

        cookie_dict = {c["name"]: c["value"] for c in cookies}
        akamai_found = _AKAMAI_COOKIE_NAMES & cookie_dict.keys()
        if akamai_found:
            log(f"Playwright harvested {len(akamai_found)} Akamai cookie(s): "
                f"{sorted(akamai_found)}", "OK")
        else:
            log("Playwright completed but no Akamai cookies were set — "
                "site may not be challenging this session, or the challenge "
                "didn't fire in time", "WARN")
        return cookie_dict

    except Exception as exc:
        log_tb("Playwright Akamai cookie harvest failed — falling back to curl_cffi-only", exc)
        return None


def _inject_cookies_into_session(sess, cookie_dict: dict, domain: str = ".nseindia.com"):
    """Copy harvested browser cookies into the curl_cffi/requests session that
    will make the actual API call, so that call rides on a real, JS-verified
    Akamai session instead of curl_cffi's fingerprint alone."""
    if not cookie_dict:
        return 0
    injected = 0
    for name, value in cookie_dict.items():
        try:
            sess.cookies.set(name, value, domain=domain)
            injected += 1
        except Exception as exc:
            log(f"Could not inject cookie '{name}': {exc}", "WARN")
    log(f"Injected {injected}/{len(cookie_dict)} harvested cookie(s) into NSE session", "DBG")
    return injected


def _extract_json_from_binary(raw_bytes: bytes):
    """Last-resort JSON extraction from mangled/compressed responses."""
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            text  = raw_bytes.decode(enc).lstrip("\ufeff").lstrip("\x00").lstrip()
            start = text.find("{")
            if start == -1:
                start = text.find("[")
            if start != -1:
                brace = text[start]
                close = "}" if brace == "{" else "]"
                count = 0
                for i, ch in enumerate(text[start:], start):
                    if ch == brace:   count += 1
                    elif ch == close:
                        count -= 1
                        if count == 0:
                            return json.loads(text[start:i + 1])
            return json.loads(text)
        except Exception:
            continue
    try:
        return _extract_json_from_binary(zlib.decompress(raw_bytes, 16 + zlib.MAX_WBITS))
    except Exception:
        pass
    raise ValueError(
        f"Could not extract JSON from response — "
        f"first 120 bytes: {raw_bytes[:120]!r}"
    )


def _nse_json(sess, url: str, params=None, timeout: int = 25, referer: str = None,
              _retry: int = 2, min_bytes: int = 0, session_factory=None, _rounds: int = 2,
              path_key: str = None, section_warmup: str = None):
    """
    GET a NSE endpoint and return parsed JSON.
    Raises ValueError with FULL context on any failure.

    Two layers of resilience:
      1. Within a "round": retries up to _retry times on timeout / 429 / 502/503,
         with growing backoff (same session/fingerprint — handles transient
         rate-limiting).
      2. Across "rounds": if a whole round fails (exhausted retries, empty body,
         HTML-instead-of-JSON, or a suspiciously small payload under `min_bytes`),
         and `session_factory` was given, get a BRAND NEW session — a different
         browser fingerprint / header set via the rotation pool — and try again.
         This is what actually gets past a tarpit/block tied to one fingerprint,
         as opposed to just re-hitting the same wall harder.

    `min_bytes`: if set, a response smaller than this is treated as "likely
    blocked" even if it's technically valid JSON (e.g. NSE sometimes returns a
    trivial `{}`/`[]`/error stub under bot detection instead of a real 403).
    Leave at 0 for endpoints where a small-but-real response is normal.

    `path_key`: identifies the URL path for per-path throttling (see
    _PATH_THROTTLE_GAP_SEC). Auto-derived from `url` if not given.

    `section_warmup`: an optional NSE *section page* URL (e.g. the actual
    companies-listing page a browser would visit before calling this API)
    to hit once at the start of each round, on the current session, before
    the real API call. The existing session warmup only visits the
    homepage + the market-data page — never the section page the referer
    header claims to come from — so this fills that gap cheaply for the
    endpoints where it matters, in case NSE ties a cookie to having
    actually visited that section.
    """
    req_headers = {}
    if referer:
        req_headers["Referer"]          = referer
        req_headers["X-Requested-With"] = "XMLHttpRequest"

    short_url = url.replace('https://www.nseindia.com', '').replace('https://nsearchives.nseindia.com', '[arch]')[:80]
    log(f"NSE GET {short_url}", "DBG")

    if path_key is None:
        path_key = short_url.split("?")[0]

    current_sess = sess
    last_err: Exception = ValueError(f"NSE {url}: no attempts were made")

    for round_num in range(1, _rounds + 1):
        round_failed = False

        if section_warmup:
            try:
                _throttle(path_key)
                wr = current_sess.get(section_warmup, timeout=timeout)
                log(f"NSE section warmup {section_warmup[-45:]} → HTTP {wr.status_code} "
                    f"({len(wr.content)}B) | {_diag_headers(wr)}", "DBG")
                time.sleep(1.0)
            except Exception as exc:
                log(f"NSE section warmup failed for {section_warmup}: {exc} — continuing anyway", "WARN")

        for attempt in range(1, _retry + 1):
            try:
                _throttle(path_key)
                resp = current_sess.get(url, params=params, timeout=timeout, headers=req_headers)
            except _TIMEOUT_EXCS:
                if attempt < _retry:
                    wait = 8 * attempt
                    log(f"TIMEOUT — backing off {wait}s before retry {attempt}/{_retry - 1} "
                        f"(round {round_num}/{_rounds})...", "WARN")
                    time.sleep(wait)
                    continue
                last_err = ValueError(f"NSE request timed out after {timeout}s\n  URL: {url}")
                round_failed = True
                break
            except _CONN_EXCS as exc:
                last_err = ValueError(f"NSE connection error: {exc}\n  URL: {url}")
                round_failed = True
                break
            except _REQ_EXCS as exc:
                last_err = ValueError(f"NSE request failed ({type(exc).__name__}): {exc}\n  URL: {url}")
                round_failed = True
                break
            except Exception as exc:
                last_err = ValueError(f"NSE request failed ({type(exc).__name__}): {exc}\n  URL: {url}")
                round_failed = True
                break

            raw = resp.content
            log(f"NSE response: HTTP {resp.status_code}, {len(raw)} bytes "
                f"(round {round_num}/{_rounds}, attempt {attempt}/{_retry}) | {_diag_headers(resp)}", "DBG")

            if resp.status_code in (429, 502, 503) and attempt < _retry:
                wait = 6 * attempt
                log(f"HTTP {resp.status_code} — backing off {wait}s before retry "
                    f"{attempt}/{_retry - 1}...", "WARN")
                time.sleep(wait)
                continue

            if not raw or len(raw) < 10:
                last_err = ValueError(
                    f"NSE returned EMPTY body (HTTP {resp.status_code})\n"
                    f"  URL: {url}\n"
                    f"  This usually means NSE blocked the request or the endpoint changed."
                )
                round_failed = True
                break

            # lstrip() handles NSE padding raw HTML with spaces/newlines before <!DOCTYPE
            raw_stripped = raw.lstrip()
            if raw_stripped[:1] == b"<" or raw_stripped[:9].lower() == b"<!doctype":
                preview = raw[:400].decode("utf-8", errors="ignore")
                last_err = ValueError(
                    f"NSE returned HTML instead of JSON (HTTP {resp.status_code})\n"
                    f"  URL: {url}\n"
                    f"  Cause: NSE rate-limit / IP block / endpoint moved\n"
                    f"  HTML preview: {preview[:250]}"
                )
                round_failed = True
                break

            if min_bytes and len(raw) < min_bytes:
                log(f"Response suspiciously small ({len(raw)}B < {min_bytes}B expected for "
                    f"this endpoint) — treating as a soft block", "WARN")
                last_err = ValueError(
                    f"NSE returned a suspiciously small payload ({len(raw)}B, HTTP "
                    f"{resp.status_code})\n  URL: {url}\n"
                    f"  Cause: likely bot-detection returning a stub instead of real data"
                )
                round_failed = True
                break

            try:
                return resp.json()
            except Exception:
                pass

            log("Standard JSON decode failed — attempting binary extraction", "WARN")
            try:
                return _extract_json_from_binary(raw)
            except Exception as exc:
                last_err = exc
                round_failed = True
                break

        if round_failed and round_num < _rounds and session_factory is not None:
            log(f"Round {round_num}/{_rounds} failed ({last_err}) — rotating session "
                f"fingerprint and retrying with a fresh one...", "WARN")
            try:
                current_sess = session_factory()
            except Exception as exc:
                log_tb("Failed to build rotated session — reusing previous one", exc)
            time.sleep(3)
            continue
        elif round_failed:
            break

    raise ValueError(
        f"NSE {url} failed after {_rounds} round(s) x {_retry} retries "
        f"(persistent rate-limit / fingerprint block / IP block).\n  Last error: {last_err}"
    )


# ══════════════════════════════════════════════════════════════════════
# TRADING DAY
# ══════════════════════════════════════════════════════════════════════

_NSE_HOLIDAYS = {
    date(2024, 1, 22), date(2024, 3, 25), date(2024, 3, 29), date(2024, 4, 14),
    date(2024, 4, 17), date(2024, 5, 23), date(2024, 6, 17), date(2024, 7, 17),
    date(2024, 8, 15), date(2024, 10, 2), date(2024, 11, 1), date(2024, 11, 15),
    date(2024, 11, 20), date(2024, 12, 25),
    date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31), date(2025, 4, 10),
    date(2025, 4, 14), date(2025, 4, 18), date(2025, 5, 1),  date(2025, 8, 15),
    date(2025, 8, 27), date(2025, 10, 2), date(2025, 10, 24), date(2025, 11, 5),
    date(2025, 12, 25),
    date(2026, 1, 26), date(2026, 3, 20), date(2026, 4, 3),  date(2026, 4, 14),
    date(2026, 6, 17),  # Bakri Id / Eid ul-Adha (approx — verify on NSE calendar)
    date(2026, 7, 6),   # Muharram (approx — verify on NSE calendar)
    date(2026, 8, 15),
    date(2026, 8, 25),  # Ganesh Chaturthi
    date(2026, 10, 2),
    date(2026, 10, 20), # Diwali - Laxmi Pujan
    date(2026, 10, 21), # Diwali Balipratipada
    date(2026, 11, 4),  # Guru Nanak Jayanti
    date(2026, 12, 25),
}

def get_last_trading_day():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    d = now_ist.date()
    if now_ist.hour < 15 or (now_ist.hour == 15 and now_ist.minute < 30):
        d -= timedelta(days=1)
        log(f"Market not closed yet ({now_ist.strftime('%H:%M')} IST) — using previous day", "DBG")
    for _ in range(14):
        if d.weekday() < 5 and d not in _NSE_HOLIDAYS:
            break
        d -= timedelta(days=1)
    else:
        raise RuntimeError(
            "Could not find a trading day in the last 14 calendar days. "
            "Check _NSE_HOLIDAYS — you may be missing a holiday entry that caused an infinite skip."
        )
    log(f"Last trading day: {d} (weekday={d.strftime('%A')})", "DBG")
    return d.strftime("%d%m%Y"), d.strftime("%Y-%m-%d")

def _month_abbr(mm: str) -> str:
    return {"01":"JAN","02":"FEB","03":"MAR","04":"APR","05":"MAY","06":"JUN",
            "07":"JUL","08":"AUG","09":"SEP","10":"OCT","11":"NOV","12":"DEC"}.get(mm, mm)


# ══════════════════════════════════════════════════════════════════════
# BHAVCOPY
# ══════════════════════════════════════════════════════════════════════

def _download_bhavcopy_file(date_str: str, warmed_sess: requests.Session = None) -> pd.DataFrame:
    dd, mm, yyyy = date_str[:2], date_str[2:4], date_str[4:]
    mon, yyyymmdd = _month_abbr(mm), f"{yyyy}{mm}{dd}"
    urls = [
        (f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip", True),
        (f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv",     False),
        (f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",            False),
        (f"https://archives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mon}/cm{date_str}bhav.csv.zip", True),
        (f"https://archives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mon}/cm{date_str}bhav.csv",     False),
    ]
    # Use the warmed session (with NSE cookies) if provided; otherwise build a cold one.
    # A cold session often gets 403 from nsearchives.nseindia.com.
    if warmed_sess:
        sess = warmed_sess
        # Ensure archive host headers are present
        sess.headers.setdefault("Referer", "https://www.nseindia.com/")
    else:
        sess = nse_session()  # rotated fingerprint, cookie-warmed — far more reliable than a cold requests.Session
        sess.headers.setdefault("Referer", "https://www.nseindia.com/")
        sess.headers.setdefault("Accept", "*/*")
    attempt_log = []

    for url, is_zip in urls:
        try:
            log(f"Trying {url[45:]}", "DBG")
            r = sess.get(url, timeout=30)
            note = f"HTTP {r.status_code} ({len(r.content)}B) — {url[45:]}"
            attempt_log.append(note)
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.BytesIO(r.content), compression="zip" if is_zip else None)
                df.columns = df.columns.str.strip()
                if len(df) > 100:
                    log(f"File downloaded: {len(df)} rows from {url[45:]}", "OK")
                    return df
                else:
                    log(f"File too small: only {len(df)} rows — skipping", "WARN")
                    attempt_log[-1] += f" [only {len(df)} rows — too small]"
            elif r.status_code == 404:
                log(f"404 — not published yet: {url[45:]}", "DBG")
            elif r.status_code in (401, 403):
                log(f"HTTP {r.status_code} — IP block or auth: {url[45:]}", "WARN")
            else:
                log(f"HTTP {r.status_code} — unexpected: {url[45:]}", "WARN")
        except _TIMEOUT_EXCS:
            msg = f"TIMEOUT — {url[45:]}"
            attempt_log.append(msg); log(msg, "WARN")
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc} — {url[45:]}"
            attempt_log.append(msg); log(msg, "WARN")
        time.sleep(0.5)

    raise Exception(
        f"All {len(urls)} bhavcopy file URLs failed for {date_str}.\n"
        f"Individual attempts:\n" + "\n".join(f"  {a}" for a in attempt_log) + "\n"
        f"Note: NSE archive files are usually published ~1h after market close (16:30 IST)."
    )


def _download_bhavcopy_api(sess=None) -> pd.DataFrame:
    sess = sess or nse_session()
    indices = [
        ("NIFTY%20TOTAL%20MARKET",    "NIFTY TOTAL MARKET"),
        ("NIFTY%20500",               "NIFTY 500"),
        ("NIFTY%20MIDCAP%20150",      "NIFTY MIDCAP 150"),
        ("NIFTY%20SMALLCAP%20250",    "NIFTY SMALLCAP 250"),
        ("NIFTY%20MICROCAP%20250",    "NIFTY MICROCAP 250"),
        ("NIFTY%20LARGEMIDCAP%20250", "NIFTY LARGEMIDCAP 250"),
        ("NIFTY%20MIDSMALLCAP%20400", "NIFTY MIDSMALLCAP 400"),
    ]
    all_raw: dict = {}
    failed:  list = []
    referer = "https://www.nseindia.com/market-data/live-equity-market"

    for idx_url, idx_name in indices:
        try:
            data = _nse_json(sess, f"https://www.nseindia.com/api/equity-stockIndices?index={idx_url}",
                             referer=referer, timeout=30, session_factory=nse_session)
            rows = data.get("data", [])
            if not rows:
                log(f"{idx_name}: 0 rows returned. API response keys: {list(data.keys())}", "WARN")
                continue
            new = 0
            for r in rows:
                sym = str(r.get("symbol","")).strip().upper()
                if not sym or sym in all_raw: continue
                try:
                    lp = float(str(r.get("lastPrice","0")).replace(",",""))
                except Exception:
                    lp = 0.0
                if lp > 0:
                    all_raw[sym] = r; new += 1
            log(f"{idx_name}: {len(rows)} rows (+{new} new), total unique: {len(all_raw)}")
            time.sleep(0.8)
        except Exception as exc:
            log_tb(f"Bhavcopy API '{idx_name}' failed", exc)
            failed.append(idx_name)

    if failed:
        log(f"Failed indices: {failed}. Continuing with {len(all_raw)} symbols collected so far.", "WARN")
    if not all_raw:
        raise ValueError(
            f"Bhavcopy API fallback returned NO data from any of {len(indices)} indices. "
            f"All failed: {failed}. NSE may be blocking GitHub Actions runner IP."
        )

    df = pd.DataFrame(list(all_raw.values()))
    rename = {
        "symbol":"SYMBOL","open":"OPEN","dayHigh":"HIGH","dayLow":"LOW",
        "lastPrice":"CLOSE","previousClose":"PREV_CLOSE",
        "totalTradedVolume":"VOLUME","totalTradedValue":"TURNOVER_LAKHS",
        "change":"CHANGE","pChange":"PCHANGE",
        "yearHigh":"YEAR_HIGH","yearLow":"YEAR_LOW",
        "perChange365d":"PERCHANGE_365D","perChange30d":"PERCHANGE_30D",
        "nearWKH":"NEAR_52WH","nearWKL":"NEAR_52WL","series":"SERIES",
    }
    df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
    if "TURNOVER_LAKHS" in df.columns:
        df["TURNOVER_LAKHS"] = pd.to_numeric(
            df["TURNOVER_LAKHS"].astype(str).str.replace(",",""), errors="coerce"
        ).fillna(0) / 100_000.0
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    for col in ["OPEN","HIGH","LOW","CLOSE","PREV_CLOSE","VOLUME","CHANGE","PCHANGE","YEAR_HIGH","YEAR_LOW"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",",""), errors="coerce")
    df = df[df["CLOSE"] > 0].dropna(subset=["CLOSE"])
    priority = ["SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","PREV_CLOSE","CHANGE","PCHANGE",
                "VOLUME","TURNOVER_LAKHS","YEAR_HIGH","YEAR_LOW","NEAR_52WH","NEAR_52WL",
                "PERCHANGE_365D","PERCHANGE_30D"]
    front = [c for c in priority if c in df.columns]
    return df[front + [c for c in df.columns if c not in front]].reset_index(drop=True)


def clean_bhavcopy(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.upper()
    log(f"Bhavcopy raw columns (first 10): {list(df.columns[:10])}", "DBG")

    if not {"SYMBOL","CLOSE"}.issubset(df.columns):
        col_maps = [
            {"TCKRSYMB":"SYMBOL","SCTYSRS":"SERIES","OPNPRIC":"OPEN","HGHPRIC":"HIGH",
             "LWPRIC":"LOW","CLSPRIC":"CLOSE","TTLTRADGVOL":"VOLUME","TTLTRFVAL":"TURNOVER_LAKHS"},
            {"SYMBOL":"SYMBOL","SERIES":"SERIES","OPEN":"OPEN","HIGH":"HIGH","LOW":"LOW",
             "CLOSE":"CLOSE","TOTTRDQTY":"VOLUME","TOTTRDVAL":"TURNOVER"},
            {"SYMBOL":"SYMBOL","SERIES":"SERIES","OPEN_PRICE":"OPEN","HIGH_PRICE":"HIGH",
             "LOW_PRICE":"LOW","CLOSE_PRICE":"CLOSE","TTL_TRD_QNTY":"VOLUME","TURNOVER_LACS":"TURNOVER_LAKHS"},
        ]
        matched = False
        for mapping in col_maps:
            if all(k in df.columns for k in mapping):
                df = df.rename(columns=mapping)
                log(f"Column map matched on keys: {list(mapping.keys())[:5]}", "DBG")
                matched = True; break
        if not matched:
            raise ValueError(
                f"Bhavcopy column format not recognised.\n"
                f"  All columns in file: {list(df.columns)}\n"
                f"  None of the 3 known column formats matched.\n"
                f"  NSE may have changed their file format — check the raw file manually."
            )
        if "TURNOVER_LAKHS" not in df.columns and "TURNOVER" in df.columns:
            df["TURNOVER_LAKHS"] = pd.to_numeric(df["TURNOVER"], errors="coerce").fillna(0) / 100_000

    if "SERIES" in df.columns:
        before = len(df)
        df = df[df["SERIES"].astype(str).str.strip().str.upper() == "EQ"].copy()
        log(f"EQ filter: {before} → {len(df)} rows", "DBG")

    for col in ["OPEN","HIGH","LOW","CLOSE","VOLUME"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["CLOSE"] > 0].dropna(subset=["CLOSE"])
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    return df.reset_index(drop=True)


def fetch_bhavcopy(date_str: str, sess=None) -> pd.DataFrame:
    sess = sess or nse_session()  # reuse the run-wide session if given, else warm one
    try:
        return clean_bhavcopy(_download_bhavcopy_file(date_str, warmed_sess=sess))
    except Exception as exc:
        log_tb("Bhavcopy file download/parse failed — switching to live API fallback", exc)
        log("🔄 Live API fallback starting...")
        return _download_bhavcopy_api(sess=sess)


# ══════════════════════════════════════════════════════════════════════
# FII / DII
# ══════════════════════════════════════════════════════════════════════

def _parse_crore(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(",","").strip())
    except Exception as exc:
        log(f"_parse_crore: cannot parse {val!r} — {exc}", "WARN")
        return 0.0

def _first_key(d: dict, keys: list, label: str = ""):
    """Return first matching key value, logging which key was used."""
    for k in keys:
        if k in d and d[k] not in (None, "", "-"):
            if label:
                log(f"  {label}: key='{k}' value={d[k]!r}", "DBG")
            return d[k]
    if label:
        log(f"  {label}: NONE of {keys} found. Row has keys: {list(d.keys())}", "WARN")
    return None

def fetch_fii_dii(sess=None):
    sess = sess or nse_session()
    data = _nse_json(sess, "https://www.nseindia.com/api/fiidiiTradeReact",
                     referer="https://www.nseindia.com/report-detail/eq_fii_dii",
                     timeout=30, session_factory=nse_session)

    if isinstance(data, dict):
        rows = data.get("data", []) or [data]
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]
    log(f"FII/DII: {len(rows)} row(s) from API")
    for i, r in enumerate(rows):
        log(f"  row[{i}] keys: {list(r.keys())}", "DBG")
        log(f"  row[{i}] values: {dict(list(r.items())[:6])}", "DBG")

    by_date: dict = defaultdict(lambda: {
        "DATE":"","FII_BUY_CR":0,"FII_SELL_CR":0,"FII_NET_CR":0,
        "DII_BUY_CR":0,"DII_SELL_CR":0,"DII_NET_CR":0
    })

    for idx, row in enumerate(rows):
        raw_date = _first_key(row, ["date","DATE","tradeDate","trade_date"], f"row[{idx}].date")
        if not raw_date:
            raw_date = datetime.today().strftime("%d-%b-%Y")
            log(f"row[{idx}]: no date found — using today {raw_date}", "WARN")
        try:
            row_date = pd.to_datetime(str(raw_date), dayfirst=True).strftime("%Y-%m-%d")
        except Exception as exc:
            log(f"row[{idx}]: date parse failed for {raw_date!r}: {exc} — using today", "WARN")
            row_date = datetime.today().strftime("%Y-%m-%d")

        category = str(row.get("category","")).strip().upper()
        log(f"row[{idx}]: category='{category}', date='{row_date}'", "DBG")

        buy_raw  = _first_key(row, ["buyValue","fiiBuy","diiBuy","BUY_VALUE"],  f"row[{idx}].buy")
        sell_raw = _first_key(row, ["sellValue","fiiSell","diiSell","SELL_VALUE"], f"row[{idx}].sell")
        net_raw  = _first_key(row, ["netValue","fiiNet","diiNet",
                                    "FII_NET_PURCHASE_SALES","DII_NET_PURCHASE_SALES"], f"row[{idx}].net")

        buy_val  = _parse_crore(buy_raw)
        sell_val = _parse_crore(sell_raw)
        net_val  = _parse_crore(net_raw) if net_raw is not None else (buy_val - sell_val)

        rec = by_date[row_date]
        rec["DATE"] = row_date
        if "FII" in category or "FPI" in category or "FOREIGN" in category:
            rec["FII_BUY_CR"]  = round(buy_val,2)
            rec["FII_SELL_CR"] = round(sell_val,2)
            rec["FII_NET_CR"]  = round(net_val,2)
        elif "DII" in category or "DOMESTIC" in category:
            rec["DII_BUY_CR"]  = round(buy_val,2)
            rec["DII_SELL_CR"] = round(sell_val,2)
            rec["DII_NET_CR"]  = round(net_val,2)
        else:
            log(f"row[{idx}]: unknown category '{category}' — assigning by position (FII first, DII second)", "WARN")
            if rec["FII_NET_CR"] == 0 and rec["DII_NET_CR"] == 0:
                rec["FII_BUY_CR"]=round(buy_val,2); rec["FII_SELL_CR"]=round(sell_val,2); rec["FII_NET_CR"]=round(net_val,2)
            else:
                rec["DII_BUY_CR"]=round(buy_val,2); rec["DII_SELL_CR"]=round(sell_val,2); rec["DII_NET_CR"]=round(net_val,2)

    if not by_date:
        raise ValueError(
            f"FII/DII: API returned {len(rows)} rows but none were parseable. "
            f"Check DEBUG logs above for key names and values."
        )

    records = sorted(by_date.values(), key=lambda x: x["DATE"], reverse=True)
    fii = records[0]["FII_NET_CR"]
    dii = records[0]["DII_NET_CR"]
    log(f"FII ₹{fii:+.2f}Cr | DII ₹{dii:+.2f}Cr ({len(records)} day(s))", "OK")
    return pd.DataFrame(records), fii, dii


# ══════════════════════════════════════════════════════════════════════
# INSIDER TRADES
# ══════════════════════════════════════════════════════════════════════

def fetch_insider(days_back: int = 30, max_days_per_request: int = 14, sess=None):
    sess = sess or nse_session()
    # Always use IST — GitHub Actions runners use UTC; datetime.today() gives wrong date
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    full_from = now_ist - timedelta(days=days_back)
    full_to   = now_ist

    # ── Chunked fetch ────────────────────────────────────────────────
    # NSE's corporates-pit endpoint appears to silently return 0 rows when
    # the from_date→to_date span is too wide (observed: 30-day request → 0
    # rows, while the sibling corporate-announcements endpoint's 14-day
    # request on the same run returned 7198 rows — same session, same
    # params shape, only the date span differs). Rather than erroring, NSE
    # just hands back an empty array, so this never looked like a block to
    # the retry logic. Splitting into <=max_days_per_request windows works
    # around that cap regardless of whether it's a hard NSE limit or a
    # bot-detection soft-block tied to wide-range requests specifically.
    chunks = []
    chunk_end = full_to
    while chunk_end > full_from:
        chunk_start = max(full_from, chunk_end - timedelta(days=max_days_per_request))
        chunks.append((chunk_start, chunk_end))
        chunk_end = chunk_start

    log(f"Insider: fetching {full_from.strftime('%d-%m-%Y')} → {full_to.strftime('%d-%m-%Y')} "
        f"in {len(chunks)} chunk(s) of <={max_days_per_request}d (IST: {now_ist.strftime('%Y-%m-%d %H:%M')})")

    # ── Akamai cookie harvest ────────────────────────────────────────
    # corporates-pit is the one endpoint that curl_cffi alone can't get past
    # (confirmed: it sets bm_sz/bm_sv — Akamai Bot Manager — then stubs or
    # hangs the actual call). Do this ONCE per run, before the chunk loop,
    # not per-chunk: it's a real browser launch (a few seconds), and the
    # cookies it earns are reusable across all chunks on this session.
    # Never fatal — if Playwright isn't installed or the harvest fails, this
    # just logs and falls through to the plain curl_cffi attempt below,
    # exactly as it worked before this was added.
    try:
        harvested = harvest_akamai_cookies(
            "https://www.nseindia.com/companies-listing/corporate-filings-pit"
        )
        if harvested:
            _inject_cookies_into_session(sess, harvested)
    except Exception as exc:
        log_tb("Akamai cookie harvest step raised unexpectedly — continuing without it", exc)

    rows: list = []
    seen_keys: set = set()
    chunk_failures = []

    for i, (c_from, c_to) in enumerate(chunks, 1):
        from_dt = c_from.strftime("%d-%m-%Y")
        to_dt   = c_to.strftime("%d-%m-%Y")
        log(f"Insider chunk {i}/{len(chunks)}: {from_dt} → {to_dt}", "DBG")
        try:
            data = _nse_json(sess, "https://www.nseindia.com/api/corporates-pit",
                             params={"index":"equities","from_date":from_dt,"to_date":to_dt},
                             referer="https://www.nseindia.com/companies-listing/corporate-filings-pit",
                             section_warmup="https://www.nseindia.com/companies-listing/corporate-filings-pit",
                             timeout=25, min_bytes=20, session_factory=nse_session,
                             path_key="/api/corporates-pit")
        except Exception as exc:
            log_tb(f"Insider chunk {i}/{len(chunks)} ({from_dt} → {to_dt}) failed", exc)
            chunk_failures.append(f"{from_dt}→{to_dt}: {exc}")
            continue

        chunk_rows = data.get("data", []) if isinstance(data, dict) else (data or [])
        log(f"Insider chunk {i}/{len(chunks)}: {len(chunk_rows)} raw rows", "DBG")

        new_in_chunk = 0
        for r in chunk_rows:
            # De-dupe across chunk boundaries (a trade filed right at a
            # chunk edge could show up in two adjacent windows).
            dedupe_key = (
                str(r.get("symbol", r.get("Symbol", r.get("SYMBOL", "")))),
                str(r.get("intimDt", r.get("broadcastDt", r.get("date", "")))),
                str(r.get("acqfromDt", r.get("acqFromDt", ""))),
                str(r.get("totAcqShrs", r.get("secAcq", r.get("noOfShares", "")))),
                str(r.get("acqName", r.get("acquirerName", r.get("name", "")))),
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            rows.append(r)
            new_in_chunk += 1
        log(f"Insider chunk {i}/{len(chunks)}: +{new_in_chunk} new (total so far: {len(rows)})", "DBG")

        if i < len(chunks):
            time.sleep(1.0)

    log(f"Insider API: {len(rows)} raw rows returned across {len(chunks)} chunk(s)"
        + (f" ({len(chunk_failures)} chunk(s) failed)" if chunk_failures else ""))
    if rows:
        log(f"Insider row[0] keys: {list(rows[0].keys())}", "DBG")
        log(f"Insider row[0] sample: {dict(list(rows[0].items())[:8])}", "DBG")
    elif chunk_failures:
        log(f"Insider: ALL chunks returned 0 rows or failed. Failures: {chunk_failures}", "WARN")
    else:
        log("Insider API returned 0 rows across every chunk — genuinely no insider "
            "filings in this window, or NSE is soft-blocking this endpoint entirely", "WARN")

    SYM_KEYS    = ["symbol","Symbol","SYMBOL","scripCode"]
    MODE_KEYS   = ["acqMode","acqmode","transactionType","acquisitionMode","tdpTransactionType"]

    # ── Date keys ──────────────────────────────────────────────────────
    # Use intimDt (NSE filing/intimation date) for recency filtering.
    # acqfromDt is when the TRADE happened — could be weeks old for a
    # recently filed transaction, so it must NOT be used for the cutoff.
    FILING_DATE_KEYS = ["intimDt","broadcastDt","date"]   # filing date → cutoff filter
    TRADE_FROM_KEYS  = ["acqfromDt","acqFromDt"]          # trade start  → display only
    TRADE_TO_KEYS    = ["acqTodt","acqToDt"]              # trade end    → display only

    SHARES_KEYS = ["totAcqShrs","secAcq","noOfShares","totSecAcq","sharesAcquired","buyQuantity"]
    VAL_KEYS    = ["secVal","totVal","value","totSecVal","acquisitionValue","buyValue"]
    NAME_KEYS   = ["acqName","acquirerName","name","personName","insider"]

    # ── Transaction type allowlist ──────────────────────────────────────
    # Only keep genuine insider BUY signals.
    # ANY type not in this set is skipped — this handles "market sale",
    # "gift", "off market", "pledge", "inter-se-transfer", etc. that
    # previously slipped through because the old filter only checked for
    # the word "sell" (missing "sale", "pledge creation", "revoke", etc.)
    KEEP_TYPES = {
        "market purchase",
        "market buy",
        "purchase",
        "buy",
        "esop exercise",           # exercise of options = company gave shares, neutral-to-bullish
        "esos",
        "employee benefit",
        "preferential allotment",  # fresh shares issued — promoter/anchor buy-in
        "preferential offer",
        "rights",
        "rights issue",
        "open offer",
        "creeping acquisition",
        "bulk deal",
        "block deal",
    }
    # Also accept if any KEEP token appears anywhere in the type string
    KEEP_TOKENS = ("purchase","market buy","open offer","rights","creeping","bulk","block",
                   "allotment","esop exercise","esos","employee benefit")

    def _first(d, keys, default=""):
        for k in keys:
            if k in d and d[k] not in (None,"","-"): return d[k]
        return default

    def _safe_dt(raw, label):
        if not raw:
            return None
        try:
            ts = pd.to_datetime(str(raw), dayfirst=True)
            if ts.tzinfo is not None:
                ts = ts.tz_localize(None)
            return ts
        except Exception as exc:
            log(f"  {label}: date parse failed for {raw!r}: {exc}", "WARN")
            return None

    cutoff = pd.Timestamp(now_ist.replace(tzinfo=None) - timedelta(days=days_back))
    records = []
    skipped_type = skipped_date = skipped_nosym = skipped_err = 0

    for i, r in enumerate(rows):
        try:
            sym = str(_first(r, SYM_KEYS)).strip().upper()
            if not sym:
                skipped_nosym += 1
                log(f"  row[{i}]: no symbol — keys present: {list(r.keys())[:8]}", "DBG")
                continue

            # ── Transaction type filter (allowlist approach) ──────────
            acq_type = str(_first(r, MODE_KEYS,"")).strip().lower()

            is_buy = (acq_type in KEEP_TYPES or
                      any(tok in acq_type for tok in KEEP_TOKENS))

            if not is_buy:
                skipped_type += 1
                log(f"  row[{i}] {sym}: skip — type='{acq_type}' not in buy allowlist", "DBG")
                continue

            # ── Date filter (filing date, not trade date) ─────────────
            raw_filing = _first(r, FILING_DATE_KEYS, "")
            if not raw_filing:
                skipped_date += 1
                log(f"  row[{i}] {sym}: skip — no filing date in {FILING_DATE_KEYS}. "
                    f"Row keys: {list(r.keys())}", "WARN")
                continue

            filing_dt = _safe_dt(raw_filing, f"row[{i}] {sym}")
            if filing_dt is None:
                skipped_date += 1
                continue
            if filing_dt < cutoff:
                skipped_date += 1
                log(f"  row[{i}] {sym}: skip — filing {filing_dt.date()} < cutoff {cutoff.date()}", "DBG")
                continue

            trade_from = _first(r, TRADE_FROM_KEYS, "")
            trade_to   = _first(r, TRADE_TO_KEYS,   "")

            try:
                shares = float(str(_first(r,SHARES_KEYS,"0")).replace(",",""))
            except Exception as exc:
                log(f"  row[{i}] {sym}: shares parse error — {exc}, defaulting to 0", "WARN")
                shares = 0.0
            try:
                val = float(str(_first(r,VAL_KEYS,"0")).replace(",",""))
            except Exception as exc:
                log(f"  row[{i}] {sym}: value parse error — {exc}, estimating shares×10", "WARN")
                val = shares * 10

            records.append({
                "SYMBOL":      sym,
                "PERSON":      str(_first(r,NAME_KEYS,"Insider")),
                "SHARES":      shares,
                "VALUE_LAKHS": round(val / 100, 2),   # NSE sends value in rupees; convert to lakhs
                "DATE":        filing_dt.strftime("%Y-%m-%d"),
                "TRADE_FROM":  trade_from,
                "TRADE_TO":    trade_to,
                "TYPE":        acq_type,
            })
        except Exception as exc:
            skipped_err += 1
            log_tb(f"Insider row[{i}] unexpected error (row={dict(list(r.items())[:5])})", exc)

    log(f"Insider result: {len(records)} buys | {skipped_type} non-buy types | "
        f"{skipped_date} old/no-date | {skipped_nosym} no-symbol | {skipped_err} errors")

    if not records:
        raise ValueError(
            f"No insider buy transactions found.\n"
            f"  API returned {len(rows)} rows total.\n"
            f"  Breakdown: {skipped_type} non-buy types, {skipped_date} too-old/no-date, "
            f"{skipped_nosym} no-symbol, {skipped_err} errors.\n"
            f"  If 0 rows from API: date range params may not be accepted by this endpoint.\n"
            f"  Tip: check DEBUG logs for 'skip — type=' lines to see what types NSE is returning."
        )
    return pd.DataFrame(records), len(records)


# ══════════════════════════════════════════════════════════════════════
# FILINGS
# ══════════════════════════════════════════════════════════════════════

def fetch_filings(days_back: int = 14, sess=None):
    sess = sess or nse_session()
    # IST, not UTC — GitHub Actions runners are UTC and datetime.today() would
    # give the wrong calendar date near midnight IST (see fetch_insider).
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    from_dt = (now_ist - timedelta(days=days_back)).strftime("%d-%m-%Y")
    to_dt   = now_ist.strftime("%d-%m-%Y")
    log(f"Filings: fetching {from_dt} → {to_dt}")

    data = _nse_json(sess, "https://www.nseindia.com/api/corporate-announcements",
                     params={"index":"equities","from_date":from_dt,"to_date":to_dt},
                     referer="https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                     section_warmup="https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                     timeout=25, session_factory=nse_session,
                     path_key="/api/corporate-announcements")

    rows = data.get("data",[]) if isinstance(data,dict) else (data or [])
    log(f"Filings API: {len(rows)} raw rows")
    if rows:
        log(f"Filings row[0] keys: {list(rows[0].keys())}", "DBG")

    records = []
    skipped_nosym = skipped_err = 0
    for i, r in enumerate(rows):
        try:
            sym = str(r.get("symbol","")).strip().upper()
            if not sym:
                skipped_nosym += 1
                continue
            subj = str(r.get("subject", r.get("desc", r.get("an_desc",""))))
            dt   = str(r.get("exchdisstime", r.get("date", datetime.today().strftime("%Y-%m-%d"))))
            records.append({"SYMBOL":sym,"DATE":dt,"SUBJECT":subj,"SENTIMENT":""})
        except Exception as exc:
            skipped_err += 1
            log_tb(f"Filings row[{i}] error", exc)

    log(f"Filings: {len(records)} kept | {skipped_nosym} no-symbol | {skipped_err} errors")
    if not records:
        raise ValueError(
            f"No filings returned.\n"
            f"  API gave {len(rows)} rows, {skipped_nosym} had no symbol, {skipped_err} errors.\n"
            f"  If 0 rows: check the from_date/to_date params are accepted."
        )
    return pd.DataFrame(records), len(records)


# ══════════════════════════════════════════════════════════════════════
# EARNINGS CALENDAR
# ══════════════════════════════════════════════════════════════════════

def fetch_earnings(sess=None):
    sess = sess or nse_session()
    data = _nse_json(sess, "https://www.nseindia.com/api/event-calendar",
                     params={"index":"equities"},
                     referer="https://www.nseindia.com/companies-listing/corporate-filings",
                     timeout=30, session_factory=nse_session)

    rows = data.get("data",[]) if isinstance(data,dict) else (data or [])
    log(f"Earnings API: {len(rows)} raw rows")
    if rows:
        log(f"Earnings row[0] keys: {list(rows[0].keys())}", "DBG")
        log(f"Earnings row[0] sample: {dict(list(rows[0].items())[:6])}", "DBG")

    records = []
    skipped_type = skipped_err = 0
    for i, r in enumerate(rows):
        try:
            sym = str(r.get("symbol","")).strip().upper()
            pur = str(r.get("purpose","")).lower()
            if "result" not in pur and "dividend" not in pur:
                skipped_type += 1
                continue
            records.append({"SYMBOL":sym,"RESULT_DATE":str(r.get("date","")),"PURPOSE":pur})
        except Exception as exc:
            skipped_err += 1
            log_tb(f"Earnings row[{i}] error", exc)

    log(f"Earnings: {len(records)} results/dividends | {skipped_type} other event types | {skipped_err} errors")
    if not records:
        raise ValueError(
            f"No earnings events.\n"
            f"  API gave {len(rows)} rows — {skipped_type} were not results/dividends, "
            f"{skipped_err} had errors.\n"
            f"  If 0 rows: endpoint may have changed or NSE blocked the request."
        )
    return pd.DataFrame(records), len(records)


# ══════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════════

def get_sheets_client():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
    except Exception as exc:
        raise ValueError(
            f"Could not build Google credentials from GOOGLE_CREDS_JSON.\n"
            f"  Error: {exc}\n"
            f"  Check: is the JSON complete? Does it have type/project_id/private_key/client_email?"
        ) from exc
    try:
        return gspread.authorize(creds)
    except Exception as exc:
        raise ValueError(f"gspread.authorize failed: {exc}") from exc


def push_df(client, tab_name: str, df: pd.DataFrame) -> int:
    log(f"Pushing {len(df)} rows × {len(df.columns)} cols → '{tab_name}'", "DBG")
    try:
        wb = client.open_by_key(GOOGLE_SHEET_ID)
    except gspread.exceptions.APIError as exc:
        raise ValueError(
            f"Cannot open spreadsheet (ID={GOOGLE_SHEET_ID}).\n"
            f"  Error: {exc}\n"
            f"  Fix: share the sheet with the service account email as Editor."
        ) from exc
    except Exception as exc:
        raise ValueError(f"open_by_key failed: {exc}") from exc

    try:
        ws = wb.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        log(f"Tab '{tab_name}' not found — creating it", "DBG")
        try:
            ws = wb.add_worksheet(title=tab_name, rows=df.shape[0]+10, cols=df.shape[1]+2)
        except Exception as exc:
            raise ValueError(f"Could not create tab '{tab_name}': {exc}") from exc
    except Exception as exc:
        raise ValueError(f"worksheet('{tab_name}') failed: {exc}") from exc

    try:
        ws.clear()
    except Exception as exc:
        raise ValueError(f"clear() on '{tab_name}' failed: {exc}") from exc

    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    try:
        ws.update(range_name="A1", values=values, value_input_option="RAW")
    except gspread.exceptions.APIError as exc:
        raise ValueError(
            f"Sheets API error writing to '{tab_name}': {exc}\n"
            f"  Possible causes: daily write quota exceeded, too many cells ({len(values)} rows × {len(df.columns)} cols), bad cell values."
        ) from exc
    except Exception as exc:
        raise ValueError(f"update('A1') on '{tab_name}' failed: {exc}") from exc

    return len(values) - 1


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    log("=" * 55)
    log("⚔️  FORTRESS SNIPER — Cloud Fetcher v2.1")
    log("=" * 55)
    log(f"Python {sys.version}")
    log(f"UTC time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")

    validate_config()

    date_str, date_label = get_last_trading_day()
    log(f"Trading date: {date_label}")

    log("\n📋 Connecting to Google Sheets...")
    try:
        client = get_sheets_client()
        log("Google Sheets connected", "OK")
    except Exception as exc:
        log_tb("Google Sheets connection FAILED — cannot continue", exc)
        send_telegram(
            f"❌ {_tg_bold('Fortress Sniper — Sheets Auth Failed')}\n"
            f"Date: {_esc(date_label)}\n"
            f"{_tg_code(type(exc).__name__ + ': ' + str(exc)[:400])}"
            + (f"\n🔗 {_tg_link(_actions_url(), 'View run log')}" if _actions_url() else "")
        )
        sys.exit(1)

    results = {}
    fii_cr = dii_cr = None

    # ── Warm ONE session for the whole run ────────────────────────────
    # Each fetch_*() used to call nse_session() independently — 2 warmup
    # requests apiece, plus more per Insider chunk — bursting 10+ extra
    # requests at nseindia.com in a couple of minutes. NSE's rate-limiting
    # looks volume-based rather than tied to one specific endpoint (different
    # runs have seen different stages stall), so cutting total request count
    # is the fix that matters most. Individual stages still get a fresh,
    # differently-fingerprinted session automatically if their own request
    # fails (via session_factory=nse_session inside _nse_json) — this only
    # removes the *proactive* re-warming that was happening on every call.
    log("\n🔧 Warming shared NSE session for this run...")
    shared_sess = nse_session()

    # ── Ordering note ──────────────────────────────────────────────────
    # Insider and Filings hit corporates-pit / corporate-announcements —
    # the two endpoints that actually time out. They now run FIRST, right
    # after the shared warmup, so they get the freshest session and the
    # least prior NSE traffic in this run, before Bhavcopy/FII-DII/Earnings
    # (which have never failed) spend any of the run's request budget or
    # trip any volume-based throttling ahead of them.

    # ── 1. INSIDER ───────────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📥 [1/5] Insider trades")
    try:
        df, count = fetch_insider(sess=shared_sess)
        rows = push_df(client, SHEET_INSIDER, df)
        log(f"{count} transactions → '{SHEET_INSIDER}'", "OK")
        results["INSIDER"] = f"✅ {count} buy transactions"
    except Exception as exc:
        log_tb("Insider FAILED", exc)
        results["INSIDER"] = f"⚠️ {type(exc).__name__}: {str(exc)[:150]}"

    # ── 2. FILINGS ───────────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📥 [2/5] Filings")
    try:
        df, count = fetch_filings(sess=shared_sess)
        rows = push_df(client, SHEET_FILINGS, df)
        log(f"{count} filings → '{SHEET_FILINGS}'", "OK")
        results["FILINGS"] = f"✅ {count} filings"
    except Exception as exc:
        log_tb("Filings FAILED", exc)
        results["FILINGS"] = f"❌ {type(exc).__name__}: {str(exc)[:150]}"

    # ── 3. BHAVCOPY ──────────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📥 [3/5] Bhavcopy")
    try:
        df   = fetch_bhavcopy(date_str, sess=shared_sess)
        rows = push_df(client, SHEET_BHAVCOPY, df)
        log(f"{rows} rows → '{SHEET_BHAVCOPY}'", "OK")
        results["BHAVCOPY"] = f"✅ {rows} rows"
    except Exception as exc:
        log_tb("Bhavcopy FAILED", exc)
        results["BHAVCOPY"] = f"❌ {type(exc).__name__}: {str(exc)[:150]}"

    # ── 4. FII/DII ───────────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📥 [4/5] FII/DII")
    try:
        df, fii_cr, dii_cr = fetch_fii_dii(sess=shared_sess)
        rows = push_df(client, SHEET_FII_DII, df)
        log(f"FII ₹{fii_cr:+.2f}Cr | DII ₹{dii_cr:+.2f}Cr → {rows} rows", "OK")
        results["FII_DII"] = f"✅ FII ₹{fii_cr:+.2f}Cr | DII ₹{dii_cr:+.2f}Cr"
    except Exception as exc:
        log_tb("FII/DII FAILED", exc)
        results["FII_DII"] = f"❌ {type(exc).__name__}: {str(exc)[:150]}"

    # ── 5. EARNINGS ──────────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📥 [5/5] Earnings")
    try:
        df, count = fetch_earnings(sess=shared_sess)
        rows = push_df(client, SHEET_EARNINGS, df)
        log(f"{count} events → '{SHEET_EARNINGS}'", "OK")
        results["EARNINGS"] = f"✅ {count} events"
    except Exception as exc:
        log_tb("Earnings FAILED", exc)
        results["EARNINGS"] = f"❌ {type(exc).__name__}: {str(exc)[:150]}"

    # ── TELEGRAM SUMMARY ─────────────────────────────────────────────
    log("\n" + "─" * 45)
    log("📤 Sending Telegram summary...")

    errors = [k for k, v in results.items() if v.startswith("❌")]
    fii_str = (f"{'🟢' if (fii_cr or 0)>=0 else '🔴'} FII ₹{fii_cr:+.2f} Cr"
               if fii_cr is not None else "FII ❓ N/A")
    dii_str = (f"{'🟢' if (dii_cr or 0)>=0 else '🔴'} DII ₹{dii_cr:+.2f} Cr"
               if dii_cr is not None else "DII ❓ N/A")
    header  = "⚔️ <b>Fortress Sniper</b>" + (" ⚠️ PARTIAL FAILURE" if errors else " ✅ Done")

    msg = (
        f"{header} — {date_label}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{fii_str}\n{dii_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"{_tg_bold(k)}: {_esc(v)}" for k, v in results.items())
        + f"\n━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {datetime.utcnow().strftime('%H:%M UTC')}"
        + (f"\n🔗 {_tg_link(_actions_url(), 'View full log')}" if _actions_url() else "")
    )
    send_telegram(msg)

    if errors:
        log(f"\n⚠️  Completed WITH ERRORS in: {errors}")
        sys.exit(1)
    else:
        log("\n🎉 ALL 5 TABS UPDATED SUCCESSFULLY!", "OK")


if __name__ == "__main__":
    main()
