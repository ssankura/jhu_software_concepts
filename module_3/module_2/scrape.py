"""
scrape.py

Module 2 scraper for GradCafe using ONLY urllib (stdlib).
Key features:
- Cookie-enabled opener (GradCafe may repeat same page without session cookies)
- Browser-like headers
- Simple retry with backoff
"""

from __future__ import annotations

import time
import urllib.request
import urllib.error
import http.cookiejar
from urllib.parse import urlencode

BASE_URL = "https://www.thegradcafe.com/survey/index.php"

# Cookie jar + opener (important for pagination/session consistency)
_COOKIE_JAR = http.cookiejar.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_COOKIE_JAR))

_WARMED_UP = False

def _warm_up(headers: dict) -> None:
    """Visit the survey landing page once to establish cookies/session."""
    global _WARMED_UP
    if _WARMED_UP:
        return
    try:
        req = urllib.request.Request("https://www.thegradcafe.com/survey/", headers=headers, method="GET")
        with _OPENER.open(req, timeout=25) as resp:
            resp.read()  # discard content, keep cookies
        _WARMED_UP = True
        print("[scrape.py] Warm-up completed (cookies established).")
    except Exception as e:
        print(f"[scrape.py] Warm-up failed (continuing anyway): {e}")


def _build_url(page_number: int) -> str:
    """Build GradCafe survey URL for a given page number."""
    params = {"q": "", "t": "a", "pp": "25", "p": str(page_number), "_": str(int(time.time()))}
    return f"{BASE_URL}?{urlencode(params)}"


def scrape_data(page_number: int) -> str | None:
    """
    Fetch a single GradCafe survey page.

    Args:
        page_number: 1-based page index.

    Returns:
        HTML as a string, or None if fetch fails.
    """
    url = _build_url(page_number)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.thegradcafe.com/survey/",
        "Connection": "close",

        # ⭐ ADD THESE TWO LINES
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    _warm_up(headers)

    # Small retry loop (network + transient blocks)
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with _OPENER.open(req, timeout=25) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                return html

        except urllib.error.HTTPError as e:
            # 403/429 can happen; backoff helps
            wait = 2 ** attempt
            print(f"[scrape.py] HTTPError {e.code} on page {page_number}. Retrying in {wait}s...")
            time.sleep(wait)

        except urllib.error.URLError as e:
            wait = 2 ** attempt
            print(f"[scrape.py] URLError on page {page_number}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

        except Exception as e:
            print(f"[scrape.py] Unexpected error on page {page_number}: {e}")
            return None

    print(f"[scrape.py] Failed to fetch page {page_number} after retries.")
    return None
