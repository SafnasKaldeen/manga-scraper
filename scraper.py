"""
Google News Scraper with Supabase ingestion
- Scrapes Google News for given queries
- Deduplicates by title (checks DB before insert)
- Upserts on article_url conflict as a safety net
- Maps fields to news_articles table schema
- expires_at is handled automatically by the DB trigger set_news_article_expiry()
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import requests
from bs4 import BeautifulSoup
import re
import random
import sys
import os
from datetime import datetime, timezone

from supabase import create_client, Client


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError("SUPABASE_URL and SUPABASE_KEY must be set")
    return create_client(url, key)


def fetch_existing_titles(supabase: Client, titles: list[str]) -> set[str]:
    """
    Returns normalised (lowercased) titles that already exist in the DB.
    Uses an 'in' filter — only checks the titles we are about to insert.
    """
    if not titles:
        return set()
    try:
        response = (
            supabase.table("news_articles")
            .select("title")
            .in_("title", titles)
            .execute()
        )
        return {row["title"].strip().lower() for row in (response.data or [])}
    except Exception as e:
        print(f"⚠  Could not fetch existing titles from Supabase: {e}")
        return set()


def ingest_articles(supabase: Client, articles: list[dict], query: str) -> dict:
    """
    1. Title-based dedup against DB.
    2. Upsert new rows (article_url conflict → ignore as a safety net).
    Returns {"inserted": int, "skipped": int}.
    """
    if not articles:
        return {"inserted": 0, "skipped": 0}

    # ── Step 1: title dedup ───────────────────────────────────────────────
    all_titles = [a["title"].strip() for a in articles]
    existing   = fetch_existing_titles(supabase, all_titles)

    new_articles = []
    skipped = 0
    for a in articles:
        if a["title"].strip().lower() in existing:
            print(f"  ⏭  Duplicate title — skipping: {a['title'][:72]}")
            skipped += 1
        else:
            new_articles.append(a)

    print(f"\n  📥  {len(new_articles)} new | {skipped} title-duplicates skipped\n")

    if not new_articles:
        return {"inserted": 0, "skipped": skipped}

    # ── Step 2: build rows ────────────────────────────────────────────────
    now  = datetime.now(timezone.utc).isoformat()
    rows = []

    for a in new_articles:
        # Parse ISO datetime from Google News <time datetime="...">
        published_at   = None
        published_text = a.get("published")
        if published_text:
            try:
                published_at = datetime.fromisoformat(
                    published_text.replace("Z", "+00:00")
                ).isoformat()
            except ValueError:
                pass  # keep published_text only, published_at stays None

        # Prefer resolved real URL; fall back to google_link
        article_url = a.get("real_url") or ""
        if not article_url or "google.com" in article_url:
            article_url = a.get("google_link", "")
        if not article_url:
            print(f"  ⚠  No usable URL — skipping: {a['title'][:72]}")
            skipped += 1
            continue

        rows.append({
            "title":          a["title"].strip(),
            "publisher":      a.get("publisher") or None,
            "published_at":   published_at,
            "published_text": published_text or None,
            "google_link":    a.get("google_link") or None,
            "article_url":    article_url,
            "image_url":      a.get("image") or None,
            "query":          query,
            "scraped_at":     now,
            # expires_at: set automatically by DB trigger set_news_article_expiry()
        })

    if not rows:
        return {"inserted": 0, "skipped": skipped}

    # ── Step 3: upsert ────────────────────────────────────────────────────
    try:
        response = (
            supabase.table("news_articles")
            .upsert(rows, on_conflict="article_url", ignore_duplicates=True)
            .execute()
        )
        inserted = len(response.data) if response.data else 0
        print(f"  ✅  Inserted {inserted} rows into Supabase (news_articles)")
        return {"inserted": inserted, "skipped": skipped}
    except Exception as e:
        print(f"  ❌  Supabase upsert failed: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# SELENIUM DRIVER
# ─────────────────────────────────────────────────────────────────────────────
READY_SEL = "a.WwrzSb, a.JtKRv"
IMAGE_SEL  = ["img.Quavad", "figure img.Quavad", "figure img"]


def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    return driver


def wait_for_render(driver, timeout=30):
    print(f"  ⏳  Waiting for render (up to {timeout}s)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if driver.find_elements(By.CSS_SELECTOR, READY_SEL):
            print("  ✅  Page rendered")
            return True
        time.sleep(1)
    return False


def scroll_page(driver, scrolls=5, pause=1.5):
    for _ in range(scrolls):
        driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(pause)


# ─────────────────────────────────────────────────────────────────────────────
# DOM HELPERS — walk up the DOM tree to find related elements
# ─────────────────────────────────────────────────────────────────────────────
def js_closest_text(driver, element, css_sel):
    return driver.execute_script("""
        var el = arguments[0], sel = arguments[1];
        for (var i = 0; i < 10; i++) {
            el = el.parentElement;
            if (!el) return null;
            var found = el.querySelector(sel);
            if (found) return found.textContent.trim();
        }
        return null;
    """, element, css_sel)


def js_closest_attr(driver, element, css_sel, attr):
    return driver.execute_script("""
        var el = arguments[0], sel = arguments[1], attr = arguments[2];
        for (var i = 0; i < 10; i++) {
            el = el.parentElement;
            if (!el) return null;
            var found = el.querySelector(sel);
            if (found) return found.getAttribute(attr);
        }
        return null;
    """, element, css_sel, attr)


def js_closest_src(driver, element, css_sel):
    return driver.execute_script("""
        var el = arguments[0], sel = arguments[1];
        for (var i = 0; i < 10; i++) {
            el = el.parentElement;
            if (!el) return null;
            var img = el.querySelector(sel);
            if (img) {
                var srcset = img.getAttribute('srcset') || '';
                if (srcset) {
                    var parts = srcset.split(',').map(s => s.trim().split(' ')[0]).filter(Boolean);
                    if (parts.length) return parts[parts.length - 1];
                }
                return img.getAttribute('src') || null;
            }
        }
        return null;
    """, element, css_sel)


# ─────────────────────────────────────────────────────────────────────────────
# URL RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://news.google.com/",
})


def resolve_redirect(url, timeout=10):
    if not url or not url.startswith("http"):
        return url
    try:
        r     = SESSION.get(url, allow_redirects=True, timeout=timeout)
        final = r.url
        if "google.com" in final:
            soup = BeautifulSoup(r.text, "html.parser")
            meta = soup.find("meta", attrs={"http-equiv": "refresh"})
            if meta:
                content = meta.get("content", "")
                match   = re.search(r'url=(.+)', content, re.IGNORECASE)
                if match:
                    return match.group(1).strip("'\" ")
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href") and "google.com" not in canon["href"]:
                return canon["href"]
        return final
    except Exception:
        return url


def extract_og_image(url, timeout=8):
    if not url or "google.com" in url:
        return None
    try:
        r    = SESSION.get(url, timeout=timeout, allow_redirects=True)
        soup = BeautifulSoup(r.text, "html.parser")
        for meta in [
            soup.find("meta", property="og:image"),
            soup.find("meta", attrs={"name": "twitter:image"}),
            soup.find("meta", property="article:image"),
        ]:
            if meta:
                img = meta.get("content", "")
                if img and "gstatic.com" not in img and "google.com" not in img:
                    return img
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
def scrape_google_news(query="anime", max_articles=50) -> list[dict]:
    driver  = setup_driver()
    results = []

    try:
        url = (
            f"https://news.google.com/search"
            f"?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US%3Aen"
        )
        print(f"  🔎  {url}\n")
        driver.get(url)

        if not wait_for_render(driver, timeout=30):
            print("  ❌  Render timeout.")
            return results

        scroll_page(driver, scrolls=5, pause=1.5)

        link_els = driver.find_elements(By.CSS_SELECTOR, READY_SEL)
        print(f"  📰  {len(link_els)} links found — processing up to {max_articles}\n")

        seen_titles: set[str] = set()

        for i, link_el in enumerate(link_els[:max_articles]):
            try:
                href = link_el.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://news.google.com/" + href.lstrip("./")

                # Title
                title = ""
                for sel in ["h3", "h4"]:
                    try:
                        title = link_el.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if title:
                            break
                    except Exception:
                        pass
                if not title:
                    lines = (link_el.text or "").strip().splitlines()
                    title = next((l.strip() for l in lines if l.strip()), "")

                if not title or title.lower() in seen_titles:
                    continue
                seen_titles.add(title.lower())

                # Publisher
                publisher = (
                    js_closest_text(driver, link_el, "div.vr1PYe") or
                    js_closest_text(driver, link_el, "a.wEwyrc")   or
                    js_closest_text(driver, link_el, "div.CEMjEf span") or
                    "Unknown"
                )

                # Published datetime
                published = (
                    js_closest_attr(driver, link_el, "time.hvbAAd",   "datetime") or
                    js_closest_attr(driver, link_el, "time[datetime]", "datetime")
                )

                # Image
                image = None
                for img_sel in IMAGE_SEL:
                    image = js_closest_src(driver, link_el, img_sel)
                    if image:
                        if image.startswith("/"):
                            image = "https://news.google.com" + image
                        break

                print(f"  [{i+1:>3}] {title[:72]}")
                print(f"         Publisher : {publisher}")
                print(f"         Published : {published or 'N/A'}")

                # Resolve real URL
                real_url     = resolve_redirect(href)
                still_google = "google.com" in real_url
                print(f"         URL       : {real_url[:72]}")

                # OG image fallback
                if not image and not still_google:
                    image = extract_og_image(real_url)

                results.append({
                    "title":       title,
                    "publisher":   publisher,
                    "published":   published,
                    "google_link": href,
                    "real_url":    real_url,
                    "image":       image,
                })
                print()
                time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                print(f"  ⚠  [{i+1}]: {e}\n")

    finally:
        driver.quit()

    return results


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Accept query args from CLI: python scraper.py anime manga
    # Defaults to both anime and manga if nothing passed
    queries = sys.argv[1:] if len(sys.argv) > 1 else ["anime", "manga"]
    max_n   = 50

    supabase = get_supabase_client()

    total_inserted = 0
    total_skipped  = 0

    for query in queries:
        print(f"\n{'='*80}")
        print(f"🔍  Query: '{query}'")
        print(f"{'='*80}\n")

        articles = scrape_google_news(query=query, max_articles=max_n)
        print(f"\n  📊  Scraped {len(articles)} articles for '{query}'")

        counts          = ingest_articles(supabase, articles, query)
        total_inserted += counts["inserted"]
        total_skipped  += counts["skipped"]

    print(f"\n{'='*80}")
    print(f"🏁  All done — {total_inserted} inserted, {total_skipped} skipped")
    print(f"{'='*80}\n")
