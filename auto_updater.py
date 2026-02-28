"""
Google News Scraper → Supabase
Scrapes Google News for given queries and saves articles to news_articles table.
Skips duplicates via article_url UNIQUE constraint.
Expiry is handled automatically by the Supabase trigger: set_news_article_expiry()
"""

import os
import sys

sys.stdout.reconfigure(line_buffering=True)

import time
import random
import re
import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from supabase import create_client, Client


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("✗ Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Queries to scrape — add/remove as needed
SEARCH_QUERIES = [
    "anime",
    "manga",
    "anime news",
]

MAX_ARTICLES_PER_QUERY = 50

READY_SEL = "a.WwrzSb, a.JtKRv"
IMAGE_SEL = ["img.Quavad", "figure img.Quavad", "figure img"]


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# SELENIUM DRIVER
# ─────────────────────────────────────────────────────────────────────────────
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
    log(f"Waiting for page render (up to {timeout}s)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if driver.find_elements(By.CSS_SELECTOR, READY_SEL):
            log("Page rendered ✓")
            return True
        time.sleep(1)
    return False


def scroll_page(driver, scrolls=5, pause=1.5):
    for _ in range(scrolls):
        driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(pause)


# ─────────────────────────────────────────────────────────────────────────────
# JS DOM HELPERS — walk up ancestor tree
# ─────────────────────────────────────────────────────────────────────────────
def js_up_text(driver, el, sel):
    return driver.execute_script("""
        var node = arguments[0], sel = arguments[1];
        for (var i = 0; i < 12; i++) {
            node = node.parentElement;
            if (!node) return null;
            var f = node.querySelector(sel);
            if (f) return f.textContent.trim();
        }
        return null;
    """, el, sel)


def js_up_attr(driver, el, sel, attr):
    return driver.execute_script("""
        var node = arguments[0], sel = arguments[1], attr = arguments[2];
        for (var i = 0; i < 12; i++) {
            node = node.parentElement;
            if (!node) return null;
            var f = node.querySelector(sel);
            if (f) return f.getAttribute(attr);
        }
        return null;
    """, el, sel, attr)


def js_up_image(driver, el, sel):
    return driver.execute_script("""
        var node = arguments[0], sel = arguments[1];
        for (var i = 0; i < 12; i++) {
            node = node.parentElement;
            if (!node) return null;
            var img = node.querySelector(sel);
            if (img) {
                var ss = img.getAttribute('srcset') || '';
                if (ss) {
                    var parts = ss.split(',').map(s => s.trim().split(' ')[0]).filter(Boolean);
                    if (parts.length) return parts[parts.length - 1];
                }
                return img.getAttribute('src') || null;
            }
        }
        return null;
    """, el, sel)


# ─────────────────────────────────────────────────────────────────────────────
# URL RESOLUTION via new Selenium tab (avoids /sorry CAPTCHA)
# ─────────────────────────────────────────────────────────────────────────────
def resolve_url_via_selenium(driver, google_url, timeout=8):
    original = driver.current_window_handle
    try:
        driver.execute_script("window.open('');")
        new_handle = [h for h in driver.window_handles if h != original][-1]
        driver.switch_to.window(new_handle)
        driver.get(google_url)

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = driver.current_url
            if "google.com" not in current and "about:blank" not in current:
                driver.close()
                driver.switch_to.window(original)
                return current
            time.sleep(0.5)

        # Still on Google — try canonical from page source
        final_url = driver.current_url
        try:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href") and "google.com" not in canon["href"]:
                final_url = canon["href"]
        except Exception:
            pass

        driver.close()
        driver.switch_to.window(original)
        return final_url

    except Exception:
        try:
            driver.close()
            driver.switch_to.window(original)
        except Exception:
            pass
        return google_url


# ─────────────────────────────────────────────────────────────────────────────
# OG IMAGE fallback
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
    "Accept-Language": "en-US,en;q=0.9",
})


def extract_og_image(url, timeout=8):
    if not url or "google.com" in url:
        return None
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
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
# SUPABASE — get existing URLs to skip duplicates
# ─────────────────────────────────────────────────────────────────────────────
def get_existing_urls(query):
    """Fetch all article_urls already stored for this query."""
    try:
        result = (
            supabase.table("news_articles")
            .select("article_url")
            .eq("query", query)
            .execute()
        )
        return {row["article_url"] for row in result.data}
    except Exception as e:
        log(f"Error fetching existing URLs: {e}", "ERROR")
        return set()


def save_article_to_supabase(article: dict) -> bool:
    """
    Insert a single article. Skips gracefully if article_url already exists
    (UNIQUE constraint). Maps scraper fields → table columns.
    """
    try:
        # Parse ISO datetime string → aware datetime for Supabase timestamptz
        published_at = None
        if article.get("published"):
            try:
                published_at = article["published"]  # Already ISO string, Supabase accepts it
            except Exception:
                published_at = None

        row = {
            "title":          article["title"],
            "publisher":      article.get("publisher") or None,
            "published_at":   published_at,
            "published_text": article.get("published") or None,  # raw ISO string as text backup
            "google_link":    article.get("google_link") or None,
            "article_url":    article["real_url"],
            "image_url":      article.get("image") or None,
            "query":          article.get("query") or None,
            "scraped_at":     datetime.now(timezone.utc).isoformat(),
            "created_at":     datetime.now(timezone.utc).isoformat(),
            # expires_at is set automatically by trigger: set_news_article_expiry()
        }

        supabase.table("news_articles").insert(row).execute()
        return True

    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower() or "23505" in err:
            log(f"  ↩  Skipped (duplicate): {article['title'][:60]}")
        else:
            log(f"  ✗  DB error for '{article['title'][:50]}': {err}", "ERROR")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE ONE QUERY
# ─────────────────────────────────────────────────────────────────────────────
def scrape_query(driver, query, max_articles=50):
    log("=" * 70)
    log(f"Query: '{query}'")
    log("=" * 70)

    url = (
        f"https://news.google.com/search"
        f"?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US%3Aen"
    )
    log(f"URL: {url}")
    driver.get(url)

    if not wait_for_render(driver, timeout=30):
        log("❌  Render timeout — skipping query", "ERROR")
        return []

    scroll_page(driver, scrolls=5, pause=1.5)

    link_els = driver.find_elements(By.CSS_SELECTOR, READY_SEL)
    log(f"Found {len(link_els)} article links — processing up to {max_articles}")

    # Pre-fetch existing URLs for this query to avoid redundant DB inserts
    existing_urls = get_existing_urls(query)
    log(f"Already in DB for this query: {len(existing_urls)}")

    articles = []
    seen_titles = set()

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

            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            # Publisher
            publisher = (
                js_up_text(driver, link_el, "div.vr1PYe") or
                js_up_text(driver, link_el, "a.wEwyrc") or
                "Unknown"
            )

            # Time
            published = (
                js_up_attr(driver, link_el, "time.hvbAAd", "datetime") or
                js_up_attr(driver, link_el, "time[datetime]", "datetime")
            )

            # Thumbnail from card
            image = None
            for img_sel in IMAGE_SEL:
                image = js_up_image(driver, link_el, img_sel)
                if image:
                    if image.startswith("/"):
                        image = "https://news.google.com" + image
                    break

            # Resolve real URL via Selenium tab
            real_url = resolve_url_via_selenium(driver, href, timeout=8)
            on_google = "google.com" in real_url

            log(f"[{i+1:>3}] {title[:65]}")
            log(f"       Publisher : {publisher}  |  Published : {published or 'N/A'}")
            log(f"       URL       : {real_url[:70]}")
            log(f"       Image     : {'✅' if image else '—'}")

            # OG image fallback if no card thumbnail
            if not image and not on_google:
                image = extract_og_image(real_url)
                if image:
                    log(f"       Image(OG) : ✅")

            # Skip if real URL still on Google (blocked redirect)
            if on_google:
                log(f"       ⚠  Redirect blocked — skipping save")
                continue

            # Skip if already in DB
            if real_url in existing_urls:
                log(f"       ↩  Already in DB — skipping")
                continue

            article = {
                "title":       title,
                "publisher":   publisher,
                "published":   published,
                "google_link": href,
                "real_url":    real_url,
                "image":       image,
                "query":       query,
            }
            articles.append(article)
            log("")

            time.sleep(random.uniform(0.3, 0.7))

        except Exception as e:
            log(f"⚠  [{i+1}]: {e}", "WARNING")

    return articles


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log("=" * 70)
    log("GOOGLE NEWS SCRAPER → SUPABASE — STARTING")
    log("=" * 70)
    log(f"Queries: {SEARCH_QUERIES}")
    log(f"Max articles per query: {MAX_ARTICLES_PER_QUERY}")
    log("")

    driver = setup_driver()

    total_saved   = 0
    total_skipped = 0
    total_failed  = 0

    try:
        for q_idx, query in enumerate(SEARCH_QUERIES, 1):
            log(f"\nQUERY {q_idx}/{len(SEARCH_QUERIES)}: '{query}'")

            articles = scrape_query(driver, query, max_articles=MAX_ARTICLES_PER_QUERY)

            log(f"\nSaving {len(articles)} articles to Supabase...")

            for article in articles:
                if save_article_to_supabase(article):
                    total_saved += 1
                    log(f"  ✓  Saved: {article['title'][:65]}")
                else:
                    total_skipped += 1

            # Pause between queries
            if q_idx < len(SEARCH_QUERIES):
                log("Waiting 5s before next query...")
                time.sleep(5)

    finally:
        driver.quit()

    # ── Summary ───────────────────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("SUMMARY")
    log("=" * 70)
    log(f"Queries processed : {len(SEARCH_QUERIES)}")
    log(f"Articles saved    : {total_saved}")
    log(f"Skipped/duplicate : {total_skipped}")
    log(f"Failed            : {total_failed}")
    log("✓ Done!")
    log("=" * 70)


if __name__ == "__main__":
    main()
