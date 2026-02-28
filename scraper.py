"""
Google News Scraper → Supabase
Deduplication:
  - By article_url  (UNIQUE constraint in DB)
  - By normalised title (catches same story from different URLs)
  Both checks happen BEFORE resolving URLs, saving time on known duplicates.
expires_at set automatically by Supabase trigger: set_news_article_expiry()
"""

import os
import sys

sys.stdout.reconfigure(line_buffering=True)

import time
import random
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.by import By
from selenium.webdriver.chrome.options import Options
from supabase import create_client, Client


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("✗ Error: SUPABASE_URL and SUPABASE_KEY must be set", flush=True)
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SEARCH_QUERIES         = ["anime", "manga", "anime news"]
MAX_ARTICLES_PER_QUERY = 50
URL_RESOLVE_TIMEOUT    = 4   # seconds per article URL

READY_SEL = "a.WwrzSb, a.JtKRv"
IMAGE_SEL = ["img.Quavad", "figure img.Quavad", "figure img"]


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# TITLE NORMALISATION
# ─────────────────────────────────────────────────────────────────────────────
def norm(title: str) -> str:
    """Lowercase + strip all non-alphanumeric chars for fuzzy-safe comparison."""
    return re.sub(r"[^a-z0-9]", "", title.lower())


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
# JS DOM HELPERS
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
# URL RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://news.google.com/",
})


def resolve_url_fast(google_url):
    try:
        r = SESSION.get(google_url, allow_redirects=True, timeout=5)
        if "google.com" not in r.url:
            return r.url
    except Exception:
        pass
    return None


def resolve_url_via_selenium(driver, google_url, timeout=URL_RESOLVE_TIMEOUT):
    original = driver.current_window_handle
    try:
        driver.execute_script("window.open('');")
        new_handle = [h for h in driver.window_handles if h != original][-1]
        driver.switch_to.window(new_handle)
        driver.set_page_load_timeout(timeout)
        try:
            driver.get(google_url)
        except Exception:
            pass

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = driver.current_url
            if "google.com" not in current and "about:blank" not in current:
                driver.close()
                driver.switch_to.window(original)
                driver.set_page_load_timeout(30)
                return current
            time.sleep(0.3)

        final = driver.current_url
        driver.close()
        driver.switch_to.window(original)
        driver.set_page_load_timeout(30)
        return final if "google.com" not in final else google_url

    except Exception as e:
        try:
            driver.close()
            driver.switch_to.window(original)
            driver.set_page_load_timeout(30)
        except Exception:
            pass
        return google_url


def resolve_url(driver, google_url):
    url = resolve_url_fast(google_url)
    if url:
        return url, "fast"
    return resolve_url_via_selenium(driver, google_url), "tab"


# ─────────────────────────────────────────────────────────────────────────────
# OG IMAGE
# ─────────────────────────────────────────────────────────────────────────────
def extract_og_image(url, timeout=5):
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
# SUPABASE
# ─────────────────────────────────────────────────────────────────────────────
def load_existing_records():
    existing_urls   = set()
    existing_titles = set()
    try:
        batch = 1000
        start = 0
        while True:
            result = (
                supabase.table("news_articles")
                .select("article_url, title")
                .range(start, start + batch - 1)
                .execute()
            )
            if not result.data:
                break
            for row in result.data:
                existing_urls.add(row["article_url"])
                existing_titles.add(norm(row["title"]))
            if len(result.data) < batch:
                break
            start += batch

        log(f"Loaded {len(existing_urls)} existing URLs and {len(existing_titles)} titles from DB")
    except Exception as e:
        log(f"Error loading existing records: {e}", "ERROR")

    return existing_urls, existing_titles


def save_article_to_supabase(article: dict) -> bool:
    try:
        row = {
            "title":          article["title"],
            "publisher":      article.get("publisher") or None,
            "published_at":   article.get("published") or None,
            "published_text": article.get("published") or None,
            "google_link":    article.get("google_link") or None,
            "article_url":    article["real_url"],
            "image_url":      article.get("image") or None,
            "query":          article.get("query") or None,
            "scraped_at":     datetime.now(timezone.utc).isoformat(),
            "created_at":     datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("news_articles").insert(row).execute()
        return True
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower() or "23505" in err:
            log(f"  ↩  DB duplicate caught: {article['title'][:60]}")
        else:
            log(f"  ✗  DB error: {err}", "ERROR")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE ONE QUERY
# ─────────────────────────────────────────────────────────────────────────────
def scrape_query(driver, query, max_articles, existing_urls, existing_titles):
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
    log(f"Found {len(link_els)} links — processing up to {max_articles}")

    articles = []

    for i, link_el in enumerate(link_els[:max_articles]):
        try:
            href = link_el.get_attribute("href") or ""
            if not href:
                continue
            if not href.startswith("http"):
                href = "https://news.google.com/" + href.lstrip("./")

            # ── Title ─────────────────────────────────────────────────────
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

            if not title:
                continue

            # ── Title dedup ───────────────────────────────────────────────
            title_key = norm(title)
            if title_key in existing_titles:
                log(f"[{i+1:>3}] ↩  Title already known: {title[:65]}")
                continue

            # ── Publisher & time ──────────────────────────────────────────
            publisher = (
                js_up_text(driver, link_el, "div.vr1PYe") or
                js_up_text(driver, link_el, "a.wEwyrc") or
                "Unknown"
            )
            published = (
                js_up_attr(driver, link_el, "time.hvbAAd", "datetime") or
                js_up_attr(driver, link_el, "time[datetime]", "datetime")
            )

            # ── Thumbnail ─────────────────────────────────────────────────
            image = None
            for img_sel in IMAGE_SEL:
                image = js_up_image(driver, link_el, img_sel)
                if image:
                    if image.startswith("/"):
                        image = "https://news.google.com" + image
                    break

            # ── Resolve real URL ──────────────────────────────────────────
            log(f"[{i+1:>3}] Resolving: {title[:55]}...")
            t0 = time.time()
            real_url, method = resolve_url(driver, href)
            elapsed = time.time() - t0
            on_google = "google.com" in real_url

            log(f"       Publisher : {publisher}  |  Published : {published or 'N/A'}")
            log(f"       URL ({method}, {elapsed:.1f}s): {real_url[:65]}")
            log(f"       Image     : {'✅' if image else '—'}")

            # OG image fallback
            if not image and not on_google:
                image = extract_og_image(real_url)
                if image:
                    log(f"       Image(OG) : ✅")

            if on_google:
                log(f"       ⚠  Redirect blocked — skipping")
                continue

            # ── URL dedup ─────────────────────────────────────────────────
            if real_url in existing_urls:
                log(f"       ↩  URL already in DB — skipping")
                existing_titles.add(title_key)
                continue

            # ── Accept ────────────────────────────────────────────────────
            existing_urls.add(real_url)
            existing_titles.add(title_key)

            articles.append({
                "title":       title,
                "publisher":   publisher,
                "published":   published,
                "google_link": href,
                "real_url":    real_url,
                "image":       image,
                "query":       query,
            })
            log("")

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
    log(f"Queries       : {SEARCH_QUERIES}")
    log(f"Max per query : {MAX_ARTICLES_PER_QUERY}")
    log(f"URL timeout   : {URL_RESOLVE_TIMEOUT}s per article")
    log("")

    existing_urls, existing_titles = load_existing_records()

    driver        = setup_driver()
    total_saved   = 0
    total_skipped = 0

    try:
        for q_idx, query in enumerate(SEARCH_QUERIES, 1):
            log(f"\nQUERY {q_idx}/{len(SEARCH_QUERIES)}: '{query}'")

            articles = scrape_query(
                driver, query, MAX_ARTICLES_PER_QUERY,
                existing_urls, existing_titles
            )

            log(f"\nSaving {len(articles)} new articles to Supabase...")
            for article in articles:
                if save_article_to_supabase(article):
                    total_saved += 1
                    log(f"  ✓  Saved: {article['title'][:65]}")
                else:
                    total_skipped += 1

            if q_idx < len(SEARCH_QUERIES):
                log("Waiting 5s before next query...")
                time.sleep(5)

    finally:
        driver.quit()

    log("\n" + "=" * 70)
    log("SUMMARY")
    log("=" * 70)
    log(f"Queries processed : {len(SEARCH_QUERIES)}")
    log(f"Articles saved    : {total_saved}")
    log(f"Skipped/duplicate : {total_skipped}")
    log("✓ Done!")
    log("=" * 70)


if __name__ == "__main__":
    main()
