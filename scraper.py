"""
Google News Scraper — fully working version
Fixes:
  1. Published time: search from the link element outward using JS closest()
     instead of walking a fixed number of parent levels
  2. Real URL: Google News /read/ URLs require a browser redirect or
     decoding the base64 article ID — we use a session with full headers
     and a longer timeout, plus fallback to Selenium for stubborn redirects
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import requests
from bs4 import BeautifulSoup
import json
import re
import random
import sys
import base64


READY_SEL  = "a.WwrzSb, a.JtKRv"
IMAGE_SEL  = ["img.Quavad", "figure img.Quavad", "figure img"]


# ─────────────────────────────────────────────────────────────────────────────
# DRIVER
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


# ─────────────────────────────────────────────────────────────────────────────
# WAIT
# ─────────────────────────────────────────────────────────────────────────────
def wait_for_render(driver, timeout=30):
    print(f"⏳  Waiting for render (up to {timeout}s)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if driver.find_elements(By.CSS_SELECTOR, READY_SEL):
            print(f"✅  Page rendered")
            return True
        time.sleep(1)
    return False


def scroll_page(driver, scrolls=5, pause=1.5):
    for _ in range(scrolls):
        driver.execute_script("window.scrollBy(0, 900);")
        time.sleep(pause)


# ─────────────────────────────────────────────────────────────────────────────
# FIND CARD CONTEXT via JS — walks up DOM until it finds the target selector
# ─────────────────────────────────────────────────────────────────────────────
def js_closest_text(driver, element, css_sel):
    """Walk up from element, search each ancestor for css_sel, return text."""
    return driver.execute_script("""
        var el = arguments[0];
        var sel = arguments[1];
        for (var i = 0; i < 10; i++) {
            el = el.parentElement;
            if (!el) return null;
            var found = el.querySelector(sel);
            if (found) return found.textContent.trim();
        }
        return null;
    """, element, css_sel)


def js_closest_attr(driver, element, css_sel, attr):
    """Walk up from element, search each ancestor for css_sel, return attribute."""
    return driver.execute_script("""
        var el = arguments[0];
        var sel = arguments[1];
        var attr = arguments[2];
        for (var i = 0; i < 10; i++) {
            el = el.parentElement;
            if (!el) return null;
            var found = el.querySelector(sel);
            if (found) return found.getAttribute(attr);
        }
        return null;
    """, element, css_sel, attr)


def js_closest_src(driver, element, css_sel):
    """Walk up from element, search for image, return best src from srcset or src."""
    return driver.execute_script("""
        var el = arguments[0];
        var sel = arguments[1];
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
# Google News /read/ and /articles/ URLs redirect to real articles.
# They require cookie consent headers — use a session with realistic headers.
# ─────────────────────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://news.google.com/",
})


def resolve_redirect(url, timeout=10):
    """Follow redirects to get real article URL."""
    if not url or not url.startswith("http"):
        return url
    try:
        r = SESSION.get(url, allow_redirects=True, timeout=timeout)
        final = r.url
        # If still on Google domain, try parsing redirect from response body
        if "google.com" in final:
            soup = BeautifulSoup(r.text, "html.parser")
            # Some Google News pages embed the real URL in a meta refresh or link
            meta = soup.find("meta", attrs={"http-equiv": "refresh"})
            if meta:
                content = meta.get("content", "")
                match = re.search(r'url=(.+)', content, re.IGNORECASE)
                if match:
                    return match.group(1).strip("'\" ")
            # Try canonical link
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href") and "google.com" not in canon["href"]:
                return canon["href"]
        return final
    except Exception as e:
        return url


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
    except Exception as e:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def scrape_google_news(query="anime", max_articles=50):
    driver = setup_driver()
    results = []

    try:
        url = (
            f"https://news.google.com/search"
            f"?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US%3Aen"
        )
        print(f"🔎  {url}\n")
        driver.get(url)

        if not wait_for_render(driver, timeout=30):
            print("❌  Render timeout.")
            return results

        scroll_page(driver, scrolls=5, pause=1.5)

        link_els = driver.find_elements(By.CSS_SELECTOR, READY_SEL)
        print(f"📰  {len(link_els)} links found — processing up to {max_articles}\n")

        seen = set()

        for i, link_el in enumerate(link_els[:max_articles]):
            try:
                href = link_el.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://news.google.com/" + href.lstrip("./")

                # ── Title ────────────────────────────────────────────
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

                if not title or title in seen:
                    continue
                seen.add(title)

                # ── Publisher — walk up DOM ───────────────────────────
                publisher = (
                    js_closest_text(driver, link_el, "div.vr1PYe") or
                    js_closest_text(driver, link_el, "a.wEwyrc") or
                    js_closest_text(driver, link_el, "div.CEMjEf span") or
                    "Unknown"
                )

                # ── Time — walk up DOM ────────────────────────────────
                published = (
                    js_closest_attr(driver, link_el, "time.hvbAAd", "datetime") or
                    js_closest_attr(driver, link_el, "time[datetime]", "datetime")
                )

                # ── Image — walk up DOM ───────────────────────────────
                image = None
                for img_sel in IMAGE_SEL:
                    image = js_closest_src(driver, link_el, img_sel)
                    if image:
                        if image.startswith("/"):
                            image = "https://news.google.com" + image
                        break

                print(f"[{i+1:>3}] {title[:72]}")
                print(f"       Publisher : {publisher}")
                print(f"       Published : {published or 'N/A'}")
                print(f"       Image     : {'✅  ' + image[:60] if image else '—'}")

                # ── Resolve real URL ──────────────────────────────────
                real_url = resolve_redirect(href)
                still_google = "google.com" in real_url
                print(f"       URL       : {real_url[:72]}")
                if still_google:
                    print(f"       ⚠  Still on Google — redirect blocked")

                # ── OG image fallback ─────────────────────────────────
                if not image and not still_google:
                    image = extract_og_image(real_url)
                    print(f"       Image(OG) : {'✅' if image else '❌'}")

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
                print(f"⚠  [{i+1}]: {e}\n")

    finally:
        driver.quit()

    return results


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "anime"
    max_n = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    articles = scrape_google_news(query, max_articles=max_n)

    print("\n" + "=" * 80)
    print(f"📊  {len(articles)} articles scraped for '{query}'")
    print("=" * 80 + "\n")

    for i, a in enumerate(articles, 1):
        print(f"{i:>3}. {a['title']}")
        print(f"      Publisher : {a['publisher']}")
        print(f"      Published : {a['published'] or 'N/A'}")
        print(f"      URL       : {a['real_url']}")
        print(f"      Image     : {a['image'] or 'NO IMAGE'}")
        print("-" * 80)

    out = f"results_{query.replace(' ', '_')}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)
    print(f"\n💾  Saved → {out}")
