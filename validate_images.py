import os
import sys

# Force unbuffered output so GitHub Actions shows logs in real time
sys.stdout.reconfigure(line_buffering=True)

import requests
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client, Client

# ─── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
MANGA_SLUG   = os.environ.get("MANGA_SLUG", "").strip()
FIX_URLS     = os.environ.get("FIX_URLS", "true").lower() == "true"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("✗ Error: SUPABASE_URL and SUPABASE_KEY must be set")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

REPORT_FILE    = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
MANGA_BASE_URL = "https://www.mangaread.org/manga/"
EXTENSIONS     = [".jpg", ".jpeg", ".png", ".webp"]
MAX_WORKERS    = 20  # concurrent URL checks

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer":    "https://www.mangaread.org/",
    "Accept":     "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

SCRAPE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.mangaread.org/",
}


# ─── Logging ──────────────────────────────────────────────────────────────────
def log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{level}] {message}"
    print(line)
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ─── URL checking ─────────────────────────────────────────────────────────────
def check_url(url: str) -> tuple[bool, int]:
    """HEAD request. Returns (ok, status_code)."""
    try:
        r = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        # Some servers reject HEAD — fall back to GET with stream
        if r.status_code == 405:
            r = requests.get(url, headers=HEADERS, timeout=10, stream=True)
            r.close()
        return r.ok, r.status_code
    except Exception as e:
        log(f"  Request error for {url}: {e}", "WARNING")
        return False, 0


def try_extension_variants(broken_url: str) -> str | None:
    """Try .jpg / .jpeg / .png / .webp swaps."""
    base = broken_url.rsplit(".", 1)[0]
    for ext in EXTENSIONS:
        candidate = base + ext
        if candidate == broken_url:
            continue
        ok, status = check_url(candidate)
        if ok:
            log(f"  ✓ Extension fix found: {candidate}")
            return candidate
    return None


# ─── Supabase helpers ─────────────────────────────────────────────────────────
def get_mangas() -> list[dict]:
    if MANGA_SLUG:
        result = supabase.table("mangas").select("id, title, slug").eq("slug", MANGA_SLUG).execute()
    else:
        result = supabase.table("mangas").select("id, title, slug").execute()
    return result.data or []


def get_panels_for_manga(manga_id: str) -> list[dict]:
    """Paginate all panels for a manga via chapter join."""
    panels = []
    BATCH  = 1000
    start  = 0

    while True:
        result = (
            supabase.table("panels")
            .select("id, image_url, panel_number, chapter_id, chapter:chapters!inner(manga_id, chapter_number, manga:mangas!inner(slug))")
            .eq("chapter.manga_id", manga_id)
            .range(start, start + BATCH - 1)
            .execute()
        )
        data = result.data or []
        panels.extend(data)
        if len(data) < BATCH:
            break
        start += BATCH

    return panels


def update_panel_url(panel_id: str, new_url: str) -> bool:
    try:
        supabase.table("panels").update({"image_url": new_url}).eq("id", panel_id).execute()
        return True
    except Exception as e:
        log(f"  ✗ Supabase update failed for panel {panel_id}: {e}", "ERROR")
        return False


# ─── Mangaread.org re-scrape fallback ────────────────────────────────────────
def scrape_chapter_images(chapter_url: str) -> list[str]:
    try:
        response = requests.get(chapter_url, headers=SCRAPE_HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        images = soup.select(".page-break.no-gaps img")
        image_urls = []
        for img in images:
            img_url = (
                img.get("src")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or img.get("data-original")
            )
            if img_url:
                img_url = img_url.strip()
                full_url = urljoin(chapter_url, img_url)
                image_urls.append(full_url)
        return image_urls
    except Exception as e:
        log(f"  ✗ Failed to scrape {chapter_url}: {e}", "ERROR")
        return []


def find_url_from_mangaread(panel: dict) -> str | None:
    try:
        chapter_data   = panel.get("chapter", {})
        chapter_number = chapter_data.get("chapter_number")
        manga_data     = chapter_data.get("manga", {})
        manga_slug     = manga_data.get("slug")

        if not chapter_number or not manga_slug:
            log(f"  ✗ Missing chapter/manga info in panel data", "ERROR")
            return None

        if isinstance(chapter_number, float) and chapter_number.is_integer():
            chapter_str = str(int(chapter_number))
        else:
            chapter_str = f"{chapter_number:.10g}".replace('.', '-')

        chapter_url = f"{MANGA_BASE_URL}{manga_slug}/chapter-{chapter_str}/"
        log(f"  🔍 Re-scraping from mangaread: {chapter_url}")

        image_urls = scrape_chapter_images(chapter_url)
        if not image_urls:
            log(f"  ✗ No images found on chapter page", "ERROR")
            return None

        panel_number = panel.get("panel_number", 1)
        panel_index  = panel_number - 1

        if panel_index < 0 or panel_index >= len(image_urls):
            log(f"  ✗ Panel {panel_number} out of range (chapter has {len(image_urls)} images)", "ERROR")
            return None

        fresh_url = image_urls[panel_index]
        log(f"  ✓ Got fresh URL from mangaread: {fresh_url}")

        ok, status = check_url(fresh_url)
        if ok:
            return fresh_url

        log(f"  ✗ Fresh URL also returns {status} — chapter may be broken on source", "ERROR")
        return None

    except Exception as e:
        log(f"  ✗ Error during mangaread fallback: {e}", "ERROR")
        return None


# ─── Core fix logic ───────────────────────────────────────────────────────────
def fix_broken_panel(panel: dict) -> tuple[str, str | None]:
    image_url = panel["image_url"]

    log(f"  Trying extension variants...")
    working = try_extension_variants(image_url)
    if working:
        return "extension_fix", working

    log(f"  Extension variants failed — falling back to mangaread re-scrape...")
    time.sleep(1)
    working = find_url_from_mangaread(panel)
    if working:
        return "rescrape_fix", working

    return "unfixable", None


# ─── Per-manga validation ─────────────────────────────────────────────────────
def check_panel_worker(panel: dict) -> tuple[dict, bool, int]:
    """Worker function for thread pool — returns (panel, ok, status)."""
    ok, status = check_url(panel["image_url"])
    return panel, ok, status


def validate_manga(manga: dict) -> dict:
    manga_id    = manga["id"]
    manga_title = manga["title"]
    manga_slug  = manga["slug"]

    log("=" * 70)
    log(f"Validating: {manga_title} ({manga_slug})")
    log("=" * 70)

    panels = get_panels_for_manga(manga_id)
    log(f"Total panels to check: {len(panels)}")
    log(f"Running {MAX_WORKERS} concurrent URL checks...")

    broken_panels     = []
    checked           = 0
    extension_fixed   = 0
    rescrape_fixed    = 0
    unfixable_panels  = []

    # ── Parallel URL checks ──────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_panel_worker, panel): panel for panel in panels}

        for future in as_completed(futures):
            panel, ok, status = future.result()
            checked += 1

            if checked % 200 == 0:
                log(f"  Progress: {checked}/{len(panels)} checked, {len(broken_panels)} broken so far...")

            if not ok:
                log(f"  ✗ BROKEN [{status}] panel {panel['panel_number']}: {panel['image_url']}", "WARNING")
                broken_panels.append((panel, status))

    log(f"  ✓ URL check complete: {checked} checked, {len(broken_panels)} broken")

    # ── Fix broken panels sequentially (scraping must be polite) ────────────
    if FIX_URLS:
        for panel, status in broken_panels:
            strategy, working_url = fix_broken_panel(panel)

            if working_url:
                if update_panel_url(panel["id"], working_url):
                    log(f"  ✓ Fixed via [{strategy}] panel {panel['panel_number']} → {working_url}")
                    if strategy == "extension_fix":
                        extension_fixed += 1
                    else:
                        rescrape_fixed += 1
                else:
                    log(f"  ✗ Found URL but Supabase update failed", "ERROR")
                    unfixable_panels.append(panel)
            else:
                log(f"  ✗ Could not fix panel {panel['panel_number']} — all strategies failed", "ERROR")
                unfixable_panels.append(panel)

            time.sleep(0.5)
    else:
        unfixable_panels = [p for p, _ in broken_panels]

    log(f"\nResults for {manga_title}:")
    log(f"  Total checked     : {len(panels)}")
    log(f"  Broken            : {len(broken_panels)}")
    log(f"  Fixed (extension) : {extension_fixed}")
    log(f"  Fixed (re-scrape) : {rescrape_fixed}")
    log(f"  Unfixable         : {len(unfixable_panels)}")

    return {
        "manga":            manga_title,
        "total":            len(panels),
        "broken":           len(broken_panels),
        "extension_fixed":  extension_fixed,
        "rescrape_fixed":   rescrape_fixed,
        "unfixable":        len(unfixable_panels),
        "unfixable_panels": unfixable_panels,
    }


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    log("=" * 70)
    log("IMAGE URL VALIDATOR - STARTING")
    log("=" * 70)
    log(f"Target      : {MANGA_SLUG or 'ALL mangas'}")
    log(f"Auto-fix    : {FIX_URLS}")
    log(f"Workers     : {MAX_WORKERS}")
    log(f"Report file : {REPORT_FILE}")
    log("")

    mangas = get_mangas()
    if not mangas:
        log("No mangas found", "ERROR")
        sys.exit(1)

    log(f"Mangas to validate: {len(mangas)}\n")

    results = []
    for idx, manga in enumerate(mangas, 1):
        log(f"\nMANGA {idx}/{len(mangas)}")
        result = validate_manga(manga)
        results.append(result)
        if idx < len(mangas):
            time.sleep(3)

    # ── Summary ──────────────────────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("VALIDATION SUMMARY")
    log("=" * 70)

    total_panels         = sum(r["total"]           for r in results)
    total_broken         = sum(r["broken"]          for r in results)
    total_ext_fixed      = sum(r["extension_fixed"] for r in results)
    total_rescrape_fixed = sum(r["rescrape_fixed"]  for r in results)
    total_unfixable      = sum(r["unfixable"]       for r in results)

    log(f"Mangas validated       : {len(results)}")
    log(f"Total panels checked   : {total_panels}")
    log(f"Broken URLs found      : {total_broken}")
    log(f"Fixed (extension swap) : {total_ext_fixed}")
    log(f"Fixed (re-scraped)     : {total_rescrape_fixed}")
    log(f"Still broken           : {total_unfixable}")

    if total_unfixable > 0:
        log("\n✗ Panels that could not be fixed:", "ERROR")
        for r in results:
            for panel in r["unfixable_panels"]:
                log(
                    f"  - {r['manga']} | chapter_id={panel['chapter_id']} "
                    f"| panel={panel['panel_number']} | {panel['image_url']}",
                    "ERROR"
                )

    log("\n✓ Validation complete!")
    log("=" * 70)

    if total_unfixable > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
