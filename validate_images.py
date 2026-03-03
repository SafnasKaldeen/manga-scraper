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
MAX_WORKERS    = 20

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


# ─── Chapter number helpers (from auto-updater) ───────────────────────────────
def to_db_format(chapter_number) -> str:
    return f"{float(str(chapter_number)):.2f}"


def format_chapter_number_for_url(chapter_number) -> str:
    """Convert chapter number to mangaread URL format: 58.5 → '58-5', 58.0 → '58'"""
    val = float(str(chapter_number))
    if val == int(val):
        return str(int(val))
    clean = f"{val:.10g}"
    return clean.replace('.', '-')


# ─── URL checking ─────────────────────────────────────────────────────────────
def check_url(url: str) -> tuple[bool, int]:
    try:
        r = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if r.status_code == 405:
            r = requests.get(url, headers=HEADERS, timeout=10, stream=True)
            r.close()
        return r.ok, r.status_code
    except Exception as e:
        log(f"  Request error for {url}: {e}", "WARNING")
        return False, 0


def try_extension_variants(broken_url: str) -> str | None:
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


def update_all_panels_for_chapter(chapter_id: str, image_urls: list[str]) -> tuple[int, int]:
    """Delete and re-insert all panels for a chapter. Returns (success_count, fail_count)."""
    try:
        # Delete existing panels
        supabase.table("panels").delete().eq("chapter_id", chapter_id).execute()

        # Re-insert with fresh URLs
        panels_data = [
            {
                "chapter_id":    chapter_id,
                "panel_number":  idx,
                "image_url":     img_url,
                "created_at":    datetime.now().isoformat(),
            }
            for idx, img_url in enumerate(image_urls, 1)
        ]
        supabase.table("panels").insert(panels_data).execute()

        # Update total_panels count on chapter
        supabase.table("chapters").update({
            "total_panels": len(image_urls)
        }).eq("id", chapter_id).execute()

        log(f"  ✓ Re-inserted {len(image_urls)} panels for chapter_id={chapter_id}")
        return len(image_urls), 0
    except Exception as e:
        log(f"  ✗ Failed to update panels for chapter_id={chapter_id}: {e}", "ERROR")
        return 0, 1


# ─── Mangaread scraping ───────────────────────────────────────────────────────
def scrape_chapter_list(manga_slug: str) -> dict[str, str]:
    """
    Scrape the manga's chapter list page and return a dict of:
      { db_format_chapter_number → chapter_url }
    e.g. { "273.00" → "https://...chapter-273/", "273.50" → "https://...chapter-273-5/" }
    """
    manga_url = f"{MANGA_BASE_URL}{manga_slug}/"
    log(f"  📋 Scraping chapter list from: {manga_url}")

    try:
        response = requests.get(manga_url, headers=SCRAPE_HEADERS, timeout=15)
        response.raise_for_status()

        soup  = BeautifulSoup(response.content, "html.parser")
        links = soup.select("ul.main li a")

        chapter_map = {}
        for link in links:
            href = link.get("href")
            if not href or "/chapter-" not in href:
                continue
            match = re.search(r"chapter-([\d]+(?:\.[\d]+|-[\d]+)?)", href)
            if match:
                raw = match.group(1).replace("-", ".")
                try:
                    db_key      = to_db_format(raw)
                    full_url    = urljoin(manga_url, href)
                    chapter_map[db_key] = full_url
                except ValueError:
                    continue

        log(f"  📋 Found {len(chapter_map)} chapters in chapter list")
        return chapter_map

    except Exception as e:
        log(f"  ✗ Failed to scrape chapter list for {manga_slug}: {e}", "ERROR")
        return {}


def scrape_chapter_images(chapter_url: str) -> list[str]:
    try:
        response = requests.get(chapter_url, headers=SCRAPE_HEADERS, timeout=15)
        response.raise_for_status()
        soup   = BeautifulSoup(response.content, "html.parser")
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
                full_url = urljoin(chapter_url, img_url.strip())
                image_urls.append(full_url)

        return image_urls
    except Exception as e:
        log(f"  ✗ Failed to scrape images from {chapter_url}: {e}", "ERROR")
        return []


# ─── Fix strategies ───────────────────────────────────────────────────────────
def fix_single_panel_extension(panel: dict) -> str | None:
    """Try swapping the file extension on the existing URL."""
    return try_extension_variants(panel["image_url"])


def fix_chapter_via_relist(panel: dict, chapter_map: dict[str, str]) -> tuple[str, list[str]]:
    """
    Use the pre-fetched chapter_map to find the correct URL for this panel's chapter,
    then re-scrape all images from that page.
    Returns (correct_chapter_url, image_urls_list) — empty list if failed.
    """
    chapter_data   = panel.get("chapter", {})
    chapter_number = chapter_data.get("chapter_number")

    if not chapter_number:
        log(f"  ✗ No chapter_number in panel data", "ERROR")
        return "", []

    db_key = to_db_format(chapter_number)
    correct_url = chapter_map.get(db_key)

    if not correct_url:
        log(f"  ✗ Chapter {db_key} not found in chapter list — may have been removed from source", "WARNING")
        return "", []

    log(f"  🔍 Re-scraping correct chapter URL: {correct_url}")
    image_urls = scrape_chapter_images(correct_url)

    if not image_urls:
        log(f"  ✗ No images found at {correct_url}", "ERROR")

    return correct_url, image_urls


# ─── Per-manga validation ─────────────────────────────────────────────────────
def check_panel_worker(panel: dict) -> tuple[dict, bool, int]:
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

    broken_panels    = []
    checked          = 0
    extension_fixed  = 0
    relist_fixed     = 0
    unfixable_panels = []

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

    if not broken_panels or not FIX_URLS:
        if not FIX_URLS:
            unfixable_panels = [p for p, _ in broken_panels]
        log(f"\nResults for {manga_title}:")
        log(f"  Total checked     : {len(panels)}")
        log(f"  Broken            : {len(broken_panels)}")
        log(f"  Unfixable         : {len(unfixable_panels)}")
        return {
            "manga":            manga_title,
            "total":            len(panels),
            "broken":           len(broken_panels),
            "extension_fixed":  0,
            "relist_fixed":     relist_fixed,
            "unfixable":        len(unfixable_panels),
            "unfixable_panels": unfixable_panels,
        }

    # ── Group broken panels by chapter_id ───────────────────────────────────
    # This lets us fix an entire chapter in one re-scrape instead of
    # hitting mangaread once per panel (which was causing the repeated
    # "No images found" spam in the logs).
    broken_by_chapter: dict[str, list[dict]] = {}
    single_panel_broken: list[tuple[dict, int]] = []

    for panel, status in broken_panels:
        chapter_id = panel["chapter_id"]
        broken_by_chapter.setdefault(chapter_id, []).append(panel)

    # Chapters where ALL panels are broken → full chapter re-scrape is better
    # Chapters where only SOME panels are broken → try extension swap first
    full_chapter_ids = set()
    partial_chapter_ids = set()

    for chapter_id, ch_panels in broken_by_chapter.items():
        # Count total panels for this chapter from our full panels list
        total_in_chapter = sum(1 for p in panels if p["chapter_id"] == chapter_id)
        if len(ch_panels) == total_in_chapter:
            full_chapter_ids.add(chapter_id)
        else:
            partial_chapter_ids.add(chapter_id)

    log(f"  Chapters with ALL panels broken  : {len(full_chapter_ids)}")
    log(f"  Chapters with SOME panels broken : {len(partial_chapter_ids)}")

    # ── Fetch chapter list ONCE from mangaread for re-scrape fallback ────────
    chapter_map: dict[str, str] = {}
    if full_chapter_ids or partial_chapter_ids:
        log(f"  Fetching chapter list from mangaread to resolve correct URLs...")
        chapter_map = scrape_chapter_list(manga_slug)
        time.sleep(1)

    # ── Fix fully-broken chapters: re-scrape entire chapter ─────────────────
    for chapter_id in full_chapter_ids:
        ch_panels   = broken_by_chapter[chapter_id]
        sample      = ch_panels[0]
        chapter_num = sample.get("chapter", {}).get("chapter_number")

        log(f"\n  ── Full chapter fix: chapter_id={chapter_id} (chapter {chapter_num}) ──")

        correct_url, image_urls = fix_chapter_via_relist(sample, chapter_map)

        if image_urls:
            success, fail = update_all_panels_for_chapter(chapter_id, image_urls)
            if success:
                relist_fixed += len(ch_panels)
                log(f"  ✓ Re-listed chapter fixed: {len(ch_panels)} broken panels replaced with {success} fresh ones")
            else:
                unfixable_panels.extend(ch_panels)
        else:
            log(f"  ✗ Could not fix chapter_id={chapter_id} — chapter may be gone from source", "ERROR")
            unfixable_panels.extend(ch_panels)

        time.sleep(1)

    # ── Fix partially-broken chapters: try extension swap, then relist ───────
    for chapter_id in partial_chapter_ids:
        ch_panels = broken_by_chapter[chapter_id]

        log(f"\n  ── Partial chapter fix: chapter_id={chapter_id} ({len(ch_panels)} broken panels) ──")

        for panel in ch_panels:
            # Strategy 1: extension swap
            working = fix_single_panel_extension(panel)
            if working:
                if update_panel_url(panel["id"], working):
                    extension_fixed += 1
                    log(f"  ✓ Extension fix: panel {panel['panel_number']} → {working}")
                    continue
                else:
                    unfixable_panels.append(panel)
                    continue

            # Strategy 2: re-scrape the chapter and pick the right panel by index
            log(f"  Extension failed for panel {panel['panel_number']} — trying chapter re-scrape...")
            correct_url, image_urls = fix_chapter_via_relist(panel, chapter_map)

            if image_urls:
                panel_index = panel["panel_number"] - 1
                if 0 <= panel_index < len(image_urls):
                    fresh_url = image_urls[panel_index]
                    ok, _ = check_url(fresh_url)
                    if ok and update_panel_url(panel["id"], fresh_url):
                        relist_fixed += 1
                        log(f"  ✓ Re-scrape fix: panel {panel['panel_number']} → {fresh_url}")
                    else:
                        unfixable_panels.append(panel)
                else:
                    log(f"  ✗ Panel {panel['panel_number']} out of range ({len(image_urls)} images in chapter)", "ERROR")
                    unfixable_panels.append(panel)
            else:
                unfixable_panels.append(panel)

            time.sleep(0.5)

    log(f"\nResults for {manga_title}:")
    log(f"  Total checked         : {len(panels)}")
    log(f"  Broken                : {len(broken_panels)}")
    log(f"  Fixed (extension swap): {extension_fixed}")
    log(f"  Fixed (chapter relist): {relist_fixed}")
    log(f"  Unfixable             : {len(unfixable_panels)}")

    return {
        "manga":            manga_title,
        "total":            len(panels),
        "broken":           len(broken_panels),
        "extension_fixed":  extension_fixed,
        "relist_fixed":     relist_fixed,
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

    total_panels          = sum(r["total"]           for r in results)
    total_broken          = sum(r["broken"]          for r in results)
    total_ext_fixed       = sum(r["extension_fixed"] for r in results)
    total_relist_fixed    = sum(r["relist_fixed"]    for r in results)
    total_unfixable       = sum(r["unfixable"]       for r in results)

    log(f"Mangas validated          : {len(results)}")
    log(f"Total panels checked      : {total_panels}")
    log(f"Broken URLs found         : {total_broken}")
    log(f"Fixed (extension swap)    : {total_ext_fixed}")
    log(f"Fixed (chapter relist)    : {total_relist_fixed}")
    log(f"Still broken              : {total_unfixable}")

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
