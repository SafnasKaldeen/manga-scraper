import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
from datetime import datetime
from supabase import create_client, Client

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
FORCE_UPDATE = os.environ.get("FORCE_UPDATE", "false").lower() == "true"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("✗ Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set")
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Base URL for manga source
MANGA_BASE_URL = "https://www.mangaread.org/manga/"


def log_message(message, level="INFO"):
    """Log message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def get_all_mangas_from_supabase():
    """Fetch all mangas from Supabase"""
    try:
        log_message("Fetching all mangas from Supabase...")
        result = supabase.table('mangas').select('id, title, slug').execute()
        log_message(f"Found {len(result.data)} mangas in database")
        return result.data
    except Exception as e:
        log_message(f"Error fetching mangas: {e}", "ERROR")
        return []


def get_existing_chapters_from_supabase(manga_id):
    """Get list of existing chapter numbers for a manga"""
    try:
        result = supabase.table('chapters').select('chapter_number').eq('manga_id', manga_id).execute()
        # Convert to set of floats for easy comparison
        existing = set()
        for ch in result.data:
            num = ch['chapter_number']
            # Handle both int and float chapter numbers
            if isinstance(num, (int, float)):
                existing.add(float(num))
        return existing
    except Exception as e:
        log_message(f"Error fetching existing chapters: {e}", "ERROR")
        return set()


def get_available_chapters_from_source(manga_slug):
    """Scrape available chapters from mangaread.org"""
    manga_url = f"{MANGA_BASE_URL}{manga_slug}/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
    try:
        log_message(f"Fetching chapter list from: {manga_url}")
        response = requests.get(manga_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all chapter links
        chapter_links = []
        links = soup.select('ul.main li a')
        
        for link in links:
            href = link.get('href')
            if href and '/chapter-' in href:
                # Extract chapter number (supports decimals like 100.5)
                match = re.search(r'chapter-([\d.]+)', href)
                if match:
                    chapter_num_str = match.group(1)
                    try:
                        chapter_num = float(chapter_num_str)
                        full_url = urljoin(manga_url, href)
                        chapter_links.append({
                            'url': full_url,
                            'number': chapter_num,
                            'text': link.get_text(strip=True)
                        })
                    except ValueError:
                        continue
        
        # Remove duplicates
        unique_chapters = {ch['number']: ch for ch in chapter_links}
        log_message(f"Found {len(unique_chapters)} unique chapters")
        
        return unique_chapters
        
    except Exception as e:
        log_message(f"Error fetching chapter list: {e}", "ERROR")
        return {}


def find_missing_chapters(existing_chapters, available_chapters):
    """Compare existing vs available chapters and return missing ones"""
    available_nums = set(available_chapters.keys())
    
    if FORCE_UPDATE:
        log_message("FORCE_UPDATE enabled - will update all chapters")
        missing = available_nums
    else:
        missing = available_nums - existing_chapters
    
    missing_chapter_list = []
    for num in sorted(missing):
        missing_chapter_list.append(available_chapters[num])
    
    return missing_chapter_list


def scrape_chapter_images(chapter_url):
    """Scrape image URLs from a single chapter"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.mangaread.org/',
    }
    
    try:
        response = requests.get(chapter_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find images only in page-break no-gaps class
        images = soup.select('.page-break.no-gaps img')
        
        # Extract image URLs in order
        image_urls = []
        for img in images:
            img_url = (img.get('src') or 
                      img.get('data-src') or 
                      img.get('data-lazy-src') or
                      img.get('data-original'))
            
            if img_url:
                img_url = img_url.strip()
                full_url = urljoin(chapter_url, img_url)
                image_urls.append(full_url)
        
        return image_urls, True, ""
        
    except Exception as e:
        return [], False, str(e)


def save_chapter_to_supabase(manga_id, chapter_number, chapter_title, image_urls):
    """Save or update chapter and its panels to Supabase"""
    try:
        # Convert chapter_number to appropriate type
        if isinstance(chapter_number, str):
            chapter_number = float(chapter_number)
        
        if isinstance(chapter_number, float) and chapter_number.is_integer():
            chapter_number = int(chapter_number)
        
        # Check if chapter exists
        existing = supabase.table('chapters').select('id').eq('manga_id', manga_id).eq('chapter_number', chapter_number).execute()
        
        if existing.data:
            # Update existing chapter
            chapter_id = existing.data[0]['id']
            
            # Delete existing panels
            supabase.table('panels').delete().eq('chapter_id', chapter_id).execute()
            
            # Update chapter
            supabase.table('chapters').update({
                'title': chapter_title,
                'total_panels': len(image_urls),
                'published_at': datetime.now().isoformat()
            }).eq('id', chapter_id).execute()
            log_message(f"Updated chapter {chapter_number}")
        else:
            # Insert new chapter
            result = supabase.table('chapters').insert({
                'manga_id': manga_id,
                'chapter_number': chapter_number,
                'title': chapter_title,
                'total_panels': len(image_urls),
                'published_at': datetime.now().isoformat(),
                'created_at': datetime.now().isoformat()
            }).execute()
            chapter_id = result.data[0]['id']
            log_message(f"Created new chapter {chapter_number}")
        
        # Insert panels in batch
        if image_urls:
            panels_data = []
            for idx, img_url in enumerate(image_urls, 1):
                panels_data.append({
                    'chapter_id': chapter_id,
                    'panel_number': idx,
                    'image_url': img_url,
                    'created_at': datetime.now().isoformat()
                })
            
            supabase.table('panels').insert(panels_data).execute()
            log_message(f"Saved {len(panels_data)} panels for chapter {chapter_number}")
        
        return True
    
    except Exception as e:
        log_message(f"Error saving chapter: {e}", "ERROR")
        return False


def update_manga_stats(manga_id):
    """Update manga statistics"""
    try:
        chapters = supabase.table('chapters').select('total_panels').eq('manga_id', manga_id).execute()
        
        total_chapters = len(chapters.data)
        total_panels = sum(ch.get('total_panels', 0) for ch in chapters.data)
        
        supabase.table('mangas').update({
            'total_chapters': total_chapters,
            'total_panels': total_panels,
            'updated_at': datetime.now().isoformat()
        }).eq('id', manga_id).execute()
        
        log_message(f"Updated manga stats: {total_chapters} chapters, {total_panels} panels")
        
    except Exception as e:
        log_message(f"Error updating manga stats: {e}", "WARNING")


def process_manga(manga):
    """Process a single manga - check for new chapters and update"""
    manga_id = manga['id']
    manga_title = manga['title']
    manga_slug = manga['slug']
    
    log_message("=" * 70)
    log_message(f"Processing: {manga_title} ({manga_slug})")
    log_message("=" * 70)
    
    try:
        # Step 1: Get existing chapters from Supabase
        existing_chapters = get_existing_chapters_from_supabase(manga_id)
        log_message(f"Existing chapters in DB: {len(existing_chapters)}")
        
        # Step 2: Get available chapters from source
        available_chapters = get_available_chapters_from_source(manga_slug)
        
        if not available_chapters:
            log_message("No chapters found on source website", "WARNING")
            return {
                'manga': manga_title,
                'status': 'failed',
                'reason': 'No chapters found',
                'new_chapters': 0,
                'failed_chapters': 0
            }
        
        # Step 3: Find missing chapters
        missing_chapters = find_missing_chapters(existing_chapters, available_chapters)
        
        if not missing_chapters:
            log_message("✓ All chapters up to date!")
            return {
                'manga': manga_title,
                'status': 'up_to_date',
                'new_chapters': 0,
                'failed_chapters': 0
            }
        
        log_message(f"Found {len(missing_chapters)} new chapters to scrape")
        
        # Step 4: Scrape and save missing chapters
        success_count = 0
        failed_count = 0
        
        for idx, chapter in enumerate(missing_chapters, 1):
            chapter_num = chapter['number']
            chapter_url = chapter['url']
            chapter_title = chapter['text']
            
            log_message(f"[{idx}/{len(missing_chapters)}] Scraping chapter {chapter_num}...")
            
            try:
                # Scrape images
                image_urls, success, error = scrape_chapter_images(chapter_url)
                
                if success and image_urls:
                    # Save to Supabase
                    if save_chapter_to_supabase(manga_id, chapter_num, chapter_title, image_urls):
                        success_count += 1
                        log_message(f"✓ Chapter {chapter_num} saved successfully")
                    else:
                        failed_count += 1
                        log_message(f"✗ Failed to save chapter {chapter_num}", "ERROR")
                else:
                    failed_count += 1
                    log_message(f"✗ Failed to scrape chapter {chapter_num}: {error}", "ERROR")
                
                # Be polite to the server
                time.sleep(2)
                
            except Exception as e:
                failed_count += 1
                log_message(f"✗ Error processing chapter {chapter_num}: {e}", "ERROR")
        
        # Update manga statistics
        update_manga_stats(manga_id)
        
        log_message(f"✓ Completed: {success_count} new, {failed_count} failed")
        
        return {
            'manga': manga_title,
            'status': 'updated',
            'new_chapters': success_count,
            'failed_chapters': failed_count
        }
        
    except Exception as e:
        log_message(f"✗ Fatal error processing manga: {e}", "ERROR")
        return {
            'manga': manga_title,
            'status': 'failed',
            'reason': str(e),
            'new_chapters': 0,
            'failed_chapters': 0
        }


def main():
    """Main function - Auto update all mangas"""
    log_message("=" * 70)
    log_message("AUTO MANGA UPDATER - STARTING")
    log_message("=" * 70)
    log_message(f"Force Update: {FORCE_UPDATE}")
    log_message("")
    
    # Get all mangas from Supabase
    mangas = get_all_mangas_from_supabase()
    
    if not mangas:
        log_message("No mangas found in database", "ERROR")
        sys.exit(1)
    
    log_message(f"Will process {len(mangas)} mangas")
    log_message("")
    
    # Process each manga
    results = []
    
    for idx, manga in enumerate(mangas, 1):
        log_message(f"\n{'=' * 70}")
        log_message(f"MANGA {idx}/{len(mangas)}")
        log_message(f"{'=' * 70}\n")
        
        result = process_manga(manga)
        results.append(result)
        
        # Delay between mangas to avoid rate limiting
        if idx < len(mangas):
            time.sleep(5)
    
    # Summary
    log_message("\n" + "=" * 70)
    log_message("UPDATE SUMMARY")
    log_message("=" * 70)
    
    total_new = sum(r['new_chapters'] for r in results)
    total_failed = sum(r['failed_chapters'] for r in results)
    up_to_date = sum(1 for r in results if r['status'] == 'up_to_date')
    updated = sum(1 for r in results if r['status'] == 'updated')
    failed = sum(1 for r in results if r['status'] == 'failed')
    
    log_message(f"Total Mangas Processed: {len(results)}")
    log_message(f"  - Up to date: {up_to_date}")
    log_message(f"  - Updated: {updated}")
    log_message(f"  - Failed: {failed}")
    log_message(f"\nTotal New Chapters Added: {total_new}")
    log_message(f"Total Failed Chapters: {total_failed}")
    
    if failed > 0:
        log_message("\n✗ Failed Mangas:", "ERROR")
        for r in results:
            if r['status'] == 'failed':
                reason = r.get('reason', 'Unknown error')
                log_message(f"  - {r['manga']}: {reason}", "ERROR")
    
    log_message("\n✓ Auto update completed!")
    log_message("=" * 70)


if __name__ == "__main__":
    main()
