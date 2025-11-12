import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
from datetime import datetime
from supabase import create_client, Client

# Supabase Configuration
SUPABASE_URL = "https://ppfbpmbomksqlgojwdhr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwZmJwbWJvbWtzcWxnb2p3ZGhyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA4NTQ5NDMsImV4cCI6MjA3NjQzMDk0M30.5j7kSkZhoMZgvCGcxdG2phuoN3dwout3JgD1i1cUqaY"


if not SUPABASE_KEY:
    print("✗ Error: SUPABASE_KEY environment variable must be set")
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_manga_slug_from_url(manga_url):
    """Extract slug from manga URL"""
    match = re.search(r'/manga/([^/]+)', manga_url)
    return match.group(1) if match else None


def scrape_chapter_urls(url):
    """
    Scrape image URLs from a single chapter.
    Returns: (image_urls, success_status, error_message)
    """
    print(f"\nFetching page: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.mangaread.org/',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find images only in page-break no-gaps class
        images = soup.select('.page-break.no-gaps img')
        
        print(f"Found {len(images)} images in .page-break.no-gaps")
        
        # Extract image URLs in order
        image_urls = []
        for img in images:
            # Try different attributes where image URL might be stored
            img_url = (img.get('src') or 
                      img.get('data-src') or 
                      img.get('data-lazy-src') or
                      img.get('data-original'))
            
            if img_url:
                # Strip whitespace/newlines from URL
                img_url = img_url.strip()
                # Convert relative URLs to absolute
                full_url = urljoin(url, img_url)
                image_urls.append(full_url)
        
        print(f"Extracted {len(image_urls)} image URLs")
        
        if not image_urls:
            return [], False, "No images found"
        
        return image_urls, True, ""
        
    except Exception as e:
        error_msg = str(e)
        print(f"✗ Error: {error_msg}")
        return [], False, error_msg


def save_manga_to_supabase(manga_name, manga_slug, source_url):
    """
    Save or update manga in Supabase.
    Returns: manga_id (UUID)
    """
    try:
        # Check if manga exists
        existing = supabase.table('mangas').select('id').eq('slug', manga_slug).execute()
        
        if existing.data:
            # Update existing manga
            manga_id = existing.data[0]['id']
            supabase.table('mangas').update({
                'title': manga_name,
                'updated_at': datetime.now().isoformat()
            }).eq('id', manga_id).execute()
            print(f"✓ Updated manga: {manga_name} (ID: {manga_id})")
        else:
            # Insert new manga
            result = supabase.table('mangas').insert({
                'title': manga_name,
                'slug': manga_slug,
                'description': f'Scraped from {source_url}',
                'status': 'ongoing',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }).execute()
            manga_id = result.data[0]['id']
            print(f"✓ Created manga: {manga_name} (ID: {manga_id})")
        
        return manga_id
    
    except Exception as e:
        print(f"✗ Error saving manga to Supabase: {e}")
        raise


def save_chapter_to_supabase(manga_id, chapter_number, chapter_title, image_urls):
    """
    Save chapter and its panels to Supabase.
    Handles decimal chapter numbers (e.g., 100.5).
    Returns: (chapter_id, success)
    """
    try:
        # Convert chapter_number to appropriate type
        if isinstance(chapter_number, str):
            chapter_number = float(chapter_number)
        
        # Convert to int if it's a whole number (e.g., 15.0 -> 15)
        # This avoids issues with bigint columns in Supabase
        if isinstance(chapter_number, float) and chapter_number.is_integer():
            chapter_number = int(chapter_number)
        
        # Check if chapter exists - use numeric comparison for decimal chapters
        existing = supabase.table('chapters').select('id').eq('manga_id', manga_id).eq('chapter_number', chapter_number).execute()
        
        if existing.data:
            # Update existing chapter
            chapter_id = existing.data[0]['id']
            
            # Delete existing panels for this chapter
            supabase.table('panels').delete().eq('chapter_id', chapter_id).execute()
            
            # Update chapter
            supabase.table('chapters').update({
                'title': chapter_title,
                'total_panels': len(image_urls),
                'published_at': datetime.now().isoformat()
            }).eq('id', chapter_id).execute()
            print(f"  ✓ Updated chapter {chapter_number} (ID: {chapter_id})")
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
            print(f"  ✓ Created chapter {chapter_number} (ID: {chapter_id})")
        
        # Insert panels in batch
        panels_data = []
        for idx, img_url in enumerate(image_urls, 1):
            panels_data.append({
                'chapter_id': chapter_id,
                'panel_number': idx,
                'image_url': img_url,
                'created_at': datetime.now().isoformat()
            })
        
        if panels_data:
            supabase.table('panels').insert(panels_data).execute()
            print(f"  ✓ Saved {len(panels_data)} panels")
        
        # Update manga total_chapters and total_panels
        update_manga_stats(manga_id)
        
        return chapter_id, True
    
    except Exception as e:
        print(f"  ✗ Error saving chapter to Supabase: {e}")
        return None, False


def update_manga_stats(manga_id):
    """Update manga statistics (total chapters and panels)"""
    try:
        # Get all chapters for this manga
        chapters = supabase.table('chapters').select('total_panels').eq('manga_id', manga_id).execute()
        
        total_chapters = len(chapters.data)
        total_panels = sum(ch.get('total_panels', 0) for ch in chapters.data)
        
        # Update manga
        supabase.table('mangas').update({
            'total_chapters': total_chapters,
            'total_panels': total_panels,
            'updated_at': datetime.now().isoformat()
        }).eq('id', manga_id).execute()
        
    except Exception as e:
        print(f"  ⚠ Warning: Could not update manga stats: {e}")


def get_all_chapters(manga_url):
    """Get list of all chapters from the manga page"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
    print(f"Fetching chapter list from: {manga_url}")
    
    try:
        response = requests.get(manga_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all chapter links
        chapter_links = []
        links = soup.select('ul.main li a')
        
        print(f"Found {len(links)} chapter links")
        
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
                        print(f"  ⚠ Skipping invalid chapter number: {chapter_num_str}")
                        continue
        
        # Remove duplicates and sort by chapter number
        unique_chapters = {ch['number']: ch for ch in chapter_links}
        sorted_chapters = sorted(unique_chapters.values(), key=lambda x: x['number'])
        
        print(f"✓ Found {len(sorted_chapters)} unique chapters")
        if sorted_chapters:
            print(f"  First: Chapter {sorted_chapters[0]['number']}")
            print(f"  Last: Chapter {sorted_chapters[-1]['number']}")
        
        return sorted_chapters
        
    except Exception as e:
        print(f"✗ Error fetching chapter list: {e}")
        return []


def scrape_manga_to_supabase(manga_url, manga_name, manga_slug, start_chapter=1, end_chapter=None, max_retries=3):
    """
    Scrape manga chapters and save URLs to Supabase.
    Automatically retries failed chapters.
    """
    print(f"\n{'='*60}")
    print(f"SCRAPING MANGA TO SUPABASE")
    print(f"{'='*60}\n")
    
    # Save manga info
    try:
        manga_id = save_manga_to_supabase(manga_name, manga_slug, manga_url)
    except Exception as e:
        print(f"✗ Failed to save manga: {e}")
        return
    
    # Get all chapters
    chapters = get_all_chapters(manga_url)
    
    if not chapters:
        print("\n✗ Could not fetch chapter list")
        return
    
    # Filter by range
    if start_chapter or end_chapter:
        chapters = [ch for ch in chapters 
                   if (ch['number'] >= start_chapter) 
                   and (not end_chapter or ch['number'] <= end_chapter)]
    
    print(f"\n{'='*60}")
    print(f"Will process {len(chapters)} chapters")
    print(f"Manga: {manga_name} ({manga_slug})")
    print(f"Manga ID: {manga_id}")
    print(f"{'='*60}\n")
    
    success_count = 0
    failed_chapters = []
    
    # First pass: scrape all chapters
    for idx, chapter in enumerate(chapters, 1):
        chapter_num = chapter['number']
        chapter_url = chapter['url']
        chapter_title = chapter['text']
        
        print(f"\n[{idx}/{len(chapters)}] {'='*50}")
        print(f"Chapter {chapter_num}: {chapter_title}")
        print(f"{'='*60}")
        
        try:
            # Scrape image URLs
            image_urls, success, error = scrape_chapter_urls(chapter_url)
            
            if success and image_urls:
                # Save to Supabase
                chapter_id, saved = save_chapter_to_supabase(
                    manga_id, chapter_num, chapter_title, image_urls
                )
                
                if saved:
                    success_count += 1
                    print(f"  ✓ Chapter {chapter_num} saved successfully!")
                else:
                    failed_chapters.append(chapter)
            else:
                print(f"  ✗ Failed to scrape: {error}")
                failed_chapters.append(chapter)
            
            # Be polite to the server
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user")
            break
        except Exception as e:
            print(f"  ✗ Error processing chapter {chapter_num}: {e}")
            failed_chapters.append(chapter)
    
    # Retry failed chapters
    retry_count = 0
    while failed_chapters and retry_count < max_retries:
        retry_count += 1
        print(f"\n{'='*60}")
        print(f"RETRY ATTEMPT {retry_count}/{max_retries}")
        print(f"Retrying {len(failed_chapters)} failed chapters")
        print(f"{'='*60}\n")
        
        still_failed = []
        
        for idx, chapter in enumerate(failed_chapters, 1):
            chapter_num = chapter['number']
            chapter_url = chapter['url']
            chapter_title = chapter['text']
            
            print(f"\n[Retry {idx}/{len(failed_chapters)}] {'='*40}")
            print(f"Chapter {chapter_num}: {chapter_title}")
            print(f"{'='*60}")
            
            try:
                # Scrape image URLs
                image_urls, success, error = scrape_chapter_urls(chapter_url)
                
                if success and image_urls:
                    # Save to Supabase
                    chapter_id, saved = save_chapter_to_supabase(
                        manga_id, chapter_num, chapter_title, image_urls
                    )
                    
                    if saved:
                        success_count += 1
                        print(f"  ✓ Chapter {chapter_num} saved successfully on retry!")
                    else:
                        still_failed.append(chapter)
                else:
                    print(f"  ✗ Still failed: {error}")
                    still_failed.append(chapter)
                
                # Be polite to the server
                time.sleep(3)  # Longer delay for retries
                
            except KeyboardInterrupt:
                print("\n\n⚠ Retry interrupted by user")
                break
            except Exception as e:
                print(f"  ✗ Error on retry: {e}")
                still_failed.append(chapter)
        
        failed_chapters = still_failed
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}")
    print(f"✓ Successfully saved: {success_count}/{len(chapters)} chapters")
    if failed_chapters:
        print(f"\n✗ Failed chapters after {retry_count} retries:")
        for ch in failed_chapters:
            print(f"   - Chapter {ch['number']}: {ch['text']}")
    else:
        print(f"\n🎉 All chapters scraped successfully!")
    print(f"\nManga ID: {manga_id}")
    print(f"{'='*60}\n")


def verify_manga_in_supabase(manga_slug):
    """Verify manga data in Supabase"""
    try:
        # Get manga
        manga = supabase.table('mangas').select('*').eq('slug', manga_slug).execute()
        
        if not manga.data:
            print(f"✗ Manga '{manga_slug}' not found in Supabase")
            return
        
        manga_data = manga.data[0]
        manga_id = manga_data['id']
        
        print(f"\n{'='*60}")
        print(f"MANGA: {manga_data['title']}")
        print(f"{'='*60}")
        print(f"ID: {manga_id}")
        print(f"Slug: {manga_data['slug']}")
        print(f"Status: {manga_data.get('status', 'N/A')}")
        print(f"Total Chapters: {manga_data.get('total_chapters', 0)}")
        print(f"Total Panels: {manga_data.get('total_panels', 0)}")
        
        # Get chapters
        chapters = supabase.table('chapters').select('*').eq('manga_id', manga_id).order('chapter_number').execute()
        
        print(f"\nChapters in Database: {len(chapters.data)}")
        
        if chapters.data:
            # Get total panel count from chapters
            total_panels = sum(ch.get('total_panels', 0) for ch in chapters.data)
            print(f"Total Panels (from chapters): {total_panels}")
            
            print(f"\n{'='*60}")
            print("CHAPTER DETAILS (First 10)")
            print(f"{'='*60}")
            
            for ch in chapters.data[:10]:
                print(f"✓ Chapter {ch['chapter_number']}: {ch.get('title', 'Untitled')} - {ch.get('total_panels', 0)} panels")
            
            if len(chapters.data) > 10:
                print(f"... and {len(chapters.data) - 10} more chapters")
        
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"✗ Error verifying manga: {e}")


def main():
    """Main function - GitHub Actions mode only"""
    print("=" * 60)
    print("Manga URL Scraper - Supabase Edition")
    print("=" * 60)
    
    # Get environment variables
    manga_url = os.environ.get("MANGA_URL")
    operation = os.environ.get("OPERATION")
    
    if not manga_url or not operation:
        print("\n✗ Error: MANGA_URL and OPERATION environment variables are required")
        print("This script is designed to run in GitHub Actions.")
        print("\nRequired environment variables:")
        print("  - MANGA_URL: The manga URL to scrape")
        print("  - OPERATION: scrape_all, scrape_range, scrape_single, or verify")
        print("  - START_CHAPTER: (optional) Starting chapter number")
        print("  - END_CHAPTER: (optional) Ending chapter number")
        print("  - MANGA_NAME: (optional) Manga name")
        sys.exit(1)
    
    # GitHub Actions mode
    print(f"\n🤖 Running in GitHub Actions mode")
    print(f"Operation: {operation}")
    
    manga_slug = get_manga_slug_from_url(manga_url)
    if not manga_slug:
        print("✗ Invalid manga URL format")
        print("URL should be like: https://www.mangaread.org/manga/one-piece/")
        sys.exit(1)
    
    manga_name = os.environ.get("MANGA_NAME") or manga_slug.replace('-', ' ').title()
    
    # Handle start_chapter as float
    start_chapter_str = os.environ.get("START_CHAPTER", "1")
    try:
        start_chapter = float(start_chapter_str)
    except ValueError:
        start_chapter = 1.0
    
    # Handle end_chapter as float
    end_chapter_str = os.environ.get("END_CHAPTER")
    end_chapter = None
    if end_chapter_str and end_chapter_str.strip():
        try:
            end_chapter = float(end_chapter_str)
        except ValueError:
            end_chapter = None
    
    print(f"Manga: {manga_name} ({manga_slug})")
    print(f"URL: {manga_url}")
    if operation in ['scrape_range', 'scrape_single']:
        print(f"Chapter range: {start_chapter} to {end_chapter or 'end'}")
    print("=" * 60)
    
    try:
        if operation == "scrape_all":
            scrape_manga_to_supabase(manga_url, manga_name, manga_slug)
        elif operation == "scrape_range":
            scrape_manga_to_supabase(manga_url, manga_name, manga_slug, start_chapter, end_chapter)
        elif operation == "scrape_single":
            scrape_manga_to_supabase(manga_url, manga_name, manga_slug, start_chapter, start_chapter)
        elif operation == "verify":
            verify_manga_in_supabase(manga_slug)
        else:
            print(f"✗ Unknown operation: {operation}")
            print("Valid operations: scrape_all, scrape_range, scrape_single, verify")
            sys.exit(1)
        
        print("\n✓ Script completed successfully")
        
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()