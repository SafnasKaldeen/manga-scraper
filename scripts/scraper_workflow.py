#!/usr/bin/env python3
"""
Smart Manga Scraper - Only downloads NEW chapters
Triggered by GitHub Actions via Vercel webhook
"""

import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
import csv
from datetime import datetime
from io import BytesIO
import cloudinary
import cloudinary.uploader
import cloudinary.api

# ============================================
# CONFIGURATION
# ============================================

cloudinary.config(
    cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME'),
    api_key=os.environ.get('CLOUDINARY_API_KEY'),
    api_secret=os.environ.get('CLOUDINARY_API_SECRET')
)

CLOUDINARY_BASE = "manga"
METADATA_CSV = "../cloudinary_manga_metadata.csv"

# ============================================
# HELPER FUNCTIONS
# ============================================

def print_header(title):
    """Print formatted header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")


def ensure_cloudinary_folder(folder_path):
    """Create folder structure in Cloudinary"""
    try:
        cloudinary.api.create_folder(folder_path)
        return True
    except Exception as e:
        if 'already exists' in str(e).lower() or 'exist' in str(e).lower():
            return True
        print(f"⚠️  Folder creation warning: {str(e)[:50]}")
        return False


# ============================================
# METADATA MANAGEMENT
# ============================================

def load_existing_metadata(manga_slug):
    """Load existing metadata for a manga"""
    existing = {}
    
    if not os.path.exists(METADATA_CSV):
        return existing
    
    try:
        with open(METADATA_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['manga_slug'] == manga_slug:
                    chapter_num = int(row['chapter_number'])
                    existing[chapter_num] = {
                        'panel_count': int(row['panel_count']),
                        'expected_count': int(row.get('expected_count', row['panel_count'])),
                        'status': row['status']
                    }
    except Exception as e:
        print(f"⚠️  Could not load metadata: {e}")
    
    return existing


def update_metadata(manga_name, manga_slug, chapter_num, panel_count, expected_count, status, error=''):
    """Update metadata CSV with chapter information"""
    
    # Ensure CSV exists
    if not os.path.exists(METADATA_CSV):
        with open(METADATA_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'manga_name', 'manga_slug', 'chapter_number', 
                'panel_count', 'expected_count', 'status', 
                'timestamp', 'error', 'cloudinary_folder'
            ])
    
    # Read existing rows
    rows = []
    chapter_exists = False
    
    with open(METADATA_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            if row['manga_slug'] == manga_slug and int(row['chapter_number']) == chapter_num:
                # Update existing
                row['manga_name'] = manga_name
                row['panel_count'] = panel_count
                row['expected_count'] = expected_count
                row['status'] = status
                row['timestamp'] = datetime.now().isoformat()
                row['error'] = error
                row['cloudinary_folder'] = f"{CLOUDINARY_BASE}/{manga_slug}/chapter-{chapter_num:03d}"
                chapter_exists = True
            rows.append(row)
    
    # Write back
    with open(METADATA_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
        if not chapter_exists:
            writer.writerow({
                'manga_name': manga_name,
                'manga_slug': manga_slug,
                'chapter_number': chapter_num,
                'panel_count': panel_count,
                'expected_count': expected_count,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'error': error,
                'cloudinary_folder': f"{CLOUDINARY_BASE}/{manga_slug}/chapter-{chapter_num:03d}"
            })


# ============================================
# CHAPTER LIST MANAGEMENT
# ============================================

def get_all_chapters(manga_url):
    """Get list of all chapters from manga page"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
    print(f"🔍 Fetching chapter list from: {manga_url}")
    
    try:
        response = requests.get(manga_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        chapter_links = []
        links = soup.select('ul.main li a')
        
        for link in links:
            href = link.get('href')
            if href and '/chapter-' in href:
                match = re.search(r'chapter-(\d+)', href)
                if match:
                    chapter_num = int(match.group(1))
                    full_url = urljoin(manga_url, href)
                    chapter_links.append({
                        'url': full_url,
                        'number': chapter_num,
                        'text': link.get_text(strip=True)
                    })
        
        # Remove duplicates and sort
        unique_chapters = {ch['number']: ch for ch in chapter_links}
        sorted_chapters = sorted(unique_chapters.values(), key=lambda x: x['number'])
        
        print(f"✅ Found {len(sorted_chapters)} total chapters")
        
        return sorted_chapters
        
    except Exception as e:
        print(f"❌ Error fetching chapter list: {e}")
        return []


# ============================================
# CORE SCRAPING WITH DIRECT UPLOAD
# ============================================

def scrape_chapter_direct_to_cloudinary(chapter_url, manga_name, manga_slug, chapter_num):
    """
    Scrape chapter and upload directly to Cloudinary
    Returns: (panel_count, success_status, error_message, expected_count)
    """
    print(f"\n{'─'*80}")
    print(f"📥 Fetching Chapter {chapter_num}: {chapter_url}")
    print(f"{'─'*80}\n")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.mangaread.org/',
    }
    
    try:
        # Fetch chapter page
        response = requests.get(chapter_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        images = soup.select('.page-break.no-gaps img')
        expected_count = len(images)
        
        print(f"🔍 Found {expected_count} panels to upload")
        
        if expected_count == 0:
            update_metadata(manga_name, manga_slug, chapter_num, 0, 0, 'failed', 'No images found')
            return 0, False, "No images found", 0
        
        # Get image URLs
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
        
        if not image_urls:
            update_metadata(manga_name, manga_slug, chapter_num, 0, expected_count, 'failed', 'No valid image URLs')
            return 0, False, "No valid image URLs", expected_count
        
        # Create Cloudinary folder structure
        cloudinary_chapter_folder = f"{CLOUDINARY_BASE}/{manga_slug}/chapter-{chapter_num:03d}"
        
        print(f"☁️  Cloudinary folder: {cloudinary_chapter_folder}")
        ensure_cloudinary_folder(f"{CLOUDINARY_BASE}/{manga_slug}")
        ensure_cloudinary_folder(cloudinary_chapter_folder)
        
        # Upload images directly to Cloudinary
        uploaded_count = 0
        failed_count = 0
        
        print(f"\n📤 Uploading {len(image_urls)} panels directly to Cloudinary...\n")
        
        for idx, img_url in enumerate(image_urls, 1):
            try:
                # Download image to memory
                img_response = requests.get(img_url, headers=headers, timeout=15)
                img_response.raise_for_status()
                
                # Get file extension
                ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
                ext = ext.lstrip('.')
                
                # Normalize extension
                if ext.lower() == 'jpeg':
                    ext = 'jpg'
                
                # Create public_id (filename without extension)
                public_id = f"panel-{idx:03d}"
                
                # Upload to Cloudinary directly from memory
                upload_result = cloudinary.uploader.upload(
                    BytesIO(img_response.content),
                    folder=cloudinary_chapter_folder,
                    public_id=public_id,
                    overwrite=False,
                    resource_type="auto",
                    use_filename=False,
                    unique_filename=False,
                    format=ext
                )
                
                cloudinary_url = upload_result.get('secure_url')
                
                print(f"✅ [{idx}/{len(image_urls)}] Uploaded: panel-{idx:03d}.{ext}")
                uploaded_count += 1
                
                # Be polite - small delay
                time.sleep(0.3)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ [{idx}/{len(image_urls)}] Download failed: {str(e)[:60]}")
                failed_count += 1
            except Exception as e:
                print(f"❌ [{idx}/{len(image_urls)}] Upload failed: {str(e)[:60]}")
                failed_count += 1
        
        # Determine status
        if uploaded_count == expected_count:
            status = 'success'
            error_msg = ''
        elif uploaded_count > 0:
            status = 'partial'
            error_msg = f'Uploaded {uploaded_count}/{expected_count} panels'
        else:
            status = 'failed'
            error_msg = 'All uploads failed'
        
        # Update metadata
        update_metadata(
            manga_name, manga_slug, chapter_num,
            uploaded_count, expected_count, status, error_msg
        )
        
        # Summary
        print(f"\n{'─'*80}")
        print(f"📊 Chapter {chapter_num} Summary:")
        print(f"✅ Uploaded: {uploaded_count}/{expected_count}")
        print(f"❌ Failed: {failed_count}")
        print(f"📁 Cloudinary folder: {cloudinary_chapter_folder}")
        print(f"{'─'*80}\n")
        
        return uploaded_count, (uploaded_count == expected_count), error_msg, expected_count
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error: {error_msg}")
        update_metadata(manga_name, manga_slug, chapter_num, 0, 0, 'failed', error_msg)
        return 0, False, error_msg, 0


# ============================================
# SMART SCRAPING - NEW CHAPTERS ONLY
# ============================================

def scrape_new_chapters_only(manga_url, manga_name, manga_slug):
    """
    Smart scraper: Only downloads chapters that are NEW or FAILED
    """
    print_header(f"🆕 SMART SCRAPER: {manga_name}")
    
    # Get all available chapters
    all_chapters = get_all_chapters(manga_url)
    
    if not all_chapters:
        print("❌ Could not fetch chapter list")
        return {
            'success': False,
            'error': 'Could not fetch chapter list',
            'new_chapters': 0,
            'uploaded': 0
        }
    
    # Load existing metadata
    existing_metadata = load_existing_metadata(manga_slug)
    
    # Find NEW chapters (not in metadata OR failed/partial)
    new_chapters = []
    for chapter in all_chapters:
        chapter_num = chapter['number']
        
        if chapter_num not in existing_metadata:
            new_chapters.append(chapter)
            print(f"🆕 Chapter {chapter_num}: NEW")
        else:
            meta = existing_metadata[chapter_num]
            if meta['status'] != 'success' or meta['panel_count'] != meta.get('expected_count', meta['panel_count']):
                new_chapters.append(chapter)
                print(f"🔄 Chapter {chapter_num}: INCOMPLETE ({meta['status']})")
            else:
                print(f"✅ Chapter {chapter_num}: Already complete")
    
    if not new_chapters:
        print("\n✨ All chapters are up to date! Nothing to scrape.")
        return {
            'success': True,
            'new_chapters': 0,
            'uploaded': 0,
            'skipped': len(all_chapters),
            'message': 'All chapters already uploaded'
        }
    
    print(f"\n📊 Found {len(new_chapters)} new/incomplete chapters to scrape")
    print(f"☁️  Base folder: {CLOUDINARY_BASE}/{manga_slug}")
    
    success_count = 0
    failed_chapters = []
    
    for chapter in new_chapters:
        chapter_num = chapter['number']
        
        try:
            panel_count, success, error, expected = scrape_chapter_direct_to_cloudinary(
                chapter['url'],
                manga_name,
                manga_slug,
                chapter_num
            )
            
            if success and panel_count > 0:
                success_count += 1
            else:
                failed_chapters.append(chapter_num)
            
            # Delay between chapters
            time.sleep(2)
            
        except Exception as e:
            print(f"❌ Error processing chapter {chapter_num}: {e}")
            failed_chapters.append(chapter_num)
    
    # Final summary
    print_header("📊 SCRAPING SUMMARY")
    print(f"🆕 New chapters found: {len(new_chapters)}")
    print(f"✅ Successfully uploaded: {success_count}/{len(new_chapters)}")
    print(f"❌ Failed: {len(failed_chapters)}")
    
    if failed_chapters:
        print(f"\n❌ Failed chapter numbers: {', '.join(map(str, failed_chapters))}")
    
    print(f"\n☁️  Cloudinary folder: {CLOUDINARY_BASE}/{manga_slug}")
    print(f"📄 Metadata: {METADATA_CSV}")
    print("="*80 + "\n")
    
    return {
        'success': True,
        'new_chapters': len(new_chapters),
        'uploaded': success_count,
        'failed': len(failed_chapters),
        'failed_chapters': failed_chapters
    }


# ============================================
# MAIN ENTRY POINT
# ============================================

def main():
    """Main entry point for GitHub Actions"""
    
    if len(sys.argv) < 3:
        print("❌ Usage: python scrape_new_chapters.py <manga_slug> <manga_url> [manga_name]")
        sys.exit(1)
    
    manga_slug = sys.argv[1]
    manga_url = sys.argv[2]
    manga_name = sys.argv[3] if len(sys.argv) > 3 else manga_slug.replace('-', ' ').title()
    
    print_header("🤖 GITHUB ACTIONS MANGA SCRAPER")
    print(f"📚 Manga: {manga_name}")
    print(f"🔗 URL: {manga_url}")
    print(f"🏷️  Slug: {manga_slug}")
    
    try:
        result = scrape_new_chapters_only(manga_url, manga_name, manga_slug)
        
        if result['success']:
            print("\n✅ Scraping completed successfully!")
            sys.exit(0)
        else:
            print(f"\n❌ Scraping failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()