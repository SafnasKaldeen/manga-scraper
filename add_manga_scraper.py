#!/usr/bin/env python3
"""
Add New Manga Scraper
Scrapes manga chapters and panels from MangaRead and populates Supabase tables
"""

import os
import sys
import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from supabase import create_client, Client
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'manga_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def setup_driver():
    """Setup headless Chrome driver"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=options)
    return driver


def init_supabase():
    """Initialize Supabase client"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
    
    return create_client(url, key)


def create_or_get_genres(supabase: Client, genre_names: list) -> list:
    """
    Create genres if they don't exist and return their IDs
    
    Args:
        supabase: Supabase client
        genre_names: List of genre names
        
    Returns:
        List of genre IDs
    """
    genre_ids = []
    
    for genre_name in genre_names:
        genre_name = genre_name.strip()
        genre_slug = genre_name.lower().replace(' ', '-')
        
        try:
            # Check if genre exists
            result = supabase.table('genres').select('id').eq('slug', genre_slug).execute()
            
            if result.data:
                genre_id = result.data[0]['id']
                logger.info(f"   Genre '{genre_name}' already exists")
            else:
                # Create new genre
                new_genre = supabase.table('genres').insert({
                    'name': genre_name,
                    'slug': genre_slug
                }).execute()
                genre_id = new_genre.data[0]['id']
                logger.info(f"   Created new genre '{genre_name}'")
            
            genre_ids.append(genre_id)
            
        except Exception as e:
            logger.error(f"   Error processing genre '{genre_name}': {e}")
            continue
    
    return genre_ids


def create_manga(supabase: Client, manga_data: dict) -> str:
    """
    Create manga entry in database
    
    Args:
        supabase: Supabase client
        manga_data: Dictionary with manga information
        
    Returns:
        Manga ID (UUID)
    """
    logger.info("📚 Creating manga entry...")
    
    # Check if manga already exists
    existing = supabase.table('mangas').select('id').eq('slug', manga_data['slug']).execute()
    
    if existing.data:
        manga_id = existing.data[0]['id']
        logger.info(f"   ⚠️  Manga already exists with ID: {manga_id}")
        logger.info(f"   Updating existing manga...")
        
        # Update existing manga
        supabase.table('mangas').update({
            'title': manga_data['title'],
            'description': manga_data.get('description'),
            'cover_image_url': manga_data['cover_image_url'],
            'author': manga_data.get('author'),
            'status': manga_data.get('status', 'ongoing'),
            'publication_year': manga_data.get('publication_year'),
            'isLocked': manga_data.get('is_locked', False)
        }).eq('id', manga_id).execute()
        
        return manga_id
    
    # Create new manga
    manga_insert = {
        'title': manga_data['title'],
        'slug': manga_data['slug'],
        'description': manga_data.get('description'),
        'cover_image_url': manga_data['cover_image_url'],
        'author': manga_data.get('author'),
        'status': manga_data.get('status', 'ongoing'),
        'publication_year': manga_data.get('publication_year'),
        'isLocked': manga_data.get('is_locked', False)
    }
    
    result = supabase.table('mangas').insert(manga_insert).execute()
    manga_id = result.data[0]['id']
    
    logger.info(f"   ✅ Created manga with ID: {manga_id}")
    
    # Link genres
    if manga_data.get('genre_ids'):
        logger.info(f"   🏷️  Linking {len(manga_data['genre_ids'])} genres...")
        for genre_id in manga_data['genre_ids']:
            try:
                supabase.table('manga_genres').insert({
                    'manga_id': manga_id,
                    'genre_id': genre_id
                }).execute()
            except Exception as e:
                logger.error(f"   Error linking genre {genre_id}: {e}")
    
    return manga_id


def scrape_chapter_list(driver, manga_slug: str, max_chapters: int = 0):
    """
    Scrape list of chapters from MangaRead
    
    Args:
        driver: Selenium WebDriver
        manga_slug: Manga slug
        max_chapters: Maximum chapters to scrape (0 = all)
        
    Returns:
        List of chapter dictionaries
    """
    url = f"https://www.mangaread.org/manga/{manga_slug}/"
    logger.info(f"🔍 Fetching chapter list from: {url}")
    
    driver.get(url)
    time.sleep(3)
    
    chapters = []
    
    try:
        # Wait for chapter list to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chapter-list, .wp-manga-chapter, [class*='chapter']"))
        )
        
        # Try multiple selectors for chapter links
        chapter_selectors = [
            ".wp-manga-chapter a",
            ".chapter-list a",
            "li.wp-manga-chapter a",
            "[class*='chapter'] a"
        ]
        
        chapter_elements = []
        for selector in chapter_selectors:
            chapter_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if chapter_elements:
                logger.info(f"   Found {len(chapter_elements)} chapters using selector: {selector}")
                break
        
        if not chapter_elements:
            logger.error("   ❌ No chapters found with any selector")
            return chapters
        
        # Extract chapter info
        for elem in chapter_elements[:max_chapters] if max_chapters > 0 else chapter_elements:
            try:
                chapter_url = elem.get_attribute('href')
                chapter_text = elem.text.strip()
                
                # Extract chapter number from text or URL
                import re
                chapter_match = re.search(r'chapter[- ]?(\d+(?:\.\d+)?)', chapter_text, re.IGNORECASE)
                if not chapter_match:
                    chapter_match = re.search(r'chapter[- ]?(\d+(?:\.\d+)?)', chapter_url, re.IGNORECASE)
                
                if chapter_match:
                    chapter_number = float(chapter_match.group(1))
                    
                    chapters.append({
                        'chapter_number': chapter_number,
                        'title': chapter_text,
                        'url': chapter_url
                    })
                    
            except Exception as e:
                logger.error(f"   Error parsing chapter element: {e}")
                continue
        
        # Sort by chapter number
        chapters.sort(key=lambda x: x['chapter_number'])
        logger.info(f"   ✅ Found {len(chapters)} chapters")
        
    except Exception as e:
        logger.error(f"   ❌ Error scraping chapter list: {e}")
    
    return chapters


def scrape_chapter_panels(driver, chapter_url: str):
    """
    Scrape panel images from a chapter
    
    Args:
        driver: Selenium WebDriver
        chapter_url: Chapter URL
        
    Returns:
        List of panel image URLs
    """
    logger.info(f"   📖 Scraping panels from: {chapter_url}")
    
    driver.get(chapter_url)
    time.sleep(3)
    
    panels = []
    
    try:
        # Wait for images to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "img[class*='wp-manga-chapter-img'], .reading-content img, img[data-src]"))
        )
        
        # Try multiple selectors for panel images
        image_selectors = [
            "img.wp-manga-chapter-img",
            ".reading-content img",
            ".page-break img",
            "img[data-src*='manga']",
            ".entry-content img"
        ]
        
        img_elements = []
        for selector in image_selectors:
            img_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if img_elements:
                break
        
        if not img_elements:
            logger.warning(f"      ⚠️  No panel images found")
            return panels
        
        for img in img_elements:
            try:
                # Try to get image URL from src or data-src
                img_url = img.get_attribute('data-src') or img.get_attribute('src')
                
                if img_url and img_url.startswith('http'):
                    # Filter out non-manga images (ads, logos, etc.)
                    if any(skip in img_url.lower() for skip in ['logo', 'icon', 'banner', 'ad']):
                        continue
                    
                    panels.append(img_url)
                    
            except Exception as e:
                continue
        
        logger.info(f"      ✅ Found {len(panels)} panels")
        
    except Exception as e:
        logger.error(f"      ❌ Error scraping panels: {e}")
    
    return panels


def add_chapter_to_db(supabase: Client, manga_id: str, chapter_data: dict, panel_urls: list):
    """
    Add chapter and panels to database
    
    Args:
        supabase: Supabase client
        manga_id: Manga UUID
        chapter_data: Chapter information
        panel_urls: List of panel image URLs
        
    Returns:
        Chapter ID
    """
    try:
        # Check if chapter exists
        existing = supabase.table('chapters').select('id').eq('manga_id', manga_id).eq('chapter_number', chapter_data['chapter_number']).execute()
        
        if existing.data:
            chapter_id = existing.data[0]['id']
            logger.info(f"      Chapter {chapter_data['chapter_number']} already exists, updating...")
            
            # Update chapter
            supabase.table('chapters').update({
                'title': chapter_data['title'],
                'total_panels': len(panel_urls)
            }).eq('id', chapter_id).execute()
            
            # Delete existing panels
            supabase.table('panels').delete().eq('chapter_id', chapter_id).execute()
        else:
            # Create new chapter
            chapter_insert = {
                'manga_id': manga_id,
                'chapter_number': chapter_data['chapter_number'],
                'title': chapter_data['title'],
                'total_panels': len(panel_urls)
            }
            
            result = supabase.table('chapters').insert(chapter_insert).execute()
            chapter_id = result.data[0]['id']
            logger.info(f"      ✅ Created chapter {chapter_data['chapter_number']}")
        
        # Add panels
        for panel_number, panel_url in enumerate(panel_urls, start=1):
            try:
                supabase.table('panels').insert({
                    'chapter_id': chapter_id,
                    'panel_number': panel_number,
                    'image_url': panel_url
                }).execute()
            except Exception as e:
                logger.error(f"      Error adding panel {panel_number}: {e}")
                continue
        
        logger.info(f"      ✅ Added {len(panel_urls)} panels")
        return chapter_id
        
    except Exception as e:
        logger.error(f"      ❌ Error adding chapter to database: {e}")
        return None


def main():
    """Main execution function"""
    logger.info("="*80)
    logger.info("🚀 STARTING MANGA SCRAPER")
    logger.info("="*80)
    
    # Get environment variables
    manga_slug = os.getenv("MANGA_SLUG")
    title = os.getenv("MANGA_TITLE")
    cover_image_url = os.getenv("COVER_IMAGE_URL")
    description = os.getenv("DESCRIPTION", "")
    author = os.getenv("AUTHOR", "")
    status = os.getenv("STATUS", "ongoing")
    publication_year = os.getenv("PUBLICATION_YEAR", "")
    genres_str = os.getenv("GENRES", "")
    is_locked = os.getenv("IS_LOCKED", "false").lower() == "true"
    max_chapters = int(os.getenv("MAX_CHAPTERS", "0"))
    
    logger.info(f"\n📋 MANGA DETAILS:")
    logger.info(f"   Title: {title}")
    logger.info(f"   Slug: {manga_slug}")
    logger.info(f"   Author: {author or 'N/A'}")
    logger.info(f"   Status: {status}")
    logger.info(f"   Year: {publication_year or 'N/A'}")
    logger.info(f"   Genres: {genres_str or 'N/A'}")
    logger.info(f"   Locked: {is_locked}")
    logger.info(f"   Max Chapters: {max_chapters if max_chapters > 0 else 'All'}\n")
    
    # Initialize Supabase
    try:
        supabase = init_supabase()
        logger.info("✅ Connected to Supabase\n")
    except Exception as e:
        logger.error(f"❌ Error connecting to Supabase: {e}")
        sys.exit(1)
    
    # Process genres
    genre_ids = []
    if genres_str:
        logger.info("🏷️  Processing genres...")
        genre_names = [g.strip() for g in genres_str.split(',') if g.strip()]
        genre_ids = create_or_get_genres(supabase, genre_names)
        logger.info(f"   ✅ Processed {len(genre_ids)} genres\n")
    
    # Create manga entry
    manga_data = {
        'title': title,
        'slug': manga_slug,
        'description': description,
        'cover_image_url': cover_image_url,
        'author': author,
        'status': status,
        'publication_year': int(publication_year) if publication_year else None,
        'is_locked': is_locked,
        'genre_ids': genre_ids
    }
    
    manga_id = create_manga(supabase, manga_data)
    logger.info("")
    
    # Setup Selenium driver
    logger.info("🌐 Setting up web driver...")
    driver = setup_driver()
    logger.info("   ✅ Driver ready\n")
    
    try:
        # Scrape chapter list
        chapters = scrape_chapter_list(driver, manga_slug, max_chapters)
        
        if not chapters:
            logger.error("❌ No chapters found, exiting")
            sys.exit(1)
        
        logger.info(f"\n📚 Processing {len(chapters)} chapters...\n")
        
        # Process each chapter
        success_count = 0
        error_count = 0
        
        for i, chapter in enumerate(chapters, 1):
            logger.info(f"[{i}/{len(chapters)}] Chapter {chapter['chapter_number']}: {chapter['title'][:50]}...")
            
            try:
                # Scrape panels
                panel_urls = scrape_chapter_panels(driver, chapter['url'])
                
                if not panel_urls:
                    logger.warning(f"      ⚠️  No panels found, skipping")
                    error_count += 1
                    continue
                
                # Add to database
                chapter_id = add_chapter_to_db(supabase, manga_id, chapter, panel_urls)
                
                if chapter_id:
                    success_count += 1
                else:
                    error_count += 1
                
                # Be polite - wait between chapters
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"      ❌ Error processing chapter: {e}")
                error_count += 1
                continue
        
        # Final summary
        logger.info("\n" + "="*80)
        logger.info("🎉 SCRAPING COMPLETE")
        logger.info("="*80)
        logger.info(f"✅ Successful chapters: {success_count}")
        logger.info(f"❌ Failed chapters: {error_count}")
        logger.info(f"📊 Total chapters: {len(chapters)}")
        logger.info("="*80)
        
    finally:
        driver.quit()
        logger.info("\n✅ Driver closed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)
