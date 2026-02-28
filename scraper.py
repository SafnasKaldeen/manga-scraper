from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import time
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ppfbpmbomksqlgojwdhr.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwZmJwbWJvbWtzcWxnb2p3ZGhyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA4NTQ5NDMsImV4cCI6MjA3NjQzMDk0M30.5j7kSkZhoMZgvCGcxdG2phuoN3dwout3JgD1i1cUqaY")


def setup_driver_stealth():
    """
    Setup Chrome driver with selenium-stealth.
    This is the recommended method for CI/headless environments (GitHub Actions).
    """
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--disable-plugins-discovery')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')

    # Random user agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')

    # Exclude automation switches
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=options)

    # Apply stealth settings
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    return driver


def human_like_scroll(driver, scroll_attempts=5):
    """
    Scroll like a human with random delays and movements.
    """
    print(f"\n🖱️  Human-like scrolling...")

    for i in range(scroll_attempts):
        scroll_distance = random.randint(300, 800)

        driver.execute_script(f"""
            window.scrollBy({{
                top: {scroll_distance},
                left: 0,
                behavior: 'smooth'
            }});
        """)

        delay = random.uniform(1.5, 3.5)
        print(f"   Scroll {i+1}/{scroll_attempts} - waiting {delay:.1f}s...")
        time.sleep(delay)

        if random.random() < 0.3:
            driver.execute_script("window.scrollBy(0, -100);")
            time.sleep(0.5)

    # Final scroll to bottom
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)


def random_mouse_movement(driver):
    """
    Simulate random mouse movements to appear more human.
    """
    try:
        driver.execute_script("""
            var event = new MouseEvent('mousemove', {
                'view': window,
                'bubbles': true,
                'cancelable': true,
                'clientX': Math.random() * window.innerWidth,
                'clientY': Math.random() * window.innerHeight
            });
            document.dispatchEvent(event);
        """)
    except:
        pass


def parse_relative_time(time_str):
    """Convert relative time strings to datetime."""
    now = datetime.now()
    time_str = time_str.lower().strip()

    try:
        if 'hour' in time_str or 'hr' in time_str:
            hours = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(hours=hours)
        elif 'minute' in time_str or 'min' in time_str:
            minutes = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(minutes=minutes)
        elif 'day' in time_str:
            days = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(days=days)
        elif 'week' in time_str:
            weeks = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(weeks=weeks)
        elif 'month' in time_str:
            months = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(days=months * 30)
        elif 'year' in time_str:
            years = int(re.search(r'(\d+)', time_str).group(1))
            return now - timedelta(days=years * 365)
        else:
            return now
    except:
        return now


def extract_image_from_google_news_element(article):
    """Extract image directly from Google News article element."""
    try:
        img_elem = article.find_element(By.CSS_SELECTOR, 'figure img.Quavad')
        img_src = img_elem.get_attribute('src')

        if img_src and '/api/attachments/' in img_src:
            srcset = img_elem.get_attribute('srcset')
            if srcset:
                urls = [url.split()[0] for url in srcset.split(',')]
                if urls:
                    img_src = urls[-1]

            if img_src.startswith('/'):
                img_src = f"https://news.google.com{img_src}"

        return img_src
    except:
        pass

    try:
        jsdata = article.get_attribute('jsdata')
        if jsdata:
            img_urls = re.findall(r'https?://[^\s,"]+\.(?:jpg|jpeg|png|webp|gif)', jsdata)
            for url in img_urls:
                if 'gstatic.com' not in url and 'encrypted-tbn' not in url:
                    return url
    except:
        pass

    return None


def extract_article_image(url):
    """Extract image from actual article URL."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, timeout=10, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            img = og_image['content']
            if 'gstatic.com' not in img and 'google.com' not in img:
                return img

        twitter_img = soup.find('meta', attrs={'name': 'twitter:image'})
        if twitter_img and twitter_img.get('content'):
            img = twitter_img['content']
            if 'gstatic.com' not in img and 'google.com' not in img:
                return img

        article_img = soup.find('meta', property='article:image')
        if article_img and article_img.get('content'):
            img = article_img['content']
            if 'gstatic.com' not in img and 'google.com' not in img:
                return img

    except Exception as e:
        print(f"Error extracting image: {e}")

    return None


def init_supabase():
    """Initialize Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Please set SUPABASE_URL and SUPABASE_KEY in .env file")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upload_to_supabase(supabase: Client, articles, table_name="news_articles"):
    """Upload articles to Supabase with 7-day TTL."""
    expiry_date = datetime.now() + timedelta(days=7)

    uploaded_count = 0
    skipped_count = 0
    error_count = 0

    for article in articles:
        try:
            data = {
                'title': article['title'],
                'publisher': article['publisher'],
                'published_at': article['published_datetime'].isoformat() if article['published_datetime'] else None,
                'published_text': article['published_text'],
                'google_link': article['google_link'],
                'article_url': article['real_url'],
                'image_url': article['image'],
                'query': article.get('query', ''),
                'expires_at': expiry_date.isoformat(),
                'scraped_at': datetime.now().isoformat()
            }

            existing = supabase.table(table_name).select("id").eq("article_url", article['real_url']).execute()

            if existing.data:
                print(f"   ⏭️  Already exists: {article['title'][:50]}...")
                skipped_count += 1
            else:
                result = supabase.table(table_name).insert(data).execute()
                print(f"   ✅ Uploaded: {article['title'][:50]}...")
                uploaded_count += 1

        except Exception as e:
            print(f"   ❌ Error uploading {article['title'][:50]}...: {e}")
            error_count += 1

    return uploaded_count, skipped_count, error_count


def scrape_google_news(query="anime", max_articles=20, scroll_attempts=5, sort_by_time=True, supabase_client=None):
    """
    Scrape Google News using selenium-stealth (optimized for GitHub Actions).

    Args:
        query: Search query string
        max_articles: Maximum number of articles to scrape
        scroll_attempts: Number of times to scroll
        sort_by_time: Sort by publication time
        supabase_client: Supabase client instance
    """
    print("🚀 Using selenium-stealth (headless, optimized for CI)")
    driver = setup_driver_stealth()

    results = []

    # Get existing article URLs from database
    existing_urls = set()
    if supabase_client:
        try:
            response = supabase_client.table("news_articles").select("article_url").eq("query", query).execute()
            existing_urls = {row['article_url'] for row in response.data}
            print(f"📚 Found {len(existing_urls)} existing articles in database for query '{query}'\n")
        except Exception as e:
            print(f"⚠️  Could not fetch existing articles: {e}\n")

    try:
        url = f"https://news.google.com/search?q={query}&hl=en-US&gl=US&ceid=US%3Aen"
        print(f"🔎 Fetching: {url}\n")

        driver.get(url)

        print("⏳ Waiting for page to load...")
        time.sleep(random.uniform(3, 5))

        random_mouse_movement(driver)

        # Check if blocked
        page_source = driver.page_source.lower()
        if 'unusual traffic' in page_source or 'captcha' in page_source:
            print("\n⚠️  WARNING: Google has detected automation!")
            print("Trying to continue anyway...\n")

        human_like_scroll(driver, scroll_attempts)
        time.sleep(2)

        articles = driver.find_elements(By.CSS_SELECTOR, 'article')

        print(f"\n✅ Found {len(articles)} total articles on page")
        print(f"Will scrape up to {max_articles} articles\n")

        for i, article in enumerate(articles[:max_articles]):
            try:
                time.sleep(random.uniform(0.5, 1.5))

                # Title and link
                title_elem = article.find_element(By.CSS_SELECTOR, 'a.JtKRv')
                title = title_elem.text
                google_link = title_elem.get_attribute('href')

                # Publisher
                try:
                    publisher = article.find_element(By.CSS_SELECTOR, 'div[data-n-tid]').text
                except:
                    publisher = "Unknown"

                # Time
                published_datetime = None
                published_text = None
                try:
                    time_elem = article.find_element(By.CSS_SELECTOR, 'time')
                    datetime_attr = time_elem.get_attribute('datetime')
                    time_text = time_elem.text

                    if datetime_attr:
                        published_datetime = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                    else:
                        published_datetime = parse_relative_time(time_text)

                    published_text = time_text
                except:
                    pass

                print(f"[{i+1}] {title[:60]}...")
                print(f"    Publisher: {publisher}")
                print(f"    Published: {published_text or 'Unknown'}")
                if published_datetime:
                    print(f"    DateTime: {published_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

                # Image from Google News element
                image = extract_image_from_google_news_element(article)
                if image:
                    print(f"    Image (from Google News): ✅ Found")
                    print(f"           {image[:70]}...")

                # Resolve real URL
                real_url = google_link
                if google_link and google_link.startswith('http'):
                    try:
                        response = requests.get(google_link, allow_redirects=True, timeout=5)
                        real_url = response.url
                        print(f"    Real URL: {real_url[:60]}...")

                        if real_url in existing_urls:
                            print(f"    ⏭️  SKIPPING - Already in database")
                            print()
                            continue

                    except:
                        pass

                # Fallback image from article page
                if not image:
                    image = extract_article_image(real_url)
                    if image:
                        print(f"    Image (from article): ✅ Found")
                        print(f"           {image[:70]}...")
                    else:
                        print(f"    Image: ❌ Not found")

                results.append({
                    'title': title,
                    'publisher': publisher,
                    'published_datetime': published_datetime,
                    'published_text': published_text,
                    'google_link': google_link,
                    'real_url': real_url,
                    'image': image,
                    'query': query
                })

                print()

            except Exception as e:
                print(f"Error parsing article {i+1}: {e}\n")
                continue

        # Sort by time
        if sort_by_time and results:
            results.sort(
                key=lambda x: x['published_datetime'] if x['published_datetime'] else datetime.min,
                reverse=True
            )
            print(f"\n✅ Sorted {len(results)} articles by publication time (newest first)")

    except Exception as e:
        print(f"\n❌ Critical error: {e}")
    finally:
        driver.quit()

    return results


def scrape_multiple_queries(queries, max_articles_per_query=50, scroll_attempts=20, sort_by_time=True, supabase_client=None, table_name="news_articles"):
    """Scrape multiple queries sequentially."""
    all_stats = {}

    for idx, query in enumerate(queries):
        print("\n" + "="*80)
        print(f"🔍 STARTING SCRAPE {idx+1}/{len(queries)}: {query.upper()}")
        print("="*80 + "\n")

        articles = scrape_google_news(
            query=query,
            max_articles=max_articles_per_query,
            scroll_attempts=scroll_attempts,
            sort_by_time=sort_by_time,
            supabase_client=supabase_client
        )

        print("\n" + "="*80)
        print(f"📊 SCRAPED {len(articles)} ARTICLES FOR '{query}'")
        print("="*80 + "\n")

        if supabase_client and articles:
            print(f"📤 UPLOADING '{query}' ARTICLES TO SUPABASE\n")
            uploaded, skipped, errors = upload_to_supabase(supabase_client, articles, table_name)

            all_stats[query] = {
                'scraped': len(articles),
                'uploaded': uploaded,
                'skipped': skipped,
                'errors': errors
            }

            print("\n" + "-"*80)
            print(f"✨ '{query}' UPLOAD SUMMARY")
            print("-"*80)
            print(f"✅ Uploaded: {uploaded}")
            print(f"⏭️  Skipped: {skipped}")
            print(f"❌ Errors: {errors}")
            print(f"📦 Total: {len(articles)}")
            print("-"*80)
        else:
            all_stats[query] = {
                'scraped': len(articles),
                'uploaded': 0,
                'skipped': 0,
                'errors': 0
            }

        if idx < len(queries) - 1:
            wait_time = random.randint(5, 10)
            print(f"\n⏳ Waiting {wait_time} seconds before next query...\n")
            time.sleep(wait_time)

    return all_stats


if __name__ == "__main__":
    # ── Configuration ──────────────────────────────────────────────────────────
    QUERIES = ["anime", "manga"]
    MAX_ARTICLES_PER_QUERY = 50
    SCROLL_ATTEMPTS = 20
    SORT_BY_TIME = True
    TABLE_NAME = "news_articles"
    # ──────────────────────────────────────────────────────────────────────────

    print("🚀 Starting Google News Scraper (selenium-stealth / GitHub Actions)")
    print(f"Queries: {', '.join(QUERIES)}")
    print(f"Target per query: {MAX_ARTICLES_PER_QUERY} articles")
    print(f"Scroll attempts: {SCROLL_ATTEMPTS}")
    print(f"Sort by time: {'✅ Yes (newest first)' if SORT_BY_TIME else '❌ No'}\n")

    # Initialize Supabase
    try:
        supabase = init_supabase()
        print("✅ Connected to Supabase\n")
    except Exception as e:
        print(f"❌ Error connecting to Supabase: {e}")
        print("\nRequired packages:")
        print("pip install selenium selenium-stealth supabase python-dotenv beautifulsoup4 requests")
        exit(1)

    # Run scraper
    stats = scrape_multiple_queries(
        queries=QUERIES,
        max_articles_per_query=MAX_ARTICLES_PER_QUERY,
        scroll_attempts=SCROLL_ATTEMPTS,
        sort_by_time=SORT_BY_TIME,
        supabase_client=supabase,
        table_name=TABLE_NAME
    )

    # Final summary
    print("\n\n" + "="*80)
    print("🎉 FINAL SUMMARY - ALL QUERIES")
    print("="*80)

    total_scraped = 0
    total_uploaded = 0
    total_skipped = 0
    total_errors = 0

    for query, data in stats.items():
        print(f"\n📰 {query.upper()}:")
        print(f"   Scraped:  {data['scraped']}")
        print(f"   Uploaded: {data['uploaded']}")
        print(f"   Skipped:  {data['skipped']}")
        print(f"   Errors:   {data['errors']}")

        total_scraped  += data['scraped']
        total_uploaded += data['uploaded']
        total_skipped  += data['skipped']
        total_errors   += data['errors']

    print("\n" + "-"*80)
    print("📊 TOTALS:")
    print(f"   Total Scraped:   {total_scraped}")
    print(f"   Total Uploaded:  {total_uploaded}")
    print(f"   Total Skipped:   {total_skipped}")
    print(f"   Total Errors:    {total_errors}")
    print(f"   ⏰ TTL: 7 days (expires {(datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')})")
    print("="*80)
