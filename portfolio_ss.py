#!/usr/bin/env python3
# -*- coding: utf-8 -*-




import os
import time
import base64
import json
import re
from datetime import datetime
from urllib.parse import urlparse, urljoin
from dotenv import load_dotenv
from openai import OpenAI
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
# ── Console-encoding hardening (Windows CP-1252 crashes on emoji) ──
import sys
if (
    sys.platform.startswith("win")
    and sys.stdout.encoding
    and sys.stdout.encoding.lower() != "utf-8"
):
    # Re-wrap both streams so any Unicode prints safely
    sys.stdout = open(
        sys.stdout.fileno(),
        mode="w",
        encoding="utf-8",
        errors="replace",
        buffering=1,
    )
    sys.stderr = open(
        sys.stderr.fileno(),
        mode="w",
        encoding="utf-8",
        errors="replace",
        buffering=1,
    )




load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))




OUTPUT_DIR = "output"
HEADERS = {
   "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}




# More specific portfolio patterns
PORTFOLIO_PATTERNS = [
   r'/portfolio/?$',
   r'/companies/?$',
   r'/investments/?$',
   r'/our-portfolio',
   r'/portfolio-companies',
   r'/startups/?$'
]




# Stricter biotech validation
BIOTECH_SUFFIXES = [
   'therapeutics', 'pharma', 'pharmaceutical', 'pharmaceuticals', 'bio', 'biosciences',
   'medical', 'health', 'healthcare', 'sciences', 'labs', 'laboratories', 'biotech',
   'biotechnology', 'medicine', 'diagnostics', 'genomics', 'oncology', 'neuroscience'
]




os.makedirs(OUTPUT_DIR, exist_ok=True)




def deduplicate_companies_with_llm(companies, use_aggressive=False):
    """
    Use LLM to deduplicate similar company names
    """
    if not companies or len(companies) < 2:
        return companies
    
    # Create prompt for deduplication
    companies_list = "\n".join([f"{i+1}. {company}" for i, company in enumerate(companies)])
    
    if use_aggressive:
        prompt = f"""You are an expert at deduplicating company names from VC portfolios. Your job is to AGGRESSIVELY consolidate companies that are the same entity with different naming variations.

🚨 ULTRA-AGGRESSIVE DEDUPLICATION RULES:
1. **CONSOLIDATE IMMEDIATELY if ANY of these patterns match:**
   - Same core company name with/without legal suffixes (Inc, Corp, Ltd, LLC)
   - Same company + different business descriptors (Bio, Labs, Therapeutics, Medical, Sciences, Discovery, Pharma, Technologies)
   - Obvious typos or spacing variations of the same name
   - Shortened vs full company names (Apple vs Apple Inc)
   - Different capitalization of same company (iPHONE vs iPhone vs Iphone)

2. **KEEP THE LONGEST/MOST COMPLETE/MOST FORMAL VERSION**
   - "Annovis" + "Annovis Bio" → Keep "Annovis Bio"
   - "Company" + "Company Inc." → Keep "Company Inc."
   - "Labs" + "Labs Corporation" → Keep "Labs Corporation"

3. **CONSOLIDATION EXAMPLES YOU MUST FOLLOW:**
   - Angiochem + Angiochem Inc. → "Angiochem Inc."
   - Cytochroma + Cytochroma Inc. → "Cytochroma Inc."
   - Intelligent Hospital Systems + Intelligent Hospital Systems Ltd. → "Intelligent Hospital Systems Ltd."
   - Interface Biologics + Interface Biologics Inc. → "Interface Biologics Inc."
   - MedCurrent + MedCurrent Corporation → "MedCurrent Corporation"
   - Monteris + Monteris Medical Inc. → "Monteris Medical Inc."
   - Novadaq + Novadaq Technologies → "Novadaq Technologies"
   - Clementia + Clementia Pharmaceuticals → "Clementia Pharmaceuticals"
   - Any company name appearing twice with slight variations

4. **BE RUTHLESS:**
   - If 70%+ of the company name matches, consolidate them
   - Don't worry about being too aggressive - it's better to over-consolidate than under-consolidate
   - Focus on keeping the most professional/complete version

5. **WHAT DEFINITELY COUNTS AS DUPLICATES:**
   - Exact matches with different legal entities (Inc vs Corp vs Ltd)
   - Company + Company + descriptive suffix
   - Typos and OCR errors (common in web scraping)
   - Same company with/without punctuation, spaces, capitalization

INPUT COMPANIES:
{companies_list}

🎯 **YOUR TASK:** Return ONLY a JSON array of deduplicated names. Be EXTREMELY aggressive - if there's any doubt, consolidate and keep the more complete version.

OUTPUT FORMAT: ["Company Name 1", "Company Name 2", ...]"""
    else:
        prompt = f"""You are an expert at identifying and consolidating duplicate company names from venture capital portfolios.

TASK: Remove clear duplicates from this list while being conservative. Only consolidate when you're confident they're the same company.

RULES:
1. Consolidate obvious duplicates (same name with/without Inc, Corp, Ltd, LLC)
2. Keep the most complete/formal version when consolidating
3. Be conservative - when in doubt, keep both names separate
4. Don't consolidate companies that might be different divisions or subsidiaries

COMPANIES TO DEDUPLICATE:
{companies_list}

Return only a JSON array of the final deduplicated company names:
["Company Name 1", "Company Name 2", ...]"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2000
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Extract JSON array from response
        # Look for JSON array pattern
        json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if json_match:
            try:
                deduplicated = json.loads(json_match.group(0))
                if isinstance(deduplicated, list):
                    print(f"   [DEDUP] {len(companies)} → {len(deduplicated)} companies")
                    
                    # Show what was consolidated (for transparency)
                    if len(deduplicated) < len(companies):
                        removed_count = len(companies) - len(deduplicated)
                        print(f"   [CONSOLIDATED] Removed {removed_count} duplicates")
                        
                        # Show which companies were removed (first few)
                        kept_names = set(deduplicated)
                        removed_names = [c for c in companies if c not in kept_names]
                        if removed_names:
                            print(f"   [REMOVED] {removed_names[:5]}{'...' if len(removed_names) > 5 else ''}")
                    
                    return deduplicated
                else:
                    print(f"   [DEDUP ERROR] Response not a list: {response_text[:100]}")
                    return companies
            except json.JSONDecodeError as e:
                print(f"   [DEDUP ERROR] JSON parse error: {e}")
                print(f"   [RESPONSE] {response_text[:200]}...")
                return companies
        else:
            print(f"   [DEDUP ERROR] No JSON array found in response")
            print(f"   [RESPONSE] {response_text[:200]}...")
            return companies
            
    except Exception as e:
        print(f"   [DEDUP ERROR] LLM call failed: {e}")
        return companies




def process_single_portfolio_url(url):
    """
    Process a single portfolio URL and return companies found
    Args:
        url (str): Portfolio URL to process
    Returns:
        list: Companies found from this URL
    """
    try:
        print(f"   [PROCESSING] {url}")
        shots = take_smart_screenshots(url)
        companies = analyze_screenshots_precise(shots, url)
        print(f"   [FOUND] {len(companies)} companies from {url}")
        return companies
    except Exception as e:
        print(f"   [ERROR] Failed to process {url}: {e}")
        return []


def process_portfolio_urls_parallel(urls_to_scrape, max_workers=6):
    """
    Process multiple portfolio URLs in parallel using ThreadPoolExecutor
    Args:
        urls_to_scrape (list): List of URLs to process
        max_workers (int): Maximum number of parallel workers
    Returns:
        list: All companies found from all URLs
    """
    all_companies = []
    
    print(f"🔧 Processing {len(urls_to_scrape)} portfolio URLs with {max_workers} parallel workers...")
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {executor.submit(process_single_portfolio_url, url): url for url in urls_to_scrape}
        
        # Process completed tasks as they finish
        for i, future in enumerate(as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                companies = future.result()
                all_companies.extend(companies)
                print(f"   [PROGRESS] {i}/{len(urls_to_scrape)} URLs processed ({len(companies)} companies from {url})")
            except Exception as e:
                print(f"   [ERROR] Exception processing {url}: {e}")
    
    print(f"🎯 Parallel processing complete! Found {len(all_companies)} total companies")
    return all_companies


def get_domain(url):
   """Extract domain from URL"""
   parsed = urlparse(url)
   return parsed.netloc.replace("www.", "")




def get_vc_name_from_url(url):
   """Extract VC name from URL for attribution"""
   domain = get_domain(url)
   vc_name = domain.replace('.com', '').replace('.', ' ').title()
   return vc_name




def find_portfolio_urls_enhanced(base_url):
   """Enhanced portfolio URL discovery with multiple strategies"""
   domain = get_domain(base_url)
   portfolio_urls = set()
 
   try:
       print(f"  [SEARCHING] Multi-strategy portfolio discovery on {domain}...")

       # Strategy 1: Scrape homepage for portfolio links
       response = requests.get(base_url, headers=HEADERS, timeout=15)
       response.raise_for_status()
       soup = BeautifulSoup(response.text, 'html.parser')

       # Look for portfolio-related links
       for a in soup.find_all('a', href=True):
           href = a['href'].strip()
           if not href:
               continue

           # Convert to absolute URL
           if href.startswith('/'):
               full_url = urljoin(base_url, href)
           elif href.startswith('http'):
               full_url = href
           else:
               full_url = urljoin(base_url, href)

           # Check if it's the same domain and portfolio-related
           if get_domain(full_url) == domain:
               path = urlparse(full_url).path.lower()
               text = a.get_text().lower().strip()

               # Check path and link text for portfolio indicators
               portfolio_indicators = ['portfolio', 'companies', 'investments', 'startups', 'ventures']
               if (any(indicator in path for indicator in portfolio_indicators) or
                   any(indicator in text for indicator in portfolio_indicators)):
                   portfolio_urls.add(full_url)

       # Strategy 2: Try common portfolio URL patterns
       common_patterns = [
           '/portfolio', '/portfolio/', '/companies', '/companies/',
           '/investments', '/investments/', '/our-portfolio', '/our-companies',
           '/portfolio-companies', '/startups', '/ventures'
       ]

       for pattern in common_patterns:
           test_url = urljoin(base_url, pattern)
           try:
               test_response = requests.head(test_url, headers=HEADERS, timeout=10)
               if test_response.status_code == 200:
                   portfolio_urls.add(test_url)
                   print(f"    [FOUND] Direct pattern: {pattern}")
           except:
               continue

       # Fallback: Strategy 3 - Look for sitemap ONLY if no URLs found so far
       if not portfolio_urls:
           sitemap_urls = ['/sitemap.xml', '/sitemap_index.xml', '/robots.txt']
           for sitemap_path in sitemap_urls:
               try:
                   sitemap_url = urljoin(base_url, sitemap_path)
                   sitemap_response = requests.get(sitemap_url, headers=HEADERS, timeout=10)
                   if sitemap_response.status_code == 200:
                       # Parse sitemap or robots.txt for portfolio URLs
                       content = sitemap_response.text.lower()
                       for line in content.split('\n'):
                           if any(indicator in line for indicator in ['portfolio', 'companies', 'investments']):
                               # Extract URL from sitemap line
                               url_match = re.search(r"https?://[^\s<>\"']+", line)
                               if url_match and get_domain(url_match.group()) == domain:
                                   portfolio_urls.add(url_match.group())
                       print(f"    [SITEMAP] Found additional URLs in {sitemap_path}")
                       break
               except:
                   continue

       # PRIORITY: Ensure base portfolio URL is always included (without anchors)
       portfolio_list = list(portfolio_urls)
       base_portfolio_url = None

       # Look for the cleanest base portfolio URL
       for url in portfolio_list:
           parsed = urlparse(url)
           path = parsed.path.lower()
           # Check if this is a clean base portfolio URL (no anchors, minimal path)
           if (path in ['/portfolio', '/portfolio/', '/companies', '/companies/', '/investments', '/investments/'] and 
               not parsed.fragment and not parsed.query):
               base_portfolio_url = url
               break

       # If no clean base portfolio URL found, try to construct one
       if not base_portfolio_url:
           for pattern in ['/portfolio', '/companies', '/investments']:
               test_url = urljoin(base_url, pattern)
               try:
                   test_response = requests.head(test_url, headers=HEADERS, timeout=10)
                   if test_response.status_code == 200:
                       base_portfolio_url = test_url
                       portfolio_list.insert(0, base_portfolio_url)  # Add to front
                       print(f"    [BASE] Added base portfolio URL: {pattern}")
                       break
               except:
                   continue

       # Ensure base portfolio URL is first in the list
       if base_portfolio_url and base_portfolio_url in portfolio_list:
           portfolio_list.remove(base_portfolio_url)
           portfolio_list.insert(0, base_portfolio_url)
           print(f"    [PRIORITY] Base portfolio URL set as first: {urlparse(base_portfolio_url).path}")

       if portfolio_list:
           print(f"  [FOUND] {len(portfolio_list)} portfolio URLs:")
           for i, url in enumerate(portfolio_list[:5], 1):
               path = urlparse(url).path or "/"
               if i == 1:
                   print(f"    {i}. {path} [BASE]")
               else:
                   print(f"    {i}. {path}")
           if len(portfolio_list) > 5:
               print(f"    ... and {len(portfolio_list)-5} more")
       else:
           print(f"  [FALLBACK] No portfolio URLs found, using homepage")
           portfolio_list = [base_url]

       return portfolio_list

   except Exception as e:
       print(f"  [ERROR] Portfolio discovery failed: {e}")
       return [base_url]




def setup_selenium_driver():
   """Setup Selenium WebDriver with multiple fallback options"""
   driver = None
 
   import random
   try:
       from selenium import webdriver
       from selenium.webdriver.chrome.options import Options
       from selenium.webdriver.chrome.service import Service
       from selenium.webdriver.common.by import By
       from selenium.webdriver.support.ui import WebDriverWait
       from selenium.webdriver.support import expected_conditions as EC

       print(f"    [SELENIUM] Setting up Chrome WebDriver...")

       # Pick a random port in a safe range to avoid conflicts (9224–9324)
       port = random.randint(9224, 9324)
       chrome_options = Options()
       chrome_options.add_argument('--headless')
       chrome_options.add_argument('--no-sandbox')
       chrome_options.add_argument('--disable-dev-shm-usage')
       chrome_options.add_argument('--window-size=1920,1080')
       chrome_options.add_argument('--force-device-scale-factor=2')
       chrome_options.add_argument('--disable-blink-features=AutomationControlled')
       chrome_options.add_argument('--disable-extensions')
       chrome_options.add_argument('--disable-plugins')
       chrome_options.add_argument('--disable-web-security')
       chrome_options.add_argument('--allow-running-insecure-content')
       chrome_options.add_argument('--disable-gpu')
       chrome_options.add_argument(f'--remote-debugging-port={port}')
       chrome_options.add_argument("--logc-level=3")

       # Try to use system Chrome first
       try:
           # Try with system Chrome
           driver = webdriver.Chrome(options=chrome_options)
           print(f"    [SUCCESS] Chrome WebDriver initialized on port {port}")
           return driver, True
       except Exception as e:
           print(f"    [FALLBACK] System Chrome failed: {e}")

       # Try with chromedriver-autoinstaller
       try:
           import chromedriver_autoinstaller
           chromedriver_autoinstaller.install()
           # Pick a new port in case previous one is still in use
           port2 = random.randint(9224, 9324)
           chrome_options.add_argument(f'--remote-debugging-port={port2}')
           driver = webdriver.Chrome(options=chrome_options)
           print(f"    [SUCCESS] Chrome WebDriver with auto-installer on port {port2}")
           return driver, True
       except Exception as e:
           print(f"    [FALLBACK] Auto-installer failed: {e}")

       # Try Firefox as fallback
       try:
           from selenium.webdriver.firefox.options import Options as FirefoxOptions
           firefox_options = FirefoxOptions()
           firefox_options.add_argument('--headless')
           firefox_options.add_argument('--width=1920')
           firefox_options.add_argument('--height=1080')

           # Firefox does not use the same debugging port, so no need to set
           driver = webdriver.Firefox(options=firefox_options)
           print(f"    [SUCCESS] Firefox WebDriver as fallback")
           return driver, True
       except Exception as e:
           print(f"    [FALLBACK] Firefox also failed: {e}")

   except ImportError:
       print(f"    [ERROR] Selenium not available")
   except Exception as e:
       print(f"    [ERROR] WebDriver setup failed: {e}")

   return None, False




def take_smart_screenshots(url):
   """Improved screenshot strategy with Selenium fallback to requests"""
   driver = None
   selenium_available = False
 
   try:
       # Try Selenium first
       driver, selenium_available = setup_selenium_driver()
     
       if selenium_available and driver:
           return take_selenium_screenshots(driver, url)
       else:
           print(f"    [FALLBACK] Using requests + BeautifulSoup approach")
           return take_requests_screenshots(url)
         
   except Exception as e:
       print(f"    [ERROR] Screenshot capture failed: {e}")
       return []
   finally:
       if driver:
           try:
               driver.quit()
           except:
               pass




def take_selenium_screenshots(driver, url):
   """Take screenshots using Selenium"""
   try:
       from selenium.webdriver.common.by import By
       from selenium.webdriver.support.ui import WebDriverWait
       from selenium.webdriver.support import expected_conditions as EC
     
       print(f"    [SELENIUM] Smart capture for {urlparse(url).path or '/'}")
     
       driver.get(url)
       time.sleep(5)
     
       wait = WebDriverWait(driver, 15)
       wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
     
       # Dismiss modals more thoroughly
       dismiss_modals(driver)
     
       # Try to expand portfolio sections
       expand_portfolio_sections(driver)
     
       # Get page dimensions
       page_height = driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)")
       viewport_height = driver.execute_script("return window.innerHeight")
     
       print(f"      [DIMENSIONS] Page: {page_height}px, Viewport: {viewport_height}px")
     
       screenshots = []
     
       # Strategy: Take fewer, more strategic screenshots
       if page_height <= viewport_height * 1.5:
           # Short page - just take 2 screenshots
           screenshot = driver.get_screenshot_as_png()
           screenshots.append(screenshot)
         
           driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
           time.sleep(2)
           screenshot = driver.get_screenshot_as_png()
           screenshots.append(screenshot)
           print(f"      [SHORT PAGE] 2 strategic screenshots")
       else:
           # Longer page - use larger increments to reduce overlap
           scroll_increment = viewport_height * 1.2
           current_position = 0
         
           while current_position < page_height and len(screenshots) < 12:  # Limit screenshots
               screenshot = driver.get_screenshot_as_png()
               screenshots.append(screenshot)
             
               current_position += scroll_increment
               driver.execute_script(f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}});")
               time.sleep(3)
             
               actual_position = driver.execute_script("return window.pageYOffset")
               print(f"      [SCROLL] Position: {actual_position}px")
             
               if actual_position + viewport_height >= page_height - 100:
                   break
         
           # Final screenshot at bottom
           driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
           time.sleep(2)
           screenshot = driver.get_screenshot_as_png()
           screenshots.append(screenshot)
     
       print(f"    [SELENIUM SUCCESS] Captured {len(screenshots)} screenshots")
       return screenshots
     
   except Exception as e:
       print(f"    [SELENIUM ERROR] {e}")
       return []




def take_requests_screenshots(url):
   """Fallback: Extract content using requests and BeautifulSoup"""
   try:
       print(f"    [REQUESTS] Content extraction for {urlparse(url).path or '/'}")
     
       response = requests.get(url, headers=HEADERS, timeout=30)
       response.raise_for_status()
     
       soup = BeautifulSoup(response.text, 'html.parser')
     
       # Extract company names directly from HTML
       companies = extract_companies_from_html(soup, url)
     
       # Since we can't take screenshots, we'll return the companies directly
       # We'll create a fake screenshot result to maintain compatibility
       if companies:
           print(f"    [REQUESTS SUCCESS] Extracted {len(companies)} companies from HTML")
           # Store companies in a special format for the analysis function
           return [{"type": "html_extract", "companies": companies}]
       else:
           print(f"    [REQUESTS] No companies found in HTML")
           return []
         
   except Exception as e:
       print(f"    [REQUESTS ERROR] {e}")
       return []




def extract_companies_from_html(soup, url):
   """Extract company names directly from HTML using BeautifulSoup"""
   companies = set()
 
   try:
       # Remove script and style elements
       for script in soup(["script", "style"]):
           script.decompose()
     
       # Look for portfolio sections
       portfolio_sections = soup.find_all(['div', 'section', 'ul', 'ol'],
                                        class_=re.compile(r'portfolio|companies|investments', re.I))
     
       for section in portfolio_sections:
           # Look for company names in various elements
           for element in section.find_all(['a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'span', 'div']):
               text = element.get_text().strip()
               if text and is_potential_company_name(text):
                   companies.add(text)
     
       # Also check meta tags and structured data
       meta_companies = extract_from_meta_tags(soup)
       companies.update(meta_companies)
     
       # Check for JSON-LD structured data
       json_ld_companies = extract_from_json_ld(soup)
       companies.update(json_ld_companies)
     
       # Filter and validate companies
       validated_companies = []
       for company in companies:
           if validate_company_name(company):
               validated_companies.append(company)
     
       return validated_companies
     
   except Exception as e:
       print(f"    [HTML EXTRACT ERROR] {e}")
       return []




def is_potential_company_name(text):
   """Check if text could be a company name"""
   if not text or len(text) < 2 or len(text) > 50:
       return False
 
   # Skip obvious navigation/UI text
   skip_phrases = ['learn more', 'read more', 'visit website', 'portfolio', 'about us',
                  'contact', 'home', 'news', 'blog', 'careers', 'login', 'sign up']
 
   if any(phrase in text.lower() for phrase in skip_phrases):
       return False
 
   # Must contain letters
   if not re.search(r'[A-Za-z]', text):
       return False
 
   # Check for biotech indicators
   for suffix in BIOTECH_SUFFIXES:
       if suffix in text.lower():
           return True
 
   # Check for company-like patterns
   company_patterns = [
       r'\b\w+\s+(Inc|LLC|Corp|Ltd|Co)\b',
       r'\b\w+\s+(Therapeutics|Pharma|Bio|Medical|Health)\b',
       r'\b\w+[Tt]ech\b',
       r'\b\w+[Gg]en\b',
       r'\b\w+[Cc]ell\b'
   ]
 
   for pattern in company_patterns:
       if re.search(pattern, text, re.IGNORECASE):
           return True
 
   # Simple heuristic: proper capitalization and reasonable length
   if text[0].isupper() and 3 <= len(text) <= 30:
       return True
 
   return False




def validate_company_name(company):
   """Validate extracted company name"""
   if not company or len(company) < 2 or len(company) > 50:
       return False
 
   # Skip obvious non-companies
   skip_list = ['portfolio', 'companies', 'investments', 'about', 'contact', 'home',
               'news', 'blog', 'team', 'careers', 'login', 'search', 'menu']
 
   if company.lower() in skip_list:
       return False
 
   # Must contain at least one letter
   if not re.search(r'[A-Za-z]', company):
       return False
 
   return True




def extract_from_meta_tags(soup):
   """Extract company names from meta tags"""
   companies = set()
 
   # Check meta descriptions and keywords
   meta_tags = soup.find_all('meta', {'name': ['description', 'keywords']})
   for meta in meta_tags:
       content = meta.get('content', '')
       # Simple extraction - look for capitalized words
       words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', content)
       for word in words:
           if is_potential_company_name(word):
               companies.add(word)
 
   return companies




def extract_from_json_ld(soup):
   """Extract company names from JSON-LD structured data"""
   companies = set()
 
   json_scripts = soup.find_all('script', {'type': 'application/ld+json'})
   for script in json_scripts:
       try:
           data = json.loads(script.string)
           # Look for organization names
           if isinstance(data, dict):
               if data.get('@type') == 'Organization':
                   name = data.get('name', '')
                   if name and is_potential_company_name(name):
                       companies.add(name)
       except:
           continue
 
   return companies




def dismiss_modals(driver):
   """More thorough modal dismissal"""
   try:
       from selenium.webdriver.common.by import By
     
       dismiss_selectors = [
           "button[class*='dismiss']", "button[class*='accept']", "button[class*='close']",
           "[aria-label*='close']", "[aria-label*='dismiss']", ".cookie button",
           ".modal button", ".overlay button", "[role='dialog'] button",
           ".popup button", ".banner button"
       ]
     
       for selector in dismiss_selectors:
           try:
               elements = driver.find_elements(By.CSS_SELECTOR, selector)
               for element in elements:
                   if element.is_displayed():
                       driver.execute_script("arguments[0].click();", element)
                       print(f"      [MODAL] Dismissed element")
                       time.sleep(1)
           except:
               continue
   except Exception as e:
       print(f"      [MODAL ERROR] {e}")




def expand_portfolio_sections(driver):
   """Try to expand portfolio sections"""
   try:
       from selenium.webdriver.common.by import By
     
       expand_texts = ['show all', 'view all', 'load more', 'see all', 'show more']
     
       for text in expand_texts:
           try:
               # Look for buttons and links with this text
               xpath = f"//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')] | //a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]"
               element = driver.find_element(By.XPATH, xpath)
               if element.is_displayed():
                   driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                   time.sleep(1)
                   driver.execute_script("arguments[0].click();", element)
                   print(f"      [EXPAND] Clicked '{text}' button")
                   time.sleep(3)
                   break
           except:
               continue
   except Exception as e:
       print(f"      [EXPAND ERROR] {e}")


# ──────────────────────────────────────────────────────────────────────────
#  Helper – balanced de-dup & validation of extracted company names
# ──────────────────────────────────────────────────────────────────────────
def validate_companies_strictly(companies: list[str]) -> list[str]:
    """Balanced validation – not too strict, not too loose."""
    print(f"    [VALIDATION] Balanced validation of {len(companies)} candidates…")


    # ── de-duplicate ──────────────────────────────────────────────────────
    seen, deduped = set(), []
    for name in companies:
        key = re.sub(r"[^a-z0-9]", "", name.lower())
        if key and key not in seen:
            seen.add(key)
            deduped.append(name)


    validated: list[str] = []
    for company in deduped:
        c_lower = company.lower()


        # positive signals
        score = 0
        for suffix in BIOTECH_SUFFIXES:
            if c_lower.endswith(suffix):
                score += 3
                break
            if suffix in c_lower:
                score += 2


        for kw in (
            "gene", "cell", "nano", "neuro", "immuno", "onco",
            "bio", "med", "health", "tech", "systems", "solutions",
        ):
            if kw in c_lower:
                score += 1


        # basic format sanity
        if (
            2 <= len(company) <= 50
            and re.search(r"[A-Za-z]", company)
            and len(company.split()) <= 5
        ):
            score += 1
        if company[0].isupper():
            score += 1


        # obvious negatives
        if any(bad in c_lower for bad in (
            "learn more", "visit", "click", "here", "view", "portfolio",
            "navigation", "menu", "button", "website", "page",
        )):
            continue


        if score >= 1:
            validated.append(company)


    print(f"    [VALIDATED] {len(validated)} names passed strict check")
    return validated


# ──────────────────────────────────────────────────────────────────────────
def analyze_screenshots_precise(screenshots, url):
    """Analyse screenshots (or HTML fallback) and return validated names."""
    all_companies: list[str] = []


    # fast-path for HTML extract payload
    if (
        screenshots
        and len(screenshots) == 1
        and isinstance(screenshots[0], dict)
        and screenshots[0].get("type") == "html_extract"
    ):
        return validate_companies_strictly(screenshots[0].get("companies", []))


    # full Vision loop
    for i, shot in enumerate(screenshots, 1):
        try:
            print(f"      [ANALYZING] Screenshot {i}/{len(screenshots)}…")


            # skip placeholder dicts
            if isinstance(shot, dict):
                continue


            b64_img = base64.b64encode(shot).decode("utf-8")


            balanced_prompt = """
You are an expert at extracting company names from venture capital portfolio web pages.
Given the following screenshot, extract only the names of portfolio companies.

Rules:
- Ignore navigation, menu items, buttons, and any text that is not a company name.
- Do not include "Learn More", "Read More", "Contact", or similar phrases.
- Only include real company names, as they appear in the portfolio section.
- Deduplicate similar names and return only the cleanest, most complete version of each company name.
- If a company name appears with and without a suffix (Inc, Ltd, etc.), keep the most complete version.
- If you see a grid, list, or section of company logos or names, extract all the company names shown.

Output:
- Output only a valid JSON array of company names, with no code block, no extra quotes, and no trailing commas. Do not include any commentary or formatting.
  Example: ["Acme Therapeutics", "BioGenix", "NanoHealth Inc."]

Positive examples:
- "Acme Therapeutics"
- "BioGenix"
- "NanoHealth Inc."

Negative examples (do not include):
- "Learn More"
- "Contact"
- "Our Portfolio"
- "Read More"
- "Home"
"""


            response = client.chat.completions.create(
                model="gpt-4.1",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Extract company names, minimise false positives."
                        ),
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": balanced_prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{b64_img}",
                                    "detail": "high",
                                },
                            },
                        ],
                    },
                ],
                max_tokens=750,
                temperature=0.2,
            )


            raw = response.choices[0].message.content.strip()
            if raw == "NO_COMPANIES_FOUND":
                continue

            # Try to extract a JSON array from the output
            json_match = re.search(r'\[.*\]', raw, re.DOTALL)
            names = []
            if json_match:
                try:
                    parsed = json.loads(json_match.group(0))
                    if isinstance(parsed, list):
                        names = [str(n).strip() for n in parsed if isinstance(n, str) and 2 <= len(n.strip()) <= 50]
                except Exception:
                    pass
            if not names:
                # Fallback: clean up lines manually
                for ln in raw.splitlines():
                    ln = ln.strip().strip('`').strip(',').strip('"')
                    # Remove code block markers and empty lines
                    if not ln or ln.lower().startswith('json') or ln.startswith('[') or ln.startswith(']'):
                        continue
                    # Remove trailing commas and extra quotes
                    ln = re.sub(r'^[\-\*\d\.\s\[\]]+', '', ln)
                    ln = ln.strip('"').strip(',').strip()
                    if 2 <= len(ln) <= 50:
                        names.append(ln)

            all_companies.extend(names)


        except Exception as e:
            print(f"      [VISION ERROR] {e}")
            continue  # keep processing remaining shots


    return validate_companies_strictly(all_companies)


# ──────────────────────────────────────────────────────────────────
# entry-point (runs when executed directly)
# ──────────────────────────────────────────────────────────────────
def process_portfolio_file_for_deduplication(file_path):
    """
    Process an existing portfolio JSON file and deduplicate companies
    """
    try:
        print(f"📁 Processing: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        original_companies = data.get('companies', [])
        if not original_companies:
            print("   ⚠️ No companies found in file")
            return
        
        print(f"   📊 Original count: {len(original_companies)} companies")
        
        # Deduplicate using LLM
        deduplicated_companies = deduplicate_companies_with_llm(original_companies)
        
        # Update the data
        data['companies'] = deduplicated_companies
        
        # Add deduplication metadata
        if 'deduplication_applied' not in data:
            data['deduplication_applied'] = True
            data['original_company_count'] = len(original_companies)
            data['deduplicated_company_count'] = len(deduplicated_companies)
            data['companies_removed'] = len(original_companies) - len(deduplicated_companies)
        
        # Write back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Updated file with {len(deduplicated_companies)} deduplicated companies")
        
        if len(deduplicated_companies) < len(original_companies):
            print(f"   🔄 Removed {len(original_companies) - len(deduplicated_companies)} duplicates")
        
    except Exception as e:
        print(f"   ❌ Error processing {file_path}: {e}")


if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Check for deduplication mode
    if len(sys.argv) > 1 and sys.argv[1] == "--dedupe":
        print("🔄 COMPANY DEDUPLICATION MODE")
        print("=" * 50)
        
        if len(sys.argv) > 2:
            # Process specific file provided as argument
            file_path = Path(sys.argv[2])
            if file_path.exists():
                process_portfolio_file_for_deduplication(file_path)
            else:
                print(f"❌ File not found: {file_path}")
        else:
            # Interactive mode - find portfolio files
            print("Looking for portfolio JSON files...")
            
            # Look in output/runs/ for portfolio files
            runs_dir = Path("output/runs")
            if runs_dir.exists():
                portfolio_files = []
                for vc_dir in runs_dir.iterdir():
                    if vc_dir.is_dir():
                        for file in vc_dir.glob("*portfolio*.json"):
                            portfolio_files.append(file)
                
                if portfolio_files:
                    print(f"Found {len(portfolio_files)} portfolio files:")
                    for i, file in enumerate(portfolio_files, 1):
                        print(f"  {i}. {file}")
                    
                    choice = input(f"\nChoose file to deduplicate (1-{len(portfolio_files)}, or 'all'): ").strip()
                    
                    if choice.lower() == 'all':
                        for file in portfolio_files:
                            process_portfolio_file_for_deduplication(file)
                            print()
                    elif choice.isdigit():
                        idx = int(choice) - 1
                        if 0 <= idx < len(portfolio_files):
                            process_portfolio_file_for_deduplication(portfolio_files[idx])
                        else:
                            print("Invalid choice")
                    else:
                        print("Invalid choice")
                else:
                    print("No portfolio files found in output/runs/")
            else:
                print("output/runs/ directory not found")
        sys.exit(0)
    
    # Normal portfolio scraping mode
    if len(sys.argv) < 3:
        print("Usage: portfolio_ss.py <vc_url> <vc_name_fs>")
        print("   OR: portfolio_ss.py --dedupe [file_path]")
        sys.exit(1)
        
    vc_url = sys.argv[1].rstrip("/")
    vc_name_fs = sys.argv[2]
    vc_name = get_vc_name_from_url(vc_url)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")


    # ── discover portfolio URLs then company names (PARALLEL) ─────────────────
    urls_to_scrape   = find_portfolio_urls_enhanced(vc_url)
    
    # Apply URL limit for safety and cost control
    MAX_PORTFOLIO_URLS = 10
    if len(urls_to_scrape) > MAX_PORTFOLIO_URLS:
        print(f"⚠️  Found {len(urls_to_scrape)} portfolio URLs, limiting to first {MAX_PORTFOLIO_URLS} for safety")
        urls_to_scrape = urls_to_scrape[:MAX_PORTFOLIO_URLS]
    
    # Process URLs in parallel instead of sequentially
    all_companies: list[str] = process_portfolio_urls_parallel(urls_to_scrape, max_workers=6)


    all_companies = validate_companies_strictly(all_companies)
    
    # Apply deduplication if there are potential duplicates (ULTRA AGGRESSIVE)
    if len(all_companies) > 1:
        print(f"[DEDUP] Checking for duplicates in {len(all_companies)} companies...")
        deduplicated_companies = deduplicate_companies_with_llm(all_companies, use_aggressive=True)
        if len(deduplicated_companies) < len(all_companies):
            print(f"[DEDUP] Applied deduplication: {len(all_companies)} → {len(deduplicated_companies)} companies")
            all_companies = deduplicated_companies


    # ── write artefact ─────────────────────────────────────────────
    artefact = {
        "vc_name"     : vc_name,
        "vc_url"      : vc_url,
        "vc_domain"   : get_domain(vc_url),
        "extraction_timestamp": datetime.now().isoformat(),
        "companies"   : sorted(all_companies),
    }


    # Save to output/runs/<vc_name_fs>/<vc_name_fs>_portfolio_<ts>.json
    run_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(run_dir, exist_ok=True)
    out_path = os.path.join(run_dir, f"{vc_name_fs}_portfolio_{ts}.json")


    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artefact, f, indent=2)


    print(f"[OK] extracted {len(all_companies)} companies")
    print(f"OUTPUT_FILE: {out_path}")




def run_portfolio_discovery(vc_url, vc_name_fs, output_dir=None):
    """
    Run the portfolio discovery pipeline natively.
    Args:
        vc_url (str): VC website URL
        vc_name_fs (str): Filesystem-safe VC name
        output_dir (str, optional): Output directory. Defaults to output/runs/<vc_name_fs>/
    Returns:
        str: Path to output JSON file
    """
    from datetime import datetime
    import os, json
    vc_name = get_vc_name_from_url(vc_url)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    urls_to_scrape = find_portfolio_urls_enhanced(vc_url)
    # Apply URL limit for safety and cost control
    MAX_PORTFOLIO_URLS = 4
    if len(urls_to_scrape) > MAX_PORTFOLIO_URLS:
        print(f"⚠️  Found {len(urls_to_scrape)} portfolio URLs, limiting to first {MAX_PORTFOLIO_URLS} for safety")
        urls_to_scrape = urls_to_scrape[:MAX_PORTFOLIO_URLS]

    all_companies = []
    for u in urls_to_scrape:
        shots = take_smart_screenshots(u)
        companies = analyze_screenshots_precise(shots, u)
        all_companies.extend(companies)
    all_companies = validate_companies_strictly(all_companies)

    # Apply deduplication if there are potential duplicates
    if len(all_companies) > 1:
        print(f"[DEDUP] Checking for duplicates in {len(all_companies)} companies...")
        deduplicated_companies = deduplicate_companies_with_llm(all_companies)
        if len(deduplicated_companies) < len(all_companies):
            print(f"[DEDUP] Applied deduplication: {len(all_companies)} → {len(deduplicated_companies)} companies")
            all_companies = deduplicated_companies

    artefact = {
        "vc_name": vc_name,
        "vc_url": vc_url,
        "vc_domain": get_domain(vc_url),
        "extraction_timestamp": datetime.now().isoformat(),
        "companies": sorted(all_companies),
    }
    if not output_dir:
        output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{vc_name_fs}_portfolio_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artefact, f, indent=2)
    print(f"[OK] extracted {len(all_companies)} companies")
    return out_path


