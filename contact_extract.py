# contact_extract.py
"""
Crawl a VC website for contact-related pages, extract all contact and general emails, deduplicate, and update Airtable.
"""
import sys
import json
import time
import re
from urllib.parse import urljoin, urlparse
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pyairtable import Table
from dotenv import load_dotenv
import os
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# Airtable config (reuse from workflow.py)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or "YOUR_API_KEY"
BASE_ID          = os.getenv("AIRTABLE_BASE_ID") or "YOUR_BASE_ID"
VC_TABLE_NAME    = os.getenv("AIRTABLE_VC_TABLE") or "VC Database"

# Contact-specific banned keywords for anchor links
CONTACT_BANNED_KEYWORDS = [
    "privacy", "policy", "disclaimer", "terms", "cookies", "cookie", "legal",
    "accessibility", "sitemap", "javascript", "wp-login", "feed", "rss", "blog",
    "newsletter", "press", "news", "careers", "jobs", "employment", "mailto:",
    "tel:", "login", "logout", "admin", "signup", "register", "account",
    "dashboard", "esg", "portfolio", "team", "internship", "perspectives",
    "contracts", "fellow", "companies", "resources", "media", "spotlight",
    "diversity", "sustainability", "inclusion", "resource", "cart", "announce",
    "chat", "investors", "join-us", "who-we-are", "conduct", "history",
    # social / sharing
    "linkedin", "twitter", "facebook", "instagram", "social", "share",
    # IR / reports
    "investor-relations", "report", "annual-report", "sec-filings",
    # content hubs, press releases
    "insights", "stories", "whitepaper", "case-study", "webinar", "calendar",
    "podcast", "press-release", "pressrelease", "newsroom", "media-center", "featuring",
    # weird glyph hack that occasionally appears
    "d i v e r s i t y",
]
# Targeted keywords for contact pages
CONTACT_TARGET_KEYWORDS = ["contact", "contact-us", "get-in-touch", "reach-us"]
SKIP_EXTENSIONS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".rar",
    ".csv", ".txt", ".mp4", ".mp3", ".jpg", ".png", ".jpeg", ".gif", ".svg"
)


# OpenAI client setup
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def is_filetype(url: str) -> bool:
    return url.lower().split("?")[0].endswith(SKIP_EXTENSIONS)

def is_banned(url: str) -> bool:
    path = urlparse(url).path.lower()
    scheme = urlparse(url).scheme
    if scheme in ("mailto", "tel", "javascript"):
        return True
    return any(bad in path for bad in CONTACT_BANNED_KEYWORDS)

def is_same_domain(base: str, test: str) -> bool:
    return urlparse(base).netloc == urlparse(test).netloc

def normalize_url(u: str) -> str:
    p = urlparse(u)
    return p._replace(path=p.path.rstrip("/").lower()).geturl()

def is_contact_link(anchor_text: str, href: str) -> bool:
    txt = (anchor_text or "") + " " + (href or "")
    txt = txt.lower()
    return any(kw in txt for kw in CONTACT_TARGET_KEYWORDS)



def extract_contacts_with_gpt(page_text: str) -> list:
    """
    Use GPT-4.1 to extract all contact blocks: numbers, emails, and context for each contact from raw page text.
    """
    prompt = (
        "Extract all contact information blocks for the VC firm from the following web page text. "
        "For each contact block, return a JSON object with: 'context' (the label or section, e.g. 'Press Enquiries', 'Investor Enquiries', or 'General Contact'), 'emails' (list of emails), and 'numbers' (list of phone numbers, including international format if present). "
        "Return a JSON list of these objects. Only include blocks that are for general, press, investor, or info contact (not people or careers).\n\n"
        f"PAGE TEXT:\n{page_text[:16000]}"
    )
    try:
        reply = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        ).choices[0].message.content.strip()
        # Try to extract JSON list from reply
        match = re.search(r'\[.*\]', reply, re.S)
        if match:
            blocks = json.loads(match.group(0))
            if isinstance(blocks, list):
                return blocks
    except Exception as e:
        print(f"[GPT ERROR] {e}")
    return []


def deduplicate_emails(emails: list) -> list:
    seen = set()
    deduped = []
    for e in emails:
        e_norm = e.strip().lower()
        if e_norm not in seen:
            seen.add(e_norm)
            deduped.append(e)
    return deduped


def crawl_and_extract_contacts(url: str) -> list:
    opt = uc.ChromeOptions()
    driver = uc.Chrome(options=opt)
    visited = set()
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        anchors = soup.find_all("a", href=True)
        contact_links = []
        for a in anchors:
            anchor_txt = a.get_text(" ", strip=True)
            href = a["href"]
            full_url = urljoin(url, href)
            norm_url = normalize_url(full_url)
            if not is_same_domain(url, full_url):
                continue
            if norm_url in visited:
                continue
            if not full_url.startswith("http"):
                continue
            if is_banned(full_url):
                continue
            if is_filetype(full_url):
                continue
            if is_contact_link(anchor_txt, href):
                contact_links.append(full_url)
            visited.add(norm_url)
        # Always include homepage as fallback, cap at 3 pages
        pages_to_check = [url] + contact_links[:2]  # max 3 pages

        def process_page(page_url):
            try:
                driver.get(page_url)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                html = driver.page_source
                # Use GPT to extract contact blocks from full page text
                return extract_contacts_with_gpt(html)
            except Exception as e:
                print(f"[PAGE ERROR] {page_url}: {e}")
                return []

        all_blocks = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_page, page_url): page_url for page_url in pages_to_check}
            for future in as_completed(futures):
                blocks = future.result()
                all_blocks.extend(blocks)
        # Deduplicate emails and numbers across all blocks
        seen_emails = set()
        seen_numbers = set()
        deduped_blocks = []
        for block in all_blocks:
            emails = [e for e in block.get('emails', []) if isinstance(e, str)]
            numbers = [n for n in block.get('numbers', []) if isinstance(n, str)]
            context = block.get('context', '')
            # Deduplicate within block
            emails = [e for e in emails if e.strip().lower() not in seen_emails and not seen_emails.add(e.strip().lower())]
            numbers = [n for n in numbers if n.strip() not in seen_numbers and not seen_numbers.add(n.strip())]
            if emails or numbers:
                deduped_blocks.append({'context': context, 'emails': emails, 'numbers': numbers})
        return deduped_blocks
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def update_airtable_contact_emails(vc_name: str, contact_blocks: list):
    table = Table(AIRTABLE_API_KEY, BASE_ID, VC_TABLE_NAME)
    # Find record by VC/Investor Name
    records = table.all(fields=["VC/Investor Name", "Contact Extractor Applied?"])
    rec = next((r for r in records if (r["fields"].get("VC/Investor Name") or "").strip().lower() == vc_name.strip().lower()), None)
    if not rec:
        print(f"[WARN] VC '{vc_name}' not found in Airtable.")
        return
    # Only proceed if 'Contact Extractor Applied?' is not true
    if rec["fields"].get("Contact Extractor Applied?"):
        print(f"[SKIP] Contact Extractor already applied for {vc_name}.")
        return
    record_id = rec["id"]
    # Aggregate all emails for the flat field
    all_emails = []
    for block in contact_blocks:
        all_emails.extend(block.get('emails', []))
    all_emails = deduplicate_emails(all_emails)
    # Store both the flat emails and the full structured contact info
    table.update(record_id, {
        "contact emails": ", ".join(all_emails),
        "Contact Extractor Applied?": True,
        "Contact Extractor JSON": json.dumps(contact_blocks, indent=2, ensure_ascii=False)
    }, typecast=True)
    print(f"[Airtable] Updated contact emails for {vc_name}: {all_emails}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python contact_extract.py '<VC Name>' <Website URL>")
        sys.exit(1)
    vc_name = sys.argv[1]
    url = sys.argv[2]
    print(f"\nCrawling for contact info: {vc_name} ({url})\n")
    contact_blocks = crawl_and_extract_contacts(url)
    print(f"Extracted contact blocks: {json.dumps(contact_blocks, indent=2, ensure_ascii=False)}")
    update_airtable_contact_emails(vc_name, contact_blocks)
