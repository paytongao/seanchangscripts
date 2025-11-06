W# contact_extract_updated.py
"""
Crawl VC websites for contact-related pages, extract all contact emails, deduplicate, and update Airtable.
NOW WITH: Parallel VC processing (3 VCs at a time) + Improved GPT prompt + Race condition fixes
"""
import sys
import json
import time
import re
import random
import threading
import tempfile
import shutil
from urllib.parse import urljoin, urlparse
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pyairtable import Api
from dotenv import load_dotenv
import os
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# Airtable config
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY") or "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
BASE_ID = os.getenv("AIRTABLE_BASE_ID") or "app768aQ07mCJoyu8"
VC_TABLE_NAME = os.getenv("AIRTABLE_VC_TABLE") or "VC Database"

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

# Global driver initialization lock to prevent race conditions
driver_init_lock = threading.Lock()
driver_initialized = False

def sanitize_for_filename(name: str) -> str:
    """Clean VC name for safe use in file paths"""
    # Strip and collapse all whitespace (spaces, newlines, tabs)
    clean = re.sub(r'\s+', '_', name.strip())
    # Remove invalid Windows path characters
    clean = re.sub(r'[<>:"/\\|?*]', '', clean)
    # Keep only safe characters (alphanumeric, underscore, hyphen)
    clean = re.sub(r'[^a-zA-Z0-9_-]', '', clean)
    # Limit length to 20 chars, ensure not empty
    return clean[:20] if clean else 'vc'

def ensure_driver_initialized():
    """Pre-initialize undetected_chromedriver once before parallel execution"""
    global driver_initialized

    if not driver_initialized:
        with driver_init_lock:
            if not driver_initialized:  # Double-check after acquiring lock
                print("  [INIT] Pre-initializing Chrome driver...")
                try:
                    # Create and immediately close a dummy driver to extract binaries
                    opt = uc.ChromeOptions()
                    # opt.add_argument('--headless')  # Disabled for visible browser
                    opt.add_argument('--no-sandbox')
                    opt.add_argument('--disable-dev-shm-usage')
                    dummy = uc.Chrome(options=opt)
                    dummy.quit()
                    driver_initialized = True
                    print("  [INIT] [OK] Chrome driver ready\n")
                except Exception as e:
                    print(f"  [INIT] [WARN] Driver init warning: {e}")
                    # Continue anyway - might work

def create_driver_with_retry(vc_name, max_retries=3):
    """Create Chrome driver with retry logic and unique user data directory"""
    # Small random delay to stagger driver creation
    time.sleep(random.uniform(0.1, 0.5))

    # Create unique temp directory for this VC's Chrome profile (sanitize name for valid path)
    user_data_dir = tempfile.mkdtemp(prefix=f"chrome_{sanitize_for_filename(vc_name)}_")

    for attempt in range(max_retries):
        try:
            opt = uc.ChromeOptions()
            # opt.add_argument('--headless')  # Disabled for visible browser
            opt.add_argument('--no-sandbox')
            opt.add_argument('--disable-dev-shm-usage')
            opt.add_argument(f'--user-data-dir={user_data_dir}')

            driver = uc.Chrome(options=opt)
            return driver, user_data_dir
        except (PermissionError, FileExistsError) as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"  [{vc_name}] [WARN]  Driver creation failed (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                # Cleanup temp dir on final failure
                try:
                    shutil.rmtree(user_data_dir, ignore_errors=True)
                except:
                    pass
                raise

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


def extract_contacts_with_gpt(page_text: str, page_url: str = "") -> list:
    """
    Use GPT-4.1 to extract only organizational contact email blocks from raw VC firm webpage text.
    """
    # Debug: show what we're sending to GPT
    text_to_send = page_text[:16000]
    print(f"      [GPT] Sending {len(text_to_send)} chars to GPT")
    print(f"      [GPT] Text preview: {text_to_send[:200]}...")

    prompt = f"""
You are a **precision contact extraction system** for venture capital firm websites.
Your goal is to extract **organizational contact emails** (not personal or career-related) and group them by their relevant context.

───────────────────────────────
### TASK
From the provided page text, identify all valid contact email blocks and output them as structured JSON.

───────────────────────────────
### RULES

1. **INCLUDE** emails meant for these purposes only:
   - General inquiries → info@, hello@, contact@
   - Press / Media → press@, media@, communications@
   - Investor Relations → ir@, investor@, lp@
   - Business Development / Partnerships → bd@, partnerships@

2. **EXCLUDE** the following:
   - Personal emails containing a person's name (e.g., john@, jane.smith@)
     *UNLESS* it is explicitly labeled as the **sole official contact** (e.g., "Investor Contact: john@vcfirm.com")
   - Career / HR / recruiting emails → careers@, hr@, jobs@, hiring@, recruitment@, applicants@
   - Placeholder / fake / example emails (e.g., example.com, yourcompany.com, test@)
   - Obfuscated formats (e.g., info [at] vcfirm [dot] com)
   - Contact forms with no emails → ignore entirely

3. **Email Validation**
   - Must follow standard format: `name@domain.extension`
   - Must use a real domain (not example.com or placeholders)
   - Skip malformed or incomplete addresses

4. **Context Extraction**
   - Use the actual section heading if present (e.g., "Press Enquiries", "Investor Relations")
   - If no heading exists, infer context from the email prefix:
     * info@, hello@, contact@ → "General Contact"
     * press@, media@, communications@ → "Press/Media"
     * ir@, investor@, lp@ → "Investor Relations"
     * bd@, partnerships@ → "Business Development"
   - Include region if specified (e.g., "General Contact – US")

5. **Handling Multiple Emails**
   - Group multiple emails serving the same purpose into one block.
   - If the same email appears under different headings, use the **most specific** or descriptive context.
   - If a single section mixes categories (e.g., press@ and ir@ in one line), **split them into separate context blocks.**
   - If several general emails exist (e.g., info@ and hello@), choose the one that appears first or under a clearer heading.

6. **Deduplication**
   - Do not repeat the same email in multiple contexts.
   - Merge duplicates into the most accurate context.

7. **Regional Contacts**
   - If emails are labeled by region, include the region in context:
     - Example: "General Contact – Europe", "Press/Media – US"

8. **No Results Handling**
   - If no valid emails are found, return an empty array `[]`.

9. **Output Format**
   - Return **valid JSON only** (parsable by `json.loads()`).
   - Do not include extra commentary, markdown, or prose.
   - Each object must include:
     ```
     {{
       "context": "string",
       "emails": ["email1", "email2"]
     }}
     ```
   - Output must be a JSON array of these objects.

───────────────────────────────
### EXAMPLE OUTPUT

[
  {{
    "context": "Press/Media",
    "emails": ["press@vcfirm.com", "media@vcfirm.com"]
  }},
  {{
    "context": "Investor Relations",
    "emails": ["ir@vcfirm.com"]
  }},
  {{
    "context": "General Contact",
    "emails": ["info@vcfirm.com"]
  }}
]

───────────────────────────────
### INSTRUCTIONS

Analyze the text carefully, infer logical groupings, and produce the most accurate structured output possible.
If no valid contacts exist, return `[]` exactly.

───────────────────────────────
### PAGE TEXT
{text_to_send}
"""
    try:
        reply = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=800,
        ).choices[0].message.content.strip()

        # Try to extract JSON list from reply
        match = re.search(r'\[.*\]', reply, re.S)
        if match:
            blocks = json.loads(match.group(0))
            if isinstance(blocks, list):
                print(f"      [GPT] Received {len(blocks)} contact blocks")
                return blocks
    except Exception as e:
        print(f"      [GPT ERROR] {e}")

    print(f"      [GPT] No valid contacts found in response")
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


def crawl_and_extract_contacts(url: str, vc_name: str) -> list:
    """
    Crawl a VC website and extract contact blocks.
    Returns: List of contact blocks [{"context": "...", "emails": [...]}]
    """
    print(f"\n  [{vc_name}] Starting contact extraction...")

    # Use retry logic to create driver with unique profile
    driver, user_data_dir = create_driver_with_retry(vc_name)
    visited = set()

    try:
        print(f"  [{vc_name}] [VISITING] Homepage: {url}")
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        anchors = soup.find_all("a", href=True)

        print(f"  [{vc_name}] [FOUND] {len(anchors)} total links on homepage")

        contact_links = []
        other_links = []  # Non-contact but non-banned links

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

            # Categorize links
            if is_contact_link(anchor_txt, href):
                contact_links.append(full_url)
            elif not is_banned(full_url):
                # Not a contact link, but also not banned - keep as backup
                other_links.append(full_url)

            visited.add(norm_url)

        # Build list of up to 3 UNIQUE pages
        # Start with homepage, then add contact links, then other links
        # Use normalized URLs to avoid duplicates

        pages_to_check = []
        seen_normalized = set()

        # Helper to add page if not duplicate
        def add_unique_page(page_url):
            norm = normalize_url(page_url)
            if norm not in seen_normalized and len(pages_to_check) < 3:
                print(f"      [ADD] {page_url} (normalized: {norm})")
                pages_to_check.append(page_url)
                seen_normalized.add(norm)
                return True
            else:
                if norm in seen_normalized:
                    print(f"      [SKIP] {page_url} (duplicate: {norm})")
                return False

        # 1. Always include homepage first
        add_unique_page(url)

        # 2. Add contact links (will auto-skip homepage if it's in contact_links)
        for link in contact_links:
            add_unique_page(link)

        # 3. Fill with other non-banned links (will auto-skip duplicates)
        for link in other_links:
            add_unique_page(link)

        # Debug: show what we found
        print(f"  [{vc_name}] [DEBUG] Found {len(contact_links)} contact links, {len(other_links)} other links")
        print(f"  [{vc_name}] [DEBUG] Pages to check URLs: {[urlparse(p).path or '/' for p in pages_to_check]}")

        print(f"  [{vc_name}] [PROCESSING] {len(pages_to_check)} UNIQUE pages:")
        for i, page in enumerate(pages_to_check):
            page_type = "(homepage)" if i == 0 else ("(contact)" if page in contact_links else "(other)")
            print(f"      - {page} → {urlparse(page).path or '/'} {page_type}")

        def process_page(page_url):
            try:
                page_path = urlparse(page_url).path or '/'
                print(f"      [VISITING] {page_path}")

                driver.get(page_url)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                html = driver.page_source

                print(f"      [EXTRACTED] {len(html)} chars from {page_path}")

                # Use GPT to extract contact blocks from full page text
                return extract_contacts_with_gpt(html, page_url)
            except Exception as e:
                print(f"      [PAGE ERROR] {page_url}: {e}")
                return []

        all_blocks = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_page, page_url): page_url for page_url in pages_to_check}
            for future in as_completed(futures):
                blocks = future.result()
                all_blocks.extend(blocks)

        # Deduplicate emails and numbers across all blocks
        seen_emails = set()
        deduped_blocks = []
        for block in all_blocks:
            emails = [e for e in block.get('emails', []) if isinstance(e, str)]
            context = block.get('context', '')
            # Deduplicate within block
            emails = [e for e in emails if e.strip().lower() not in seen_emails and not seen_emails.add(e.strip().lower())]
            if emails:
                deduped_blocks.append({'context': context, 'emails': emails})

        print(f"  [{vc_name}] [OK] Extracted {len(deduped_blocks)} contact blocks, {len(seen_emails)} unique emails")
        return deduped_blocks

    except Exception as e:
        print(f"  [{vc_name}] [ERROR] Crawl error: {e}")
        return []
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # Cleanup temp directory
        try:
            shutil.rmtree(user_data_dir, ignore_errors=True)
        except:
            pass


def update_airtable_contact_emails(vc_name: str, contact_blocks: list, record_id: str = None):
    """
    Update Airtable with contact blocks.

    Args:
        vc_name: VC/Investor name (for logging)
        contact_blocks: Extracted contact data
        record_id: Optional Airtable record ID (if already known from batch mode)
    """
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, VC_TABLE_NAME)

    # If record_id not provided, search by name (single VC mode)
    if not record_id:
        records = table.all(fields=["VC/Investor Name", "Contact Extractor Applied?"])
        rec = next((r for r in records if (r["fields"].get("VC/Investor Name") or "").strip().lower() == vc_name.strip().lower()), None)

        if not rec:
            print(f"  [{vc_name}] [WARN]  Not found in Airtable.")
            return

        # Only proceed if 'Contact Extractor Applied?' is not true
        if rec["fields"].get("Contact Extractor Applied?"):
            print(f"  [{vc_name}] [SKIP]  Already processed - skipping.")
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

    print(f"  [{vc_name}] [SAVE] Updated Airtable: {len(all_emails)} emails → {', '.join(all_emails[:3])}{'...' if len(all_emails) > 3 else ''}")


def process_single_vc(vc_record):
    """
    Process a single VC: crawl website → extract contacts → update Airtable
    """
    vc_name = vc_record["fields"].get("VC/Investor Name", "Unknown")
    url = vc_record["fields"].get("Website URL")
    record_id = vc_record["id"]  # Get record ID from the record we already have

    if not url:
        print(f"[{vc_name}] [WARN]  No website URL - skipping")
        return

    # Check if already processed
    if vc_record["fields"].get("Contact Extractor Applied?"):
        print(f"[{vc_name}] [SKIP]  Already processed - skipping")
        return

    start_time = time.time()
    contact_blocks = crawl_and_extract_contacts(url, vc_name)

    if contact_blocks:
        # Pass record_id to avoid redundant Airtable search
        update_airtable_contact_emails(vc_name, contact_blocks, record_id=record_id)
    else:
        print(f"  [{vc_name}] [WARN]  No contacts found")

    elapsed = time.time() - start_time
    print(f"  [{vc_name}] [TIME]  Completed in {elapsed:.1f}s")


def batch_process_vcs(max_vcs=None, parallel_vcs=3):
    """
    Process multiple VCs in parallel from Airtable.

    Args:
        max_vcs: Maximum number of VCs to process (None = all)
        parallel_vcs: Number of VCs to process simultaneously (default: 3)
    """
    print(f"\n{'='*60}")
    print(f"BATCH CONTACT EXTRACTION")
    print(f"{'='*60}")
    print(f"Parallel VCs: {parallel_vcs}")
    print(f"Max VCs to process: {max_vcs or 'ALL'}")
    print(f"{'='*60}\n")

    # Pre-initialize driver BEFORE parallel execution
    ensure_driver_initialized()

    # Fetch VCs from Airtable with A-Z sorting
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, VC_TABLE_NAME)
    vc_records = table.all(
        fields=["VC/Investor Name", "Website URL", "Contact Extractor Applied?"],
        sort=["VC/Investor Name"]  # A-Z sort
    )

    # Filter out already processed VCs
    unprocessed = [r for r in vc_records if not r["fields"].get("Contact Extractor Applied?")]

    print(f"[INFO] Total VCs in Airtable: {len(vc_records)}")
    print(f"[INFO] Already processed: {len(vc_records) - len(unprocessed)}")
    print(f"[INFO] Remaining to process: {len(unprocessed)}\n")

    if not unprocessed:
        print("[OK] All VCs already processed!")
        return

    # Limit if specified
    if max_vcs:
        unprocessed = unprocessed[:max_vcs]
        print(f"[TARGET] Processing first {len(unprocessed)} VCs\n")

    # Process in parallel
    start_time = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=parallel_vcs) as executor:
        futures = {executor.submit(process_single_vc, vc): vc for vc in unprocessed}

        for future in as_completed(futures):
            vc = futures[future]
            try:
                future.result()
                completed += 1
                print(f"\n{'-'*60}")
                print(f"Progress: {completed}/{len(unprocessed)} VCs completed")
                print(f"{'-'*60}\n")
            except Exception as e:
                vc_name = vc["fields"].get("VC/Investor Name", "Unknown")
                print(f"[{vc_name}] [ERROR] FATAL ERROR: {e}")
                import traceback
                traceback.print_exc()

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE")
    print(f"{'='*60}")
    print(f"[OK] Processed: {completed}/{len(unprocessed)} VCs")
    print(f"[TIME]  Total time: {total_time:.1f}s")
    print(f"[TIME]  Average: {total_time/completed:.1f}s per VC" if completed > 0 else "")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Check for batch mode
    if len(sys.argv) > 1 and sys.argv[1] == "--batch":
        # Batch mode: process multiple VCs
        max_vcs = int(sys.argv[2]) if len(sys.argv) > 2 else None
        parallel_vcs = int(sys.argv[3]) if len(sys.argv) > 3 else 3
        batch_process_vcs(max_vcs=max_vcs, parallel_vcs=parallel_vcs)

    elif len(sys.argv) >= 3:
        # Single VC mode
        vc_name = sys.argv[1]
        url = sys.argv[2]
        print(f"\n{'='*60}")
        print(f"SINGLE VC CONTACT EXTRACTION")
        print(f"{'='*60}")
        print(f"VC: {vc_name}")
        print(f"URL: {url}")
        print(f"{'='*60}\n")

        contact_blocks = crawl_and_extract_contacts(url, vc_name)
        print(f"\nExtracted contact blocks: {json.dumps(contact_blocks, indent=2, ensure_ascii=False)}")
        update_airtable_contact_emails(vc_name, contact_blocks)
        print(f"\n{'='*60}")
        print("DONE")
        print(f"{'='*60}\n")

    else:
        print("Usage:")
        print("  Single VC:  python contact_extract_updated.py '<VC Name>' <Website URL>")
        print("  Batch mode: python contact_extract_updated.py --batch [max_vcs] [parallel_vcs]")
        print("")
        print("Examples:")
        print("  python contact_extract_updated.py 'Andreessen Horowitz' https://a16z.com")
        print("  python contact_extract_updated.py --batch 10 3    # Process 10 VCs, 3 at a time")
        print("  python contact_extract_updated.py --batch         # Process all VCs, 3 at a time")
