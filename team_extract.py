# team_extract.py
"""
Crawl VC websites for team/people pages, extract team member names and roles, and update Airtable.
Parallel VC processing (3 VCs at a time) + GPT-based extraction + Race condition fixes
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

# Team-specific banned keywords for anchor links
TEAM_BANNED_KEYWORDS = [
    "privacy", "policy", "disclaimer", "terms", "cookies", "cookie", "legal",
    "accessibility", "sitemap", "javascript", "wp-login", "feed", "rss", "blog",
    "newsletter", "press", "news", "careers", "jobs", "employment", "mailto:",
    "tel:", "login", "logout", "admin", "signup", "register", "account",
    "dashboard", "esg", "portfolio", "internship", "perspectives",
    "contracts", "fellow", "companies", "resources", "media", "spotlight",
    "diversity", "sustainability", "inclusion", "resource", "cart", "announce",
    "chat", "investors", "join-us", "conduct", "history", "contact",
    # social / sharing
    "linkedin", "twitter", "facebook", "instagram", "social", "share",
    # IR / reports
    "investor-relations", "report", "annual-report", "sec-filings",
    # content hubs, press releases
    "insights", "stories", "whitepaper", "case-study", "webinar", "calendar",
    "podcast", "press-release", "pressrelease", "newsroom", "media-center", "featuring",
]

# Targeted keywords for team pages
TEAM_TARGET_KEYWORDS = [
    "team", "people", "about", "leadership", "partners",
    "advisors", "staff", "our-team", "meet-the-team",
    "our-people", "who-we-are", "board", "management",
    # Extended keywords for better coverage
    "founders", "executives", "bios", "crew", "founding-team",
    "founding-partners", "meet-our", "meet-us", "about-us",
    "profiles", "personnel", "investment-team", "team-members",
    "our-partners", "leadership-team"
]

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
    return any(bad in path for bad in TEAM_BANNED_KEYWORDS)

def is_same_domain(base: str, test: str) -> bool:
    return urlparse(base).netloc == urlparse(test).netloc

def normalize_url(u: str) -> str:
    p = urlparse(u)
    return p._replace(path=p.path.rstrip("/").lower()).geturl()

def is_team_link(anchor_text: str, href: str) -> bool:
    txt = (anchor_text or "") + " " + (href or "")
    txt = txt.lower()
    return any(kw in txt for kw in TEAM_TARGET_KEYWORDS)


def extract_team_with_gpt(page_text: str, page_url: str = "") -> list:
    """
    Use GPT-4 to extract team member names and roles from VC firm webpage text.
    """
    # Debug: show what we're sending to GPT
    text_to_send = page_text[:16000]
    print(f"      [GPT] Sending {len(text_to_send)} chars to GPT")
    print(f"      [GPT] Text preview: {text_to_send[:200]}...")

    prompt = f"""
You are a **precision team member extraction system** for venture capital firm websites.
Your goal is to extract **team member information** (names and roles) from the provided page text.

───────────────────────────────
### TASK
From the provided page text, identify all team members and output them as structured JSON.

───────────────────────────────
### RULES

1. **INCLUDE** these types of team members:
   - Partners (General Partners, Managing Partners, Limited Partners)
   - Principals
   - Associates
   - Venture Partners
   - Advisors
   - Board Members
   - Executive Team (CEO, CFO, CTO, etc.)
   - Investment Team members
   - Operating Partners

2. **EXCLUDE** the following:
   - Portfolio company team members (focus ONLY on the VC firm's team)
   - Guest speakers or event participants
   - Blog post authors (unless they're identified as team members)
   - External advisors mentioned in passing
   - Names mentioned in testimonials or case studies

3. **Name Extraction**
   - Extract full names (First Last or First Middle Last)
   - Include professional titles (Dr., PhD, MD, etc.) if present
   - Skip incomplete names (e.g., just "John" or just "Smith")
   - Handle different name formats (e.g., "John Smith, MD" or "Dr. Jane Doe")

4. **Role Extraction**
   - Use the exact role/title as stated on the page
   - If no role is given but context suggests one, infer it (e.g., "Partner", "Team Member")
   - Standardize common variations:
     * "Managing Partner" and "Managing Director" → use as stated
     * "GP" → "General Partner"
     * "VP" → "Venture Partner"

5. **Handling Multiple Roles**
   - If a person has multiple roles, combine them (e.g., "Managing Partner & Co-Founder")
   - Use the most senior/relevant role if only one can be chosen

6. **Deduplication**
   - Do not repeat the same person multiple times
   - If the same name appears with different roles, use the most complete/accurate one

7. **Team Sections**
   - Pay special attention to sections titled:
     * "Our Team", "Team", "People", "Leadership", "Partners"
     * "Investment Team", "Advisory Board", "Board of Directors"
   - Ignore sections like "Portfolio Companies", "Companies We've Backed"

8. **No Results Handling**
   - If no team members are found, return an empty array `[]`.

9. **Output Format**
   - Return **valid JSON only** (parsable by `json.loads()`).
   - Do not include extra commentary, markdown, or prose.
   - Each object must include:
     ```
     {{
       "name": "Full Name",
       "role": "Job Title/Role",
       "section": "Section where found (e.g., 'Partners', 'Advisory Board')"
     }}
     ```
   - Output must be a JSON array of these objects.

───────────────────────────────
### EXAMPLE OUTPUT

[
  {{
    "name": "John Smith",
    "role": "Managing Partner",
    "section": "Leadership Team"
  }},
  {{
    "name": "Jane Doe, PhD",
    "role": "General Partner",
    "section": "Partners"
  }},
  {{
    "name": "Robert Johnson",
    "role": "Principal",
    "section": "Investment Team"
  }},
  {{
    "name": "Sarah Williams",
    "role": "Venture Partner",
    "section": "Venture Partners"
  }}
]

───────────────────────────────
### INSTRUCTIONS

Analyze the text carefully, identify all team members with their roles, and produce the most accurate structured output possible.
Focus on the VC firm's internal team, NOT portfolio companies.
If no team members are found, return `[]` exactly.

───────────────────────────────
### PAGE TEXT
{text_to_send}
"""
    try:
        reply = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=2000,
        ).choices[0].message.content.strip()

        # Try to extract JSON list from reply
        match = re.search(r'\[.*\]', reply, re.S)
        if match:
            team_members = json.loads(match.group(0))
            if isinstance(team_members, list):
                print(f"      [GPT] Received {len(team_members)} team members")
                return team_members
    except Exception as e:
        print(f"      [GPT ERROR] {e}")

    print(f"      [GPT] No valid team members found in response")
    return []


def deduplicate_team_members(members: list) -> list:
    """Deduplicate team members by name (case-insensitive)"""
    seen = set()
    deduped = []
    for member in members:
        name_norm = member.get('name', '').strip().lower()
        if name_norm and name_norm not in seen:
            seen.add(name_norm)
            deduped.append(member)
    return deduped


def crawl_and_extract_team(url: str, vc_name: str) -> list:
    """
    Crawl a VC website and extract team member information.
    Returns: List of team members [{"name": "...", "role": "...", "section": "..."}]
    """
    print(f"\n  [{vc_name}] Starting team extraction...")

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

        team_links = []
        other_links = []  # Non-team but non-banned links

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
            if is_team_link(anchor_txt, href):
                team_links.append(full_url)
            elif not is_banned(full_url):
                # Not a team link, but also not banned - keep as backup
                other_links.append(full_url)

            visited.add(norm_url)

        # Build list of up to 3 UNIQUE pages
        # Start with homepage, then add team links, then other links
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

        # 2. Add team links (will auto-skip homepage if it's in team_links)
        for link in team_links:
            add_unique_page(link)

        # 3. Fill with other non-banned links (will auto-skip duplicates)
        for link in other_links:
            add_unique_page(link)

        # Debug: show what we found
        print(f"  [{vc_name}] [DEBUG] Found {len(team_links)} team links, {len(other_links)} other links")
        print(f"  [{vc_name}] [DEBUG] Pages to check URLs: {[urlparse(p).path or '/' for p in pages_to_check]}")

        print(f"  [{vc_name}] [PROCESSING] {len(pages_to_check)} UNIQUE pages:")
        for i, page in enumerate(pages_to_check):
            page_type = "(homepage)" if i == 0 else ("(team)" if page in team_links else "(other)")
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

                # Use GPT to extract team members from full page text
                return extract_team_with_gpt(html, page_url)
            except Exception as e:
                print(f"      [PAGE ERROR] {page_url}: {e}")
                return []

        all_members = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_page, page_url): page_url for page_url in pages_to_check}
            for future in as_completed(futures):
                members = future.result()
                all_members.extend(members)

        # Deduplicate team members
        deduped_members = deduplicate_team_members(all_members)

        print(f"  [{vc_name}] [OK] Extracted {len(deduped_members)} unique team members")
        return deduped_members

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


def update_airtable_team(vc_name: str, team_members: list, record_id: str = None):
    """
    Update Airtable with team member information.

    Args:
        vc_name: VC/Investor name (for logging)
        team_members: Extracted team data
        record_id: Optional Airtable record ID (if already known from batch mode)
    """
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, VC_TABLE_NAME)

    # If record_id not provided, search by name (single VC mode)
    if not record_id:
        records = table.all(fields=["VC/Investor Name", "Team Extractor Applied?"])
        rec = next((r for r in records if (r["fields"].get("VC/Investor Name") or "").strip().lower() == vc_name.strip().lower()), None)

        if not rec:
            print(f"  [{vc_name}] [WARN]  Not found in Airtable.")
            return

        # Only proceed if 'Team Extractor Applied?' is not true
        if rec["fields"].get("Team Extractor Applied?"):
            print(f"  [{vc_name}] [SKIP]  Already processed - skipping.")
            return

        record_id = rec["id"]

    # Format team members for display
    team_list = []
    for member in team_members:
        name = member.get('name', '')
        role = member.get('role', '')
        if name:
            team_list.append(f"{name} ({role})" if role else name)

    # Store both the formatted list and the full structured team info
    table.update(record_id, {
        "Team Members": "\n".join(team_list),
        "Team Extractor Applied?": True,
        "Team Members JSON": json.dumps(team_members, indent=2, ensure_ascii=False)
    }, typecast=True)

    print(f"  [{vc_name}] [SAVE] Updated Airtable: {len(team_members)} team members → {', '.join([m.get('name', '') for m in team_members[:3]])}{'...' if len(team_members) > 3 else ''}")


def process_single_vc(vc_record):
    """
    Process a single VC: crawl website → extract team → update Airtable
    """
    vc_name = vc_record["fields"].get("VC/Investor Name", "Unknown")
    url = vc_record["fields"].get("Website URL")
    record_id = vc_record["id"]  # Get record ID from the record we already have

    if not url:
        print(f"[{vc_name}] [WARN]  No website URL - skipping")
        return

    # Check if already processed
    if vc_record["fields"].get("Team Extractor Applied?"):
        print(f"[{vc_name}] [SKIP]  Already processed - skipping")
        return

    start_time = time.time()
    team_members = crawl_and_extract_team(url, vc_name)

    if team_members:
        # Pass record_id to avoid redundant Airtable search
        update_airtable_team(vc_name, team_members, record_id=record_id)
    else:
        print(f"  [{vc_name}] [WARN]  No team members found")

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
    print(f"BATCH TEAM EXTRACTION")
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
        fields=["VC/Investor Name", "Website URL", "Team Extractor Applied?"],
        sort=["VC/Investor Name"]  # A-Z sort
    )

    # Filter out already processed VCs
    unprocessed = [r for r in vc_records if not r["fields"].get("Team Extractor Applied?")]

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
        print(f"SINGLE VC TEAM EXTRACTION")
        print(f"{'='*60}")
        print(f"VC: {vc_name}")
        print(f"URL: {url}")
        print(f"{'='*60}\n")

        team_members = crawl_and_extract_team(url, vc_name)
        print(f"\nExtracted team members: {json.dumps(team_members, indent=2, ensure_ascii=False)}")
        update_airtable_team(vc_name, team_members)
        print(f"\n{'='*60}")
        print("DONE")
        print(f"{'='*60}\n")

    else:
        print("Usage:")
        print("  Single VC:  python team_extract.py '<VC Name>' <Website URL>")
        print("  Batch mode: python team_extract.py --batch [max_vcs] [parallel_vcs]")
        print("")
        print("Examples:")
        print("  python team_extract.py 'Andreessen Horowitz' https://a16z.com")
        print("  python team_extract.py --batch 10 3    # Process 10 VCs, 3 at a time")
        print("  python team_extract.py --batch         # Process all VCs, 3 at a time")
