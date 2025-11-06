import os
import json
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import random
import re
import requests
import sys
from openai import OpenAI
from dotenv import load_dotenv
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
import gc
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urlunparse
from pathlib import Path

BANNED_ANCHOR_KEYWORDS = [  # same list you had in main.py (trim if you like)
   "privacy","policy","disclaimer","terms","cookies","login","mailto:",
   "tel:","press","news","careers","jobs","team","pdf","svg", "leadership", "signs",
   "contact", "contact-us", "cart", "publications", "legal", "board-of-directors", "investors", "author",
   "medical-affairs", "our-programs", "open-positions", "stock-information", "stock-quote", "corporate-governance", 
   "sec-filings", "sec-filing", "investor-relations", "investor", "investors", "stockholder", "shareholder",
   "financials", "financial", "finances", "funding", "fundraise", "fundraising"
]

# Slugs that almost always hold pipeline / science content
PRIORITY_SLUGS = [
    "pipeline", "program", "science", "technology", "platform",
    "our-science", "our-technology", "our-platform"
]

SKIP_EXT = (".pdf",".doc",".ppt",".xls",".zip",".mp3",".mp4",".jpg",".png",".gif")

_CANON_INDEX_RE = re.compile(r"/index(?:\.html?|/)$", flags=re.I)

def _strip_www(netloc: str) -> str:
    return netloc[4:] if netloc.startswith("www.") else netloc

from urllib.parse import urlparse, urljoin, urlunparse

driver_creation_lock = threading.Lock()

def _launch_browser(headless: bool = True, port: int = 9222):
    opt = uc.ChromeOptions()
    if headless:
        opt.add_argument("--headless=new")
    # a tiny bit of jitter helps avoid bot detection
    opt.add_argument(f"--user-agent=Mozilla/5.0 z{random.randint(1111,9999)}")
    opt.add_argument(f"--remote-debugging-port={port}")
    # Lock to prevent undetected_chromedriver race condition
    with driver_creation_lock:
        return uc.Chrome(options=opt)

def _extract_visible_text(driver, limit: int = 0):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    txt = soup.get_text(" ", strip=True)
    return txt if not limit else txt[:limit]

def _extract_visible_text_from_url(url, scrolls=4, limit=0):
    def _do_extract():
        driver = _launch_browser(headless=True)
        try:
            driver.get(url)
            WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            driver.execute_script("window.scrollTo(0,400)")
            time.sleep(0.3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.3)
            height = driver.execute_script("return document.body.scrollHeight")
            for y in range(0, height, 800):
                driver.execute_script(f"window.scrollTo(0, {y})")
                time.sleep(0.3)
            txt = _extract_visible_text(driver, limit or 0)
            return txt
        finally:
            try:
                driver.quit()
            except Exception:
                pass
    # Use timeout_handler to avoid hangs
    txt, err = timeout_handler(_do_extract, timeout_duration=PAGE_TIMEOUT)
    if err:
        print(f"      [skip] {url} - Selenium timeout or error: {err}")
        return ""
    return txt


# ── Console-encoding hardening (Windows CP-1252 can’t print emoji) ──

if (
    sys.platform.startswith("win")
    and sys.stdout.encoding
    and sys.stdout.encoding.lower() != "utf-8"
):
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8",
                      errors="replace", buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode="w", encoding="utf-8",
                      errors="replace", buffering=1)

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OUTPUT_DIR = "output"

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)


# FIXED: Enhanced timeout configuration
MAX_PAGES_TO_ANALYZE = 4 # Reduced for timeout management
MAX_FIELDS_PER_STARTUP = 5  # Reduced for efficiency
COMPANY_TIMEOUT = 300  # 5 minutes per company
PAGE_TIMEOUT = 60  # 1 minute per page
AI_TIMEOUT = 45  # 45 seconds for AI requests
TOTAL_TIMEOUT = 1800  # 30 minutes total


STARTUP_3FIELDS_PROMPT = """
You are a precision data-extraction agent for biotech intelligence.

Goal → Return **one** JSON object with **exactly three** top-level keys
(list or null):

  "drug_modality" : list | null
  "disease_focus" : list | null
  "geography"     : list | null

════════════════════════════════════════════════════════════════════
GLOBAL RULES

1 • Use **only what is stated explicitly** about the company – no guessing.  
2 • If a key is completely missing, set it to null (or an empty list).  
3 • Ignore boiler-plate blocks that add no pipeline info
     (press releases about financing, careers, cookie banners, etc.).  
4 • **STRICT** Output must be valid JSON, no markdown fences. All output must be in English. If any content is in another language, translate it to English in the output.

5 • **VERTICAL FILTER (STRICT)** – Discard statements that apply *solely*
     to non-therapeutic verticals (diagnostics, devices, digital health,
     AI platforms, CRO services, etc.).  
     – If a sentence ties a criterion to such a vertical, skip it.  
     – When in doubt, exclude.
6 • **HORIZONTAL FILTER (STRICT)** – Discard statements that apply
     *only* to other companies, market commentary, or job posts.
        – If a sentence describes other companies, skip it.
        – If a sentence is about market trends, skip it.
7 • **NO INFERENCES** – Do not add or infer any information that is not
     explicitly stated in the text.
8 • **NO EXPANSIONS** – Do not expand or generalise beyond what is
     explicitly stated in the text.
9 • **THERAPEUTICS/DRUG DEVELOPMENT ONLY** - Only extract information that applies to the company's own therapeutic drug development activities.
10 • **PARTNERSHIP FILTER** – Exclude modalities or diseases that clearly belong to 
     partners/collaborators rather than the company's own pipeline.
11 • **SUBSIDIARY FILTER (STRICT)** – Exclude any information that applies to subsidiary companies,
     sister companies, portfolio companies, or other separate entities. Only extract data about
     the primary company being analyzed.
12 • **PARENT COMPANY FILTER (STRICT)** – Do not extract anything from parent companies. Only extract 
     information about the specific company being investigated. If content discusses parent company 
     activities, partnerships, or capabilities, exclude it entirely.
13 • **DRUG DEVELOPMENT REQUIREMENT (REINFORCED)** – Only extract drug modalities when the company 
     explicitly states they are developing, creating, or pursuing drugs/therapeutics. Do NOT extract 
     modalities mentioned only in the context of drug delivery, platform services, research tools, 
     or enabling technologies unless clearly integrated with their own therapeutic development.
14 • **TREATMENT-DISEASE CAPTURE** – If it is clear that the company is developing some kind of treatment for a disease, extract both the treatment modality and target disease even if not using exact technical terminology. Be conservative with this approach. 
     EXAMPLE: "We are creating new treatments for medically vulnerable patient populations to prevent bacterial bloodstream and antimicrobial resistant (AMR) infections as well as to treat GI-related immune diseases" → Extract: bacterial bloodstream infections, antimicrobial resistant (AMR) infections, GI-related immune diseases.
15 • **CONTEXT-CLUES** – Utilize contextual information from the text to aid in identifying relevant modalities and diseases. Think about each sentence in the context of the company's activities before you make your decision. Think about everything before you do it. Does it align with the detailed specifications? 

──────── FIELD-LEVEL GUIDANCE ────────
"drug_modality"
  • **THERAPEUTIC MODALITY CATEGORIES** - STRICTLY Extract modalities the company is **actually developing** using these industry-standard categories:
    **1. SMALL MOLECULES:** conventional inhibitors/activators, covalent inhibitors, allosteric modulators, PROTACs, molecular glues
    **2. GENETIC MEDICINES:** viral gene therapy (AAV, lentivirus), non-viral gene therapy, CRISPR/Cas9, base/prime editors, TALENs/ZFNs, mRNA therapeutics, siRNA/miRNA, antisense oligonucleotides (ASOs), self-amplifying RNA (saRNA), circular RNA (circRNA)
    **3. BIOLOGICS:** monoclonal antibodies (mAbs), bispecific antibodies, antibody-drug conjugates (ADCs), fusion proteins, therapeutic enzymes, Fc-modified antibodies
    **4. PEPTIDES & PROTEINS:** peptide therapeutics, protein replacement therapies, hormone therapies (insulin, EPO, growth hormone)
    **5. CELL THERAPIES:** CAR-T cells, TCR-T cells, CAR-NK cells, iPSC-derived therapies, stem cell therapies (hematopoietic, mesenchymal, embryonic)
    **6. IMMUNOTHERAPIES:** checkpoint inhibitors (PD-1, PD-L1, CTLA-4), cytokine therapies, tumor vaccines, immune agonists (TLR agonists), engineered T cell/APC platforms
    **7. MICROBIOME-BASED:** live biotherapeutic products (LBPs), postbiotics/metabolite-based therapies, fecal microbiota transplantation (FMT)
    **8. NEUROMODULATORS:** botulinum toxin, neuropeptides (CGRP inhibitors), bioelectronic medicine/vagus nerve stimulation, neural pathway modulators (NMDA)
    **9. ONCOLYTIC VIRUSES:** engineered viruses (HSV, adenovirus) that lyse cancer cells, immunostimulatory virus platforms
    **10. VACCINES:** prophylactic vaccines (traditional, mRNA, viral vector), therapeutic cancer vaccines (neoantigen, tumor-specific), DNA/RNA vaccines
    **11. ANTIBIOTICS/ANTI-INFECTIVES:** traditional antibiotics, novel mechanism agents, antimicrobial peptides (AMPs), phage therapy, host-directed therapies
    **12. OTHER/MISCELLANEOUS:** exosome-based therapies, nanoparticle delivery systems, radioimmunotherapy, photodynamic therapy, synthetic biology/gene circuits
  • **Synonym triggers** – also accept phrases such as "molecular matchmakers", "chemically-synthesised drug", "protein degraders", "bifunctional molecules", "targeted protein degradation", "drug-like molecules", "bioelectronics", "digital therapeutics"
  • **COMPANY-SPECIFIC ONLY** – Only extract modalities that the primary company itself is developing. 
    Exclude any modalities mentioned in context of subsidiaries, sister companies, portfolio companies, 
    or other separate entities.
  • If the company states it is *"modality agnostic"* or *"across all
    modalities"* → return ["modality agnostic"].  
  • Exclude pure delivery tech, diagnostics, devices, screening platforms,
    research tools (unless integrated with therapeutic modality).  
  • Flat list; null if nothing qualifies.

"disease_focus"
  • Capture **every disease area / indication** the company targets through therapeutic modalities or aims to treat / target/ adress. Keep wording as it appears (title-case where appropriate). Use the following Disease Categorization Reference as a guideline for what to look for during extraction.         
    Keep both parent and child terms if both appear.
  • **DISEASE CATEGORIZATION REFERENCE** - Use these medical categories as guidance for extraction:
    **1. INFECTIOUS DISEASES:** Viral infections (HIV/AIDS, Hepatitis B/C, Influenza, COVID-19, RSV, CMV, Dengue, Zika), Bacterial infections (Tuberculosis, MRSA, C. difficile, pneumonia, UTIs), Fungal infections (Candidiasis, Aspergillosis), Parasitic infections (Malaria, Leishmaniasis, Toxoplasmosis), Antimicrobial resistance (AMR), Vaccines (prophylactic and therapeutic)
    **2. CANCER/ONCOLOGY:** Solid tumors (Lung, Breast, Colorectal, Prostate, Pancreatic, Liver, Kidney, Ovarian, Head & Neck, Melanoma, Bladder), Hematologic cancers (Leukemias, Lymphomas, Multiple Myeloma), Rare/Pediatric cancers (Ewing Sarcoma, Retinoblastoma, Medulloblastoma), Cancer subspecialties (Immuno-oncology, Precision oncology, Tumor-agnostic indications)
    **3. CARDIOVASCULAR DISEASES:** Coronary artery disease, Heart failure, Hypertension, Arrhythmias (atrial fibrillation), Peripheral artery disease, Atherosclerosis, Pulmonary hypertension, Congenital heart defects, Stroke & embolic diseases
    **4. NEUROLOGICAL DISEASES:** Neurodegenerative diseases (Alzheimer's, Parkinson's, ALS, Huntington's), Demyelinating diseases (Multiple sclerosis), Epilepsy & seizure disorders, Neuromuscular disorders (Duchenne muscular dystrophy, SMA), Peripheral neuropathies, Traumatic brain injury (TBI), Spinal cord injury, Rare CNS disorders (Ataxias, Batten disease)
    **5. GENETIC & RARE DISEASES:** Monogenic disorders (Cystic fibrosis, sickle cell disease, Fabry disease, Gaucher disease), Chromosomal abnormalities (Down syndrome, Fragile X), Ultrarare diseases (FOP, NGLY1 deficiency), Undiagnosed genetic conditions
    **6. AUTOIMMUNE & INFLAMMATORY DISEASES:** Rheumatoid arthritis, Systemic lupus erythematosus (SLE), Psoriasis/Psoriatic arthritis, Inflammatory bowel diseases (Crohn's, ulcerative colitis), Type 1 diabetes, Celiac disease, Sjögren's syndrome, Vasculitis, Auto-inflammatory syndromes
    **7. RESPIRATORY DISEASES:** Asthma, Chronic obstructive pulmonary disease (COPD), Pulmonary fibrosis/interstitial lung disease (ILD), Acute respiratory distress syndrome (ARDS), Respiratory infections
    **8. GASTROINTESTINAL DISEASES:** Inflammatory bowel disease (IBD), Irritable bowel syndrome (IBS), GERD (acid reflux), GI cancers, Liver diseases (NAFLD/NASH, hepatitis, cirrhosis), Pancreatitis
    **9. HEMATOLOGICAL DISORDERS:** Anemias (iron-deficiency, aplastic, sickle cell), Thalassemias, Hemophilia, Clotting & platelet disorders, Myeloproliferative neoplasms (MPNs), Bone marrow failure syndromes
    **10. MUSCULOSKELETAL DISEASES:** Osteoarthritis, Osteoporosis, Muscular dystrophies, Tendon/ligament disorders, Joint degeneration, Sarcopenia
    **11. DERMATOLOGICAL DISEASES:** Psoriasis, Atopic dermatitis/eczema, Vitiligo, Acne, Rosacea, Alopecia areata, Skin infections, Skin cancers (melanoma, BCC, SCC)
    **12. PSYCHIATRIC & BEHAVIORAL DISORDERS:** Major depressive disorder (MDD), Bipolar disorder, Anxiety disorders, Schizophrenia, PTSD, ADHD, Substance use disorders, Autism spectrum disorders (ASD)
    **13. OPHTHALMIC DISEASES:** Age-related macular degeneration (AMD), Diabetic retinopathy, Glaucoma, Retinitis pigmentosa, Dry eye disease, Inherited retinal disorders, Uveitis
    **14. ENDOCRINE & METABOLIC DISEASES:** Type 1 & 2 diabetes mellitus, Obesity, Hyperlipidemia, Thyroid disorders, Adrenal disorders, Inborn errors of metabolism, Growth hormone deficiency
    **15. REPRODUCTIVE & WOMEN'S HEALTH:** Polycystic ovary syndrome (PCOS), Endometriosis, Uterine fibroids, Infertility, Menstrual disorders, Menopause-related symptoms, Gynecological cancers
    **16. PEDIATRIC DISEASES:** Congenital disorders, Neonatal complications, Rare pediatric genetic/metabolic diseases, Pediatric cancers, Developmental disorders, Pediatric autoimmune & neurological conditions
    **17. OTHER/UNSPECIFIED DISEASES:** Any disease not fitting above categories, including emerging diseases, neglected tropical diseases, pandemic threats, etc.
    
  • **COMPANY-SPECIFIC ONLY** – Only extract diseases that the primary company itself is targeting. 
    Exclude any diseases mentioned in context of subsidiaries, sister companies, portfolio companies, 
    or other separate entities.
  • If the company states it is *"disease agnostic"* → return
    ["indication agnostic"].  
  • Ignore diseases mentioned only when describing *other* companies,
    market commentary or job posts.  

  • Flat list; null if nothing qualifies.

"geography"
  • **LOCATIONS ONLY** – Extract ONLY actual geographic locations (cities, states/provinces, countries, addresses). 
    DO NOT extract company names, building names, or business entity names.
  • **Prioritize headquarters location** when multiple locations are mentioned.
  • Extract any country, region, city, or address that indicates where the company is based, operates, or has a presence.
  • Include mailing or contact addresses, even if there is no explicit verb (such as "headquartered in" or "based in").
  • If a full address is present (e.g., street, city, state, country), include it **in normalized format: "City, State, Country"**.
  • If the page or section is titled "Contact", "Offices", "Locations", or similar, always extract any address or location information found there.
  • Also extract locations mentioned in the context of company operations, headquarters, offices, or facilities.
  • **Separate headquarters from subsidiary/office locations** when both are clearly mentioned.
  • **EXCLUDE**: Company names, building names, facility names, or any non-geographic identifiers.
  • **INCLUDE ONLY**: City names, state/province names, country names, street addresses.
  • Flat list; null if nothing qualifies.

══════════════════════ EXTRACTION QUALITY ═══════════════════════
• **Specificity over Generality** – Prefer specific terms over broad categories when both are present
• **Company Voice** – Preserve the company's own terminology and phrasing  
• **Completeness** – Capture all relevant items, not just the first few mentioned
• **Accuracy** – Only extract what is explicitly stated, no assumptions
• **Primary Company Focus** – Only extract information about the main company being analyzed, not subsidiaries or partners

══════════════════════ CONSERVATIVE EXTRACTION PRINCIPLE ═══════════════════════
**CRITICAL: Better to miss information than create false positives**

• **When in doubt, EXCLUDE** – If you're uncertain whether something qualifies, don't include it
• **High confidence threshold** – Only extract information you are completely certain about
• **Strict interpretation** – Apply all filters (vertical, horizontal, partnership, subsidiary) rigorously
• **Quality over quantity** – A smaller, accurate result set is better than a larger, questionable one
• **Conservative default** – If a statement could be interpreted multiple ways, choose the most restrictive interpretation
• **Ambiguous = Exclude** – Any ambiguous statements should be omitted entirely

══════════════════════ VERIFICATION PROCESS ═══════════════════════
Before finalizing your JSON response, SYSTEMATICALLY verify each extraction:

**Step 1: DRUG_MODALITY Verification**
• Re-read each modality you extracted
• Confirm it's explicitly stated as something the company is developing/pursuing
• Verify it's therapeutic (not diagnostic, research tool, or platform)
• Check it's not describing a partner's or subsidiary's work

**Step 2: DISEASE_FOCUS Verification**  
• Re-read each disease/indication you extracted
• Confirm the company explicitly states they target/treat/address this condition
• Verify it's not market commentary or describing other companies
• Check it's not a partner's indication

**Step 3: GEOGRAPHY Verification**
• Re-read each location you extracted
• Confirm it's an actual geographic location (city/state/country)
• Verify it's not a company name, building name, or business entity
• Check it relates to the company's operations/headquarters

**Step 4: FINAL QUALITY CHECK**
• Does each field contain only what's explicitly stated?
• Are you following the company's own terminology?
• Have you excluded all non-therapeutic, partnership, and subsidiary information?
• Is your JSON valid with no markdown fences?

TEXT EXCERPTS:
\"\"\"{context}\"\"\"
""".strip()

def normalize_url(u: str) -> str:
    p = urlparse(u)
    return p._replace(path=p.path.rstrip("/").lower(),
                      fragment="").geturl()

def is_same_domain(base: str, test: str) -> bool:
    return urlparse(base).netloc == urlparse(test).netloc

def is_filetype(url: str) -> bool:
    return url.lower().split("?", 1)[0].endswith(SKIP_EXT)

def is_banned(url: str) -> bool:
    path = urlparse(url).path.lower()
    scheme = urlparse(url).scheme
    if scheme in ("mailto", "tel", "javascript"):
        return True
    return any(bad in path for bad in BANNED_ANCHOR_KEYWORDS)

# Diffbot API endpoints
CRAWL_API = "https://api.diffbot.com/v3/crawl"
ARTICLE_API = "https://api.diffbot.com/v3/article"


USER_AGENTS = [
   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
]

class TimeoutException(Exception):
   pass


def setup_signal_handlers():
   """Setup signal handlers for graceful termination"""
   def signal_handler(signum, frame):
       print(f"\n⚠️ Enrichment received signal {signum}. Shutting down...")
       print("💾 Saving partial results...")
       sys.exit(1)
  
   signal.signal(signal.SIGTERM, signal_handler)
   signal.signal(signal.SIGINT, signal_handler)

def timeout_handler(func, args=(), kwargs={}, timeout_duration=60, default=None):
   """Execute function with timeout using threading"""
   result = [default]
   exception = [None]
  
   def target():
       try:
           result[0] = func(*args, **kwargs)
       except Exception as e:
           exception[0] = e
  
   thread = threading.Thread(target=target)
   thread.daemon = True
   thread.start()
   thread.join(timeout_duration)
  
   if thread.is_alive():
       return default, TimeoutException(f"Function timed out after {timeout_duration}s")
  
   if exception[0]:
       return default, exception[0]
  
   return result[0], None

def get_random_headers():
   return {
       "User-Agent": random.choice(USER_AGENTS),
       "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
       "Accept-Language": "en-US,en;q=0.9",
       "Connection": "keep-alive",
       "Upgrade-Insecure-Requests": "1"
   }

def is_therapeutic_site(website_url: str) -> bool:
    
    text = _extract_visible_text_from_url(website_url, scrolls=4)[:8_000]  # keep prompt short

    prompt = f"""
Answer with **yes** or **no** (lower-case, no punctuation).

Say **yes** if the text shows that the company itself is discovering,
engineering, developing, testing, or commercialising **therapeutic
products** intended to treat disease in humans.

Accept ALL of the following synonyms as evidence:
  • drug, medicine, therapy, treatment, clinical candidate, asset, molecule
  • phrases such as “Phase 1/2/3”, “IND-enabling”, “pipeline”, “program”

Positive cues – **any ONE is sufficient**:
  • mention of a pre-clinical or clinical pipeline / program / asset
  • clinical-trial phases (IND-enabling, Phase 1/2/3, pivotal study, NDA, BLA)
  • modality keywords: small-molecule, biologic, antibody(-drug conjugate),
    protein, peptide, RNA therapy (mRNA, siRNA, ASO, saRNA), gene therapy,
    cell therapy, viral vector, vaccine, live biotherapeutic, microbiome
    therapy, optogenetic medicine, PROTAC, radioligand, degrader, etc.
  • explicit goal to *treat*, *cure*, *restore*, or *prevent* a named disease

Say **no** if the company is **only**:
  • diagnostics / devices / digital-health / CRO / CDMO / research tools
  • data / AI platforms without in-house therapeutics
  • insurers, consultancies, accelerators, investors, news outlets, foundations
  • describing partners’ drugs but not its own

If evidence is genuinely ambiguous, default to **no**.

TEXT:
\"\"\"{text}\"\"\"
""".strip()

    reply = client.chat.completions.create(
        model="gpt-4.1-2025-04-14",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=1,
    ).choices[0].message.content.strip().lower()

    return reply.startswith("y")

def smart_page_discovery(website_url: str, company_name: str):
    """
    Visit the homepage and all unique, same-domain anchors (depth 1, no recursion).
    Return a list[dict] for homepage + all direct links.
    """
    print(f"    [DISCOVERY] Depth-1 crawl for {website_url}")

    browser = _launch_browser(headless=True)
    discovered, visited = [], set()

    try:
        # Visit homepage first
        browser.get(website_url)
        WebDriverWait(browser, 12).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        visited.add(normalize_url(website_url))
        title = browser.title or "Untitled"
        score = score_page_value(website_url, title)
        discovered.append({
            "url": website_url,
            "title": title,
            "score": score,
            "category": categorize_page(website_url, title),
            "discovery_method": "selenium_depth1",
            "depth": 0,
            "text_preview": _extract_visible_text(browser, 180)
        })

        # Gather all same-domain anchors (depth 1)
        soup = BeautifulSoup(browser.page_source, "html.parser")
        anchors = soup.find_all("a", href=True)
        base = website_url
        queued = set()
        links = []

        for a in anchors:
            href = a["href"]
            full = urljoin(base, href)
            norm = normalize_url(full)
            if (norm in visited or 
                norm in queued or
                not full.startswith("http") or
                is_banned(full) or
                is_filetype(full) or
                not is_same_domain(base, full)):
                continue
            queued.add(norm)
            links.append(full)

        # Limit to N pages (to prevent long processing)
        N = MAX_PAGES_TO_ANALYZE - 1  # -1 because homepage already added
        links = links[:N]

        # Visit each anchor (depth 1 only)
        for link in links:
            try:
                browser.get(link)
                WebDriverWait(browser, 12).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                visited.add(normalize_url(link))
                title = browser.title or "Untitled"
                score = score_page_value(link, title)
                discovered.append({
                    "url": link,
                    "title": title,
                    "score": score,
                    "category": categorize_page(link, title),
                    "discovery_method": "selenium_depth1",
                    "depth": 1,
                    "text_preview": _extract_visible_text(browser, 180)
                })
            except Exception as e:
                print(f"      [skip] {link} - {e}")
                continue
    finally:
        try:
            browser.quit()
        except Exception:
            pass

    discovered.sort(key=lambda x: x["score"], reverse=True)
    top_pages = discovered[:MAX_PAGES_TO_ANALYZE]
    print(f"      Discovery finished → {len(top_pages)} pages kept")
    return top_pages, "selenium_depth1"

def score_page_value(url, link_text, content_preview=""):
   """Score the potential value of a page for biotech intelligence"""
   score = 0
   url_lower = url.lower()
   text_lower = link_text.lower()
  
   # HIGH VALUE PATTERNS
   high_value_patterns = {
       'pipeline': 40, 'technology': 35, 'platform': 35, 'science': 35,
       'research': 30, 'therapeutics': 40, 'products': 35, 'clinical': 35,
       'about': 30, 'contact': 25
   }
  
   # MEDIUM VALUE PATTERNS 
   medium_value_patterns = {
       'company': 20, 'team': 15, 'investors': 20,
       'partnerships': 25, 'approach': 25, 'locations': 30,
       'offices': 30
   }
  
   # BIOTECH TERMS (bonus points)
   biotech_terms = {
       'preclinical': 10, 'phase': 15, 'trial': 15, 'fda': 10,
       'regulatory': 10, 'target': 10, 'compound': 10
   }
  
   # Score URL and text
   for pattern, points in high_value_patterns.items():
       if pattern in url_lower or pattern in text_lower:
           score += points
           break
  
   if score == 0:
       for pattern, points in medium_value_patterns.items():
           if pattern in url_lower or pattern in text_lower:
               score += points
               break
  
   # Bonus points
   for term, points in biotech_terms.items():
       if term in url_lower or term in text_lower:
           score += points
  
   return max(0, min(score, 100))

def categorize_page(url, link_text):
   """Categorize a page based on URL and context"""
   url_lower = url.lower()
   text_lower = link_text.lower()
  
   categories = {
       'pipeline': ['pipeline', 'therapeutics', 'products'],
       'technology': ['technology', 'platform', 'science', 'approach'],
       'clinical': ['clinical', 'trial', 'phase', 'regulatory'],
       'company': ['about', 'company', 'team'],
       'business': ['investors', 'partnerships'],
       'geography': ['contact', 'locations', 'offices', 'address']
   }
  
   for category, keywords in categories.items():
       for keyword in keywords:
           if keyword in url_lower or keyword in text_lower:
               return category
  
   return 'other'

def llm_dedup_prompt(field, values):
    """Generate LLM prompt for per-startup field deduplication"""
    if field == "drug_modality":
        return (
            "You are deduplicating values extracted from a biotech website for the "
            "field 'drug_modality'.\n"
            "Merge semantically similar terms or exact duplicates. Think about each decision before you merge modalities and understand each modality's meaning.\n\n"
            "If a modality is a more specific version of another, keep the more specific one. Example: biologic and monoclonal antibody -> keep monoclonal antibody.\n"
            "If terms are semantically similar but one is more specific, keep the specific one. Example: hormone therapies and hormone replacement therapy -> keep hormone replacement therapy.\n"
            "A *therapeutic drug modality* directly treats or cures disease in humans. "
            "Examples: gene therapy, cell therapy, small molecule, biologic, antibody, "
            "RNA-based therapy, cancer vaccine, peptide, microbiome, targeted therapies, "
            "protein therapy.\n"
            "DO NOT include diagnostics, digital health, devices, research tools, etc.\n"
            "Merge identical modalities; keep distinct ones separate. "
            "If an umbrella like 'cell and gene therapy' appears *and* both components are "
            "present, drop the umbrella.\n\n"
            "Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n"
            f"INPUT: {json.dumps(values, ensure_ascii=False)}"
        )
    
    elif field == "disease_focus":
        return (
            "You are deduplicating values for the field 'disease_focus'.\n\n"
            "RULES:\n"
            "1. If two or more items differ *only* by qualifiers "
            "{for example: emerging, neglected, high-burden, pandemic, and more (use your judgement)}, merge them into a single entry "
            "and concatenate qualifiers with '/'. Example: "
            "['emerging infectious diseases', 'high-burden infectious diseases'] → "
            "'emerging/high-burden infectious diseases'.\n"
            "2. For umbrellas of the form 'X and Y health': if the umbrella and either "
            "'X health' or 'Y health' exist, KEEP ONLY the umbrella (e.g. keep "
            "'maternal and child health', drop 'child health'). Same for "
            "'reproductive and sexual health' vs 'sexual health'.\n"
            "3. Merge semantically similar diseases (ex: cancer, oncology, and oncological diseases). Think about each decision before you merge multiple disease entries together and understand each disease's meaning. If a disease is a more specific version of another, keep the more specific one. Example: Renal cell carcinoma and cancer -> keep renal cell carcinoma.\n"
            "4. Do NOT merge otherwise distinct diseases (HIV vs tuberculosis).\n\n"
            "Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n"
            f"INPUT: {json.dumps(values, ensure_ascii=False)}"
        )
        
    elif field == "geography":
        return (
            "You are deduplicating geographic locations for a single biotech startup.\n\n"
            "**CRITICAL: Return ONLY a valid JSON array. No explanations, no markdown, no extra text.**\n\n"
            "CITY-LEVEL MERGING RULES:\n"
            "1. **MERGE ALL LOCATIONS IN SAME CITY**: Consolidate everything to city-level\n"
            "   - Street addresses in same city → Single city entry\n"
            "   - Multiple mentions of same city → Single city entry\n"
            "   - Different formats of same city → Single normalized city entry\n\n"
            "2. **HIERARCHICAL DEDUPLICATION**: Remove broader locations when more specific locations exist\n"
            "   - If both 'London, UK' and 'UK' exist → Keep only 'London, UK' (city is more specific)\n"
            "   - If both 'Menlo Park, CA, US' and 'US' exist → Keep only 'Menlo Park, CA, US' (city is more specific)\n"
            "   - If both 'Boston, MA, US' and 'Menlo Park, CA, US' exist → Keep both (different cities)\n"
            "   - If both 'California, US' and 'US' exist → Keep only 'California, US' (state is more specific)\n\n"
            "3. **NORMALIZE TO STANDARD FORMAT**:\n"
            "   - Cities: 'City, State, Country' (e.g., 'Boston, MA, US')\n"
            "   - States only: 'State, Country' (e.g., 'California, US')\n"
            "   - Countries only: 'Country' (e.g., 'Japan')\n\n"
            "4. **STANDARD ABBREVIATIONS**:\n"
            "   - 'US' (not 'USA', 'United States', 'U.S.')\n"
            "   - 'UK' (not 'United Kingdom', 'U.K.')\n"
            "   - State abbreviations: 'MA', 'CA', 'NY', 'TX', 'PA', etc.\n\n"
            "5. **EXAMPLES**:\n"
            "   - ['123 Main St, Boston', '456 Oak Ave, Boston', 'Boston, MA'] → ['Boston, MA, US']\n"
            "   - ['Cambridge, Massachusetts', 'Cambridge, MA', 'Cambridge'] → ['Cambridge, MA, US']\n"
            "   - ['London, UK', 'Boston, MA, US', 'UK', 'US'] → ['London, UK', 'Boston, MA, US'] (remove broader UK and US)\n"
            "   - ['Menlo Park, CA, US', 'US'] → ['Menlo Park, CA, US'] (remove broader US)\n"
            "   - ['Switzerland', 'Zurich, Switzerland'] → ['Switzerland', 'Zurich, Switzerland'] (keep both as Zurich doesn't override Switzerland presence)\n\n"
            "6. **CLEAN OUTPUT**:\n"
            "   - Remove duplicates\n"
            "   - No street addresses in final output\n"
            "   - No building names or postal codes\n"
            "   - Only geographic locations (cities, states, countries)\n\n"
            "**INPUT TO PROCESS:**\n"
            f"{json.dumps(values, ensure_ascii=False)}\n\n"
            "**REQUIRED OUTPUT FORMAT: Valid JSON array only, like ['City, State, Country']**"
        )
    
    return f"Unknown field: {field}"

def deduplicate_startup_field_with_llm(field_name, values):
    """
    Deduplicate and normalize a single startup's field using LLM.
    Always processes values for normalization - even single values need canonical form.
    """
    if not values or not isinstance(values, list):
        return values
    
    # Remove empty values and strip whitespace
    clean_values = [v.strip() for v in values if v and v.strip()]
    if not clean_values:
        return None
    
    # Always process for normalization (even single values need canonical forms)
    try:
        prompt = llm_dedup_prompt(field_name, clean_values)
        response = client.chat.completions.create(
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0,
        ).choices[0].message.content.strip()
        
        print(f"   [DEBUG] LLM deduplication for {field_name}: {clean_values} -> processing...")
        
        # Extract JSON from response - try multiple approaches
        data = None
        
        # First try: look for JSON array
        match = re.search(r"\[.*\]", response, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        # Second try: parse entire response as JSON
        if data is None:
            try:
                data = json.loads(response)
            except json.JSONDecodeError:
                pass
        
        # Third try: look for quoted strings and construct array
        if data is None:
            quoted_matches = re.findall(r'"([^"]+)"', response)
            if quoted_matches:
                data = quoted_matches
        
        if isinstance(data, list) and data:
            # Filter out empty strings and None values
            normalized_data = [item for item in data if item and str(item).strip()]
            if normalized_data:
                print(f"   [LLM dedup success] {field_name}: {clean_values} -> {normalized_data}")
                return normalized_data
            else:
                print(f"   [LLM dedup fallback] {field_name}: No valid items after normalization")
                return clean_values
        else:
            print(f"   [LLM dedup fallback] {field_name}: Could not parse LLM response - {response[:100]}...")
            return clean_values
            
    except Exception as e:
        print(f"   [LLM dedup error] {field_name}: {e}")
        return clean_values

def extract_startup_fields(
    company_name: str,
    crawled_pages: list[dict],
    max_pages: int = 4,
    model: str = "gpt-4.1-2025-04-14",
):
    if not crawled_pages:
        return {}
    # 1. Concatenate the best N pages into a single context string
    chunks, link_bundle = [], []
    for i, page in enumerate(crawled_pages[:max_pages], start=1):
        url   = page["url"]
        title = page.get("title", f"PAGE {i}")
        text  = _extract_visible_text_from_url(url)[:3_500]  # cap tokens
        chunks.append(f"\n=== PAGE {i}: {title} ===\nURL: {url}\n{text}")
        link_bundle.append({"url": url, "title": title})

    context = "\n".join(chunks)[:10_000]

    # 2. Fill the prompt template
    prompt = STARTUP_3FIELDS_PROMPT.format(context=context)

    # 3. LLM call under a timeout guard
    def llm_call():
        return client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=500,
        ).choices[0].message.content.strip()

    raw_reply, err = timeout_handler(llm_call, timeout_duration=AI_TIMEOUT)
    if err:
        print(f"[AI-timeout] {err}")
        return {}

    # 4. Parse JSON – strip fences just in case
    reply = raw_reply
    if reply.startswith("```"):
        parts = reply.split("```")
        if len(parts) >= 2:
            reply = parts[1].strip()

    try:
        data = json.loads(reply)
    except Exception as e:
        print(f"[JSON-parse error] {e}")
        return {}

    # 5. Light validation / null-fill (no deduplication yet)
    raw_modality = data.get("drug_modality")
    raw_disease = data.get("disease_focus")
    raw_geography = data.get("geography")
    
    out = {
        "drug_modality": raw_modality,
        "disease_focus": raw_disease,
        "geography":     raw_geography,
        "_meta": {
            "source_links": link_bundle,
            "model": model,
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    # Fallback: if geography is missing/empty, use Serper API
    if not out["geography"] or (isinstance(out["geography"], list) and not out["geography"]):
        serper_api_key = os.getenv("SERPER_API_KEY")
        if serper_api_key:
            locations = serper_geography_fallback(company_name, serper_api_key)
            if locations:
                out["geography"] = locations
                out["_meta"]["geography_source"] = "serper_fallback"
                print(f"[Serper fallback] {company_name}: {locations}")
            else:
                out["_meta"]["geography_source"] = "none_found"
        else:
            print("[Serper fallback] SERPER_API_KEY not set; skipping fallback.")
            out["_meta"]["geography_source"] = "no_api_key"
    else:
        out["_meta"]["geography_source"] = "llm_or_webpage"
    
    # 6. FINAL STEP: Apply LLM deduplication to all fields regardless of source
    print(f"   [LLM dedup] Applying final deduplication to all fields...")
    out["drug_modality"] = deduplicate_startup_field_with_llm("drug_modality", out["drug_modality"])
    out["disease_focus"] = deduplicate_startup_field_with_llm("disease_focus", out["disease_focus"])
    out["geography"] = deduplicate_startup_field_with_llm("geography", out["geography"])
    
    return out

# ────────────────────────────────────────────────────────────────
#  ONE-COMPANY ORCHESTRATION (therapeutic gate → crawl → GPT)
# ────────────────────────────────────────────────────────────────
def process_startup_with_hybrid_system(company_info, idx, total):
    """
    1) skip if site is not therapeutic
    2) run depth-1 Selenium crawl
    3) ask GPT-4o-mini for the two target fields
       → returns a trimmed dict or None on failure
    """
    company   = company_info["company_name"]
    url       = company_info["website_url"]
    source_vc = company_info.get("source_vc", "Unknown VC")

    print(f"\n[STARTUP {idx}/{total}] {company}")
    print(f"[URL] {url}")
    start_t = time.time()

    # 1. Therapeutic gate
    if not is_therapeutic_site(url):
        print("   ✖ not a therapeutic / drug-development company – skipped")
        return None

    # 2. Depth-1 crawl
    pages, _ = smart_page_discovery(url, company)
    if not pages:
        print("   ✖ no pages worth analysing")
        return None

    # 3. GPT extraction (returns at most 2 dicts)
    fields = extract_startup_fields(company, pages)
    if not fields:
        print("   ✖ GPT returned no usable fields")
        return None
    
    result = {
        "company_name": company,
        "website_url": url,
        **fields
    }
    return result


def main() -> None:
    """
    Minimal driver:
      • read a JSON list/obj from argv or prompt
      • iterate over companies in parallel
      • call process_startup_with_hybrid_system()
      • write the results to disk
    """
    import json, sys, pathlib, time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Preload undetected_chromedriver to avoid race condition in parallel threads
    try:
        dummy = uc.Chrome(headless=True)
        dummy.quit()
    except Exception:
        pass

    # ---------- 1. pick up input ----------
    if len(sys.argv) > 1:
        raw_input = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    else:
        raw_input = input("Paste companies JSON or path: ").strip()
        if pathlib.Path(raw_input).exists():
            raw_input = pathlib.Path(raw_input).read_text(encoding="utf-8")

    data = json.loads(raw_input)
    companies = (
        data.get("websites_found")
        or data.get("companies")
        or (data if isinstance(data, list) else [data])
    )
    if not companies:
        print("❌ No companies found – aborting"); return

    print(f"📊 Will process {len(companies)} companies")

    # ---------- 2. parallel loop ----------
    results, start = [], time.time()
    max_workers = min(6, len(companies))  # Increased from 4 to 6 based on system capabilities
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx, c in enumerate(companies, 1):
            if isinstance(c, str):
                c = {"company_name": c, "website_url": c}
            if not c.get("website_url"):
                print(f"   ⤷ skipping {c.get('company_name','?')} – no URL"); continue
            futures.append(executor.submit(process_startup_with_hybrid_system, c, idx, len(companies)))
        for future in as_completed(futures):
            out = future.result()
            if out:
                results.append(out)

    # ---------- 2.5. therapeutic_investor_portfolio flag ----------
    # Count therapeutic companies (those that passed is_therapeutic_site check)
    therapeutic_count = len(results)
    min_therapeutic_threshold = 3  # Minimum companies required to establish significant therapeutic presence
    
    therapeutic_investor_portfolio = therapeutic_count >= min_therapeutic_threshold
    
    print(f"📊 Therapeutic Analysis:")
    print(f"   Companies processed: {len(companies)}")
    print(f"   Therapeutic companies found: {therapeutic_count}")
    print(f"   Minimum threshold: {min_therapeutic_threshold}")
    print(f"   Therapeutic investor portfolio: {therapeutic_investor_portfolio}")
    
    if therapeutic_count > 0 and therapeutic_count < min_therapeutic_threshold:
        print(f"   ⚠️  Found {therapeutic_count} therapeutic companies, but need {min_therapeutic_threshold} for significant presence")

    # ---------- 3. save ----------
    vc_name_fs = sys.argv[2]
    output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    output_dir = Path(output_dir)  # Ensure output_dir is a Path object
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outfile = output_dir / f"startup_extract_{ts}.json"
    # Write both results and therapeutic_investor_portfolio as top-level keys
    output_data = {
        "results": results,
        "therapeutic_investor_portfolio": therapeutic_investor_portfolio,
        "therapeutic_analysis": {
            "total_companies_processed": len(companies),
            "therapeutic_companies_found": therapeutic_count,
            "minimum_threshold": min_therapeutic_threshold,
            "meets_threshold": therapeutic_investor_portfolio
        }
    }
    outfile.write_text(json.dumps(output_data, indent=2), encoding="utf-8")

    # Print the flag for user visibility
    print(f"therapeutic_investor_portfolio: {therapeutic_investor_portfolio}")

    # ---------- 4. summary ----------
    print("\n🎉 Done.",
          f"{len(results)}/{len(companies)} succeeded –",
          f"saved → {outfile}",
          f"⏱️ {time.time()-start:.1f}s total", sep="\n")
    print(f"OUTPUT_FILE: {outfile}")

def run_enrichment(input_path, vc_name_fs, output_dir=None):
    """
    Run the enrichment pipeline natively.
    Args:
        input_path (str): Path to input JSON (from website discovery)
        vc_name_fs (str): Filesystem-safe VC name
        output_dir (str, optional): Output directory. Defaults to output/runs/<vc_name_fs>/
    Returns:
        str: Path to output JSON file
    """
    import pathlib
    import json
    from datetime import datetime
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)
    companies = (
        data.get("websites_found")
        or data.get("companies")
        or (data if isinstance(data, list) else [data])
    )
    if not companies:
        raise ValueError("No companies found for enrichment")
    # Preload undetected_chromedriver to avoid race condition in parallel threads
    try:
        dummy = uc.Chrome(headless=True)
        dummy.quit()
    except Exception:
        pass
    if not output_dir:
        output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    output_dir = pathlib.Path(output_dir)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outfile = output_dir / f"startup_extract_{ts}.json"
    # Parallel enrichment
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = []
    max_workers = min(6, len(companies))  # Increased from 4 to 6 based on system capabilities
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx, c in enumerate(companies, 1):
            if isinstance(c, str):
                c = {"company_name": c, "website_url": c}
            if not c.get("website_url"):
                continue
            futures.append(executor.submit(process_startup_with_hybrid_system, c, idx, len(companies)))
        for future in as_completed(futures):
            out = future.result()
            if out:
                results.append(out)
    # Compute therapeutic_investor_portfolio flag
    # Count therapeutic companies (those that passed is_therapeutic_site check)
    therapeutic_count = len(results)
    min_therapeutic_threshold = 3  # Minimum companies required to establish significant therapeutic presence
    
    therapeutic_investor_portfolio = therapeutic_count >= min_therapeutic_threshold
    
    print(f"📊 Therapeutic Analysis:")
    print(f"   Companies processed: {len(companies)}")
    print(f"   Therapeutic companies found: {therapeutic_count}")
    print(f"   Minimum threshold: {min_therapeutic_threshold}")
    print(f"   Therapeutic investor portfolio: {therapeutic_investor_portfolio}")
    
    if therapeutic_count > 0 and therapeutic_count < min_therapeutic_threshold:
        print(f"   ⚠️  Found {therapeutic_count} therapeutic companies, but need {min_therapeutic_threshold} for significant presence")
    # Write output as dict
    output_data = {
        "therapeutic_investor_portfolio": therapeutic_investor_portfolio,
        "therapeutic_analysis": {
            "total_companies_processed": len(companies),
            "therapeutic_companies_found": therapeutic_count,
            "minimum_threshold": min_therapeutic_threshold,
            "meets_threshold": therapeutic_investor_portfolio
        },
        "companies": results
    }
    outfile.write_text(json.dumps(output_data, indent=2), encoding="utf-8")
    print(f"[OK] enrichment complete: {outfile}")
    return str(outfile)

def serper_geography_fallback(company_name: str, serper_api_key: str) -> list:
    """
    Query Serper API for company location if geography is missing.
    Uses LLM to extract only the headquarters location.
    Returns a list with a single location string (or empty).
    """
    import requests
    query = f"What is the headquarters location of {company_name}?"
    url = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": serper_api_key,
        "Content-Type": "application/json"
    }
    data = {"q": query, "gl": "us", "hl": "en"}
    try:
        resp = requests.post(url, headers=headers, json=data, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # Collect all relevant text from organic results
        texts = []
        for result in data.get("organic", []):
            if "snippet" in result:
                texts.append(result["snippet"])
            if "title" in result:
                texts.append(result["title"])
        # Optionally add answerBox/knowledgeGraph fields if present
        if "answerBox" in data and "answer" in data["answerBox"]:
            texts.append(data["answerBox"]["answer"])
        if "knowledgeGraph" in data and "description" in data["knowledgeGraph"]:
            texts.append(data["knowledgeGraph"]["description"])
        # Compose a highly specific LLM prompt
        prompt = (
            f"You are a world-class data extraction agent for biotech and startup intelligence.\n"
            f"Your task is to extract ONLY the headquarters location for the company named '{company_name}'.\n"
            "You are given noisy, unstructured search results from Google.\n\n"
            "Instructions:\n"
            "- Only return a location if it STRICTLY and CLEARLY the headquarters location (city, state/province, country, or full address) of the company queried.\n"
            "- Ignore all unrelated information, including company descriptions, news, funding, competitors, year founded, executive names, product names, and any other non-location data.\n"
            "- If multiple locations are mentioned, choose the one most likely to be the headquarters or main office.\n"
            "- If a full address is present, prefer it. Otherwise, return the most specific city/state/country.\n"
            "- Do NOT return any company name, website, or metadata.\n"
            "- Do NOT return any list, explanation, or extra words.\n"
            "- Return only the location string, with no extra punctuation or formatting.\n"
            "- If no location is found, return an empty string.\n\n"
            f"Company: {company_name}\n\nSearch Results:\n" + "\n".join(texts)
        )
        # LLM call
        reply = client.chat.completions.create(
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=50,
        ).choices[0].message.content.strip()
        # Return as a list (for compatibility)
        if reply:
            return [reply]
        else:
            return []
    except Exception as e:
        print(f"[Serper fallback error] {company_name}: {e}")
        return []

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: comprehensive_data_enrichment.py <input_path> <vc_name_fs>")
        sys.exit(1)
    input_path = sys.argv[1]
    vc_name_fs = sys.argv[2]
    output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    output_dir = Path(output_dir)  # Ensure output_dir is a Path object
    main()