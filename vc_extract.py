# vc_extract.py  ────────────────────────────────────────────────────────────
import os, json, re, asyncio
from dotenv import load_dotenv
from openai import OpenAI
from bs4 import BeautifulSoup
import hashlib
from playwright.async_api import async_playwright

import hashlib



# ─── Playwright helpers ────────────────────────────────────────────────────
READ_MORE_BUTTONS = [
    "read more", "show more", "expand", "see more", "view more",
    "learn more", "show all", "see full", "continue reading", "expand all"
]
BUTTON_SEL = ["button", "a", "div", "span"]

def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", txt or "").strip().lower()

async def auto_scroll_and_expand(page, scrolls: int = 6, delay: int = 800):
    """Gentle scroll + click any “read more” style expanders."""
    for _ in range(scrolls):
        await page.mouse.wheel(0, 1800)
        await page.wait_for_timeout(delay)

    async def is_expander(el):
        tag  = (await el.evaluate("(e)=>e.tagName")).lower()
        href = (await el.get_attribute("href") or "")
        role = (await el.get_attribute("role") or "").lower()
        return (tag == "button" or role == "button") and not href

    async def click(el):
        if not await is_expander(el):
            return False
        before = page.url
        await el.click(timeout=1_000)
        await page.wait_for_timeout(500)
        return page.url == before

    # text-based buttons
    for txt in READ_MORE_BUTTONS:
        while True:
            btn = await page.query_selector(f"text=/{txt}/i")
            if not btn or not await click(btn):
                break
    # brute-force fallback
    for sel in BUTTON_SEL:
        while True:
            clicked = False
            for el in await page.query_selector_all(sel):
                try:
                    if any(_norm(bt) in _norm(await el.inner_text())
                           for bt in READ_MORE_BUTTONS):
                        if await click(el):
                            clicked = True
                            break
                except Exception:
                    continue
            if not clicked:
                break

# ─── Visible-text extraction ──────────────────────────────────────────────
def extract_all_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
    return "\n".join(lines)

async def playwright_extract_text(url: str) -> str:
    async with async_playwright() as p:
        headless = False
        browser  = await p.chromium.launch(headless=headless)
        context  = await browser.new_context()
        try:
            page = await context.new_page()
            page.set_default_timeout(90_000)
            await page.goto(url, wait_until="load", timeout=90_000)
            try:
                await auto_scroll_and_expand(page)
            except Exception as e:
                print(f"[WARN] expander step failed: {e}")
            try:
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            html = await page.content()
            return extract_all_visible_text(html) or ""
        finally:
            await context.close()
            await browser.close()

def extract_visible_text(url: str) -> str:
    return asyncio.run(playwright_extract_text(url))

# ─── LLM extraction setup ─────────────────────────────────────────────────
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ...existing code...

PROMPT = """
You are a precision data-extraction specialist for venture-capital intelligence.

Goal → Return a single JSON object with exactly **8** top-level keys:
  "drug_modality"                      : list
  "disease_focus"                      : list
  "geography"                          : list
  "investment_stage"                   : list
  "investment_amount"                  : string
  "requires_startup_revenue_generation": true/null
  "therapeutic_investor"               : true/false/null
  "equity_investor"                    : true/false/null

════════════════════════════════════════════════════════════════════
STRICT RULES  (read carefully!)

1. Extract only what the text states explicitly about the investment firm—never infer or guess.
2. If a field is completely missing, set it to null (or an empty list).

3. Skip press releases, news, portfolio companies, team bios, careers, contact-us blocks.
   • However, within those sections, if a sentence explicitly describes
     the firm's own investment mandate (e.g. "We invest in oncology and
     rare disease companies at Series A"), you MAY extract that
     statement. Everything else in the section is ignored.

4. Full-page bail-out – if the entire page is a financing/news article,
   output:
   {{ "drug_modality":null,
      "disease_focus":null,
      "geography":null,
      "investment_stage":null,
      "investment_amount":null,
      "requires_startup_revenue_generation": null,
      "therapeutic_investor": null,
      "equity_investor": null
   }}
   and STOP.

5. **STRICT** Output must be valid JSON, no code-fences, no markdown. All output must be in English. If any content is in another language, translate it to English in the output.

6. **VERTICAL FILTER (ENHANCED)**
   The extraction must **ignore any criterion that applies only to non-therapeutic verticals** (SaaS, fintech, prop-tech, diagnostics, med-device, digital-health, AI tools, infrastructure, etc.).
   • **When to EXCLUDE**  
     – The sentence (or clause) names a non-therapeutic vertical **and** ties the criterion to that vertical:  
         • "For our **SaaS fund** we invest at growth stage." ⟶ skip `investment_stage`.  
         • "Typical check size **for fintech deals** is $10–20 M." ⟶ skip `investment_amount`.  
         • "We target Series A in **health-tech and diagnostics**." ⟶ skip; not drug-development.
   • **When to INCLUDE**  
     a) The sentence explicitly refers to **therapeutics / life sciences/ biotech / drug development**:  
         • "We invest $5–30 M per company in **drug-development** startups."  
         • "Focus: **gene therapy** at Series A-B."  
     b) The criterion is stated **firm-wide with no vertical qualifier**:  
         • "We write checks of $3–8 M."  
         • "Our fund backs companies from seed through Series B."  
       ⟶ Treat as applicable to therapeutics and include.
   • **Context window**: Consider 2-3 surrounding sentences for vertical context
   • **Grey zones – default to EXCLUDE**  
     – Phrases like "healthcare", "health-tech", or "AI" **alone** are insufficient; only include if the same sentence (or the immediately following one) makes it clear these terms cover drug development or therapeutics.
   • **Fields affected**: `drug_modality`, `disease_focus`, `geography`, `investment_stage`, `investment_amount`, `requires_startup_revenue_generation`.
   • **Rule of thumb**: *If the model cannot confidently say "this applies to therapeutics or the whole firm," leave it out.*


──────── Field-specific guidance ────────
"drug_modality"
  • Extract ONLY therapeutic modalities that the VC firm explicitly invests in, using the following industry-standard categories as guidance triggers (regardless of phrasing):
    1. SMALL MOLECULES: inhibitors, activators, allosteric modulators, PROTACs, molecular glues, degraders
    2. GENETIC MEDICINES: gene therapy (AAV, lentivirus, non-viral), gene editing (CRISPR, base/prime editors, TALENs, ZFNs), mRNA, siRNA, miRNA, ASOs, saRNA, circRNA
    3. BIOLOGICS: monoclonal/bispecific antibodies, ADCs, fusion proteins, enzymes, Fc-modified antibodies
    4. PEPTIDES & PROTEINS: peptide drugs, protein replacement, hormones (insulin, EPO, etc.)
    5. CELL THERAPIES: CAR-T, TCR-T, CAR-NK, iPSC-derived, stem cell therapies
    6. IMMUNOTHERAPIES: checkpoint inhibitors, cytokines, tumor vaccines, immune agonists, engineered T cell/APC platforms
    7. MICROBIOME-BASED: LBPs, postbiotics, FMT
    8. NEUROMODULATORS: botulinum toxin, neuropeptides, bioelectronic medicine, neural pathway modulators
    9. ONCOLYTIC VIRUSES: engineered viruses for cancer
    10. VACCINES: prophylactic, therapeutic, mRNA, DNA, viral vector
    11. ANTI-INFECTIVES: antibiotics, antimicrobial peptides, phage therapy
    12. OTHER: exosome therapies, nanoparticles, radioimmunotherapy, photodynamic therapy, synthetic biology/gene circuits
  • Accept synonyms and related phrases (e.g., “protein degraders”, “targeted protein degradation”, “bifunctional molecules”, “bioelectronics”, “digital therapeutics”) if clearly tied to investment focus.
  • If the firm is “modality agnostic” or “invests across all modalities”, return ["modality agnostic"].
  • Exclude non-therapeutic buckets: diagnostics, devices, digital health, research tools, drug-delivery platforms, screening platforms, healthcare IT, contract services.
  • Only extract modalities that the firm itself invests in (not those of portfolio companies or partners).
  • Output a flat list of strings; null if nothing qualifies.

"disease_focus"
  • Extract EVERY disease area, indication, or sub-category the VC firm explicitly targets for investment, using the following medical categories as reference buckets:
    1. INFECTIOUS DISEASES: viral (HIV, Hepatitis, COVID-19, etc.), bacterial (TB, MRSA), fungal, parasitic, AMR, vaccines
    2. CANCER/ONCOLOGY: solid tumors (lung, breast, etc.), hematologic cancers, rare/pediatric cancers, immuno-oncology, precision oncology
    3. CARDIOVASCULAR: CAD, heart failure, hypertension, arrhythmias, stroke, congenital defects
    4. NEUROLOGICAL: neurodegeneration (Alzheimer’s, Parkinson’s, ALS), epilepsy, neuromuscular, TBI, rare CNS disorders
    5. GENETIC & RARE DISEASES: monogenic, chromosomal, ultrarare, undiagnosed
    6. AUTOIMMUNE & INFLAMMATORY: RA, lupus, psoriasis, IBD, type 1 diabetes, vasculitis
    7. RESPIRATORY: asthma, COPD, pulmonary fibrosis, ARDS
    8. GASTROINTESTINAL: IBD, IBS, GERD, liver diseases, pancreatitis
    9. HEMATOLOGICAL: anemias, thalassemias, hemophilia, MPNs
    10. MUSCULOSKELETAL: osteoarthritis, osteoporosis, muscular dystrophies
    11. DERMATOLOGICAL: psoriasis, eczema, vitiligo, skin cancers
    12. PSYCHIATRIC & BEHAVIORAL: depression, bipolar, anxiety, schizophrenia, autism
    13. OPHTHALMIC: AMD, diabetic retinopathy, glaucoma, inherited retinal disorders
    14. ENDOCRINE & METABOLIC: diabetes, obesity, thyroid, adrenal, inborn errors
    15. REPRODUCTIVE & WOMEN’S HEALTH: PCOS, endometriosis, infertility, gynecological cancers
    16. PEDIATRIC DISEASES: congenital, neonatal, rare pediatric, developmental
    17. OTHER/UNSPECIFIED: emerging, neglected, pandemic threats
  • If the firm is “disease agnostic” or “indication agnostic”, return ["indication agnostic"].
  • Ignore diseases mentioned only in portfolio news, case studies, team bios, or market commentary.
  • Only extract diseases the firm itself targets for therapeutic investment (not those of portfolio companies or partners).
  • No guessing; skip vague phrases like “addressing unmet needs” without specifics.
  • Output a flat list of strings; null if nothing qualifies.
  
"geography"
  • Extract a country/region **only when explicitly tied to investment activity** with clear investment verbs:
    "invests in", "backs companies in", "funds startups in", "sources deals from", "targets investments in"
  • **EXCLUDE office/HQ locations** unless the same sentence contains investment activity for that location
  • **EXCLUDE event/conference locations** unless tied to investment sourcing  
  • **Support hub rule**: If location X supports investments in location Y, extract only Y:
    – "Notre branche de Boston soutient les start-ups françaises" → return
      **"France"** (NOT "United States").  
  • **Clear investment connection required** - "we have a presence in Boston" ≠ "we invest in Boston companies"
  • Do NOT infer from impact statements ("improving health in LMICs") unless the
    sentence also contains an investment verb for those places.
  • Accept "global / worldwide / geography-agnostic" only if those exact words
    are explicitly linked to investment scope with an investment verb.
  • Return a flat list of geographies or null. **When in doubt, exclude.**

"investment_stage"
  • Extract **only investment stages explicitly stated as the firm's own investment criteria using the following detailed buckets for reference:**
  • **Recognized equity stages (with definitions/examples):**
    1. PRE-SEED: First external capital, often pre-product or pre-revenue (e.g., "we invest at pre-seed").
    2. SEED: Initial institutional round, early product/market fit (e.g., "seed-stage startups").
    3. SERIES A: First major VC round, scaling product and team (e.g., "Series A companies").
    4. SERIES B: Growth capital for scaling revenue and operations (e.g., "Series B and beyond").
    5. SERIES C (and later): Late-stage private rounds, expansion, pre-IPO (e.g., "Series C", "Series D").
    6. EARLY-STAGE: Umbrella for pre-seed, seed, and Series A (e.g., "early-stage biotechs").
    7. MID-STAGE / EXPANSION: Series B/C, scaling up, entering new markets (e.g., "expansion stage").
    8. LATE-STAGE: Series C+, pre-IPO, mature private companies (e.g., "late-stage investments").
    9. CLINICAL STAGE: Investment specifically in companies with drug candidates in clinical trials (e.g., "we invest in clinical-stage biotechs", "focus on companies in Phase I-III").
    10. PRE-IPO: Directly before public offering (e.g., "pre-IPO rounds").
    11. IPO PARTICIPATION: Investment at or during IPO (e.g., "participate in IPOs").
    12. PUBLIC EQUITY: Listed/public company investments (e.g., "public equity rounds").
    13. PRIVATE EQUITY: Buyouts, growth equity, non-venture private deals (e.g., "private equity transactions").
    14. ALL STAGES: If the firm states it invests "across all stages" or similar, return ["all stages"].
  • **Examples of VALID contexts:**
    – "we invest in Series A companies"
    – "our fund backs seed-stage startups"
    – "we participate in pre-IPO and IPO rounds"
    – "we invest across all stages"
  • **Examples of INVALID contexts:**
    – Portfolio company news, press releases, market commentary
    – Any mention of a stage not directly tied to the firm’s investment focus
  • **Debt exclusion:**
    – Ignore any pure debt financing stages/statements
    – If a statement mixes debt and equity, extract only the equity stage
  • **Phrase handling:**
    – If a range or umbrella phrase is used (e.g., "Series Seed–B", "Series A and onwards", "early-stage"), extract all referenced stages
    – Accept broad phrases such as "all stages" if the firm invests across all equity stages (return ["all stages"])
  • **Sector qualifier:**
    – If the stage is tied to a non-therapeutic vertical (e.g., "for SaaS we invest at growth stage"), exclude it
    – If the stage is firm-wide or tied to therapeutics, include it
  • **No guessing:**
    – If you are uncertain or no clear equity stage is found, return null
  • **Format:**
    – Return a flat list of all extracted equity stages (e.g., ["Series A", "Series B"])
    – If no valid stages, return null
    – DO NOT infer or guess from context

"investment_amount"
  • Extract ONLY **per-company check sizes** for therapeutic investments. Include surrounding context (ex: "up to 12m") to preserve original phrasing and investment range
  • **VALID**: "we invest $5-15M per company", "typical ticket size is $10M", "up to $25M per investment"
  • **INVALID**: Total fund size, cumulative deployed capital, sector allocation amounts, portfolio company raise announcements
  • **Sector filter**: If amount is tied to non-therapeutic vertical, exclude it
  • **Currency normalization**: Convert "million/mn" → "M", "billion/bn" → "B"
  • **Range format**: Preserve original phrasing ("$5-15M" not "$5M-$15M")
  • **IGNORE all of the following, even if the number is large or prominent:**
      – Total fund size (e.g. "we have $1 billion under management", "total committed capital is $1B", "we have $1B to deploy over 10 years").
      – Aggregate goals, e.g. "we aim to invest $500M in the sector", "we hope to fund 2–4 new antimicrobials with $1B".
      – Cumulative "invested to date" figures unless it is *clearly described* as "per company."
      – Any figure in a sentence about a portfolio company "raising/has raised/raised" money.
  • If there is *no explicit check size per company*, return null.
  • If uncertain, **leave blank (null)**—do NOT guess.
  • Return a string (single value or concatenated ranges); null if absent.

"requires_startup_revenue_generation"
  • Return **true** only if the text explicitly states a revenue requirement for startups:
    – Examples: "companies must have revenue", "requires $1M+ in revenue", "commercial traction required", "revenue-positive companies only", "post-revenue investments"
  • Return **null** for all other cases, including:
    – No mention of revenue
    – Phrases like "late-stage", "clinical-stage", "commercialization", or "marketed products" unless they directly mention revenue
    – Any implication or assumption based on stage or maturity
  • Do NOT infer, do NOT guess, and do NOT return true unless it is clearly stated in the investment criteria.
  • Always return: true or null (lowercase, no quotes).


"therapeutic_investor"
Extract whether this investor explicitly invests in therapeutic drug development.

**Return "true" only if the text explicitly mentions investment in:**
- Drug development, drug discovery, therapeutics, pharmaceutical medicines
- Small molecules, biologics, gene therapy, cell therapy, protein therapeutics
- Biopharma or biotech companies that develop drugs/therapeutics
- Pharmaceutical R&D or clinical-stage drug development

**Return "false" only for explicit exclusions:**
- "We do not invest in therapeutics/drug development"
- "Excludes pharmaceutical companies"
- Clear statements of non-therapeutic focus

**Return "null" for all other cases, including:**
- Generic terms without drug context: "biotech", "life sciences", "healthcare"
- Adjacent sectors: medical devices, diagnostics, digital health, healthcare IT
- Ambiguous biotech references without drug development specificity
- "Digital therapeutics" unless explicitly pharmaceutical/drug-related
- Any uncertainty or absence of clear therapeutic investment language

**Key principle:** Only return "true" when drug development investment is unmistakably stated. When in doubt, return "null".

**Output format:** true, false, or null (lowercase, no quotes)

"equity_investor"
  • Return **false** only if the text clearly states the firm is a debt-only lender and does not provide equity (e.g., "exclusively venture debt", "100% revenue-based financing; no equity rounds").
  • In all other situations—including when financing type is never mentioned, or when both equity and debt are mentioned—return **true** (the default assumption is equity investing).
    – Examples: "venture capital", "VC", "investor", "we invest in companies" → true
    – "we provide both equity and debt" → true
    – "we only provide loans" or "no equity investments" → false
  • Allowed outputs (lower-case): true or false.
  • If the text does not mention equity at all, return **true** by default, as most VC firms are equity investors.

  
──────── OUTPUT VALIDATION ────────
Before returning JSON, verify:
• All list fields contain only strings (no nested objects)
• Geographic entries are actual locations, not company/building names
• Investment amounts include currency symbols and magnitude indicators
• Stage entries use standard terminology
• Boolean fields are exactly true/false/null (lowercase)
════════════════════════════════════════════════════════════════════

TEXT TO ANALYSE (truncate beyond 18 k chars):

\"\"\"{text}\"\"\"
"""

# template for total failure
EMPTY = {
    "drug_modality": [],
    "disease_focus": [],
    "geography": [],
    "investment_stage": [],
    "investment_amount": "",
    "requires_startup_revenue_generation": None,
    "therapeutic_investor": None,
    "equity_investor": None,
}

# ─── Public entry-point ────────────────────────────────────────────────────
def extract_vc_info(url: str) -> dict:
    text = extract_visible_text(url)
    if not text.strip():
        return EMPTY | {"gpt_input_text": "", "gpt_output": ""}

    def call_openai_func(prompt, text):
        reply = client.chat.completions.create(
            model="gpt-4.1-2025-04-14",
            messages=[{"role": "user", "content": prompt.format(text=text[:18_000])}],
            temperature=0.0,
        ).choices[0].message.content.strip()
        return reply

    reply = call_openai_func(PROMPT, text)

    # ── robust JSON capture ───────────────────────────────────────────
    try:
        cleaned = reply.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```", 2)[1].strip()
        cleaned = re.sub(r"^\s*json\s*", "", cleaned, flags=re.I)
        match = re.search(r"\{.*\}", cleaned, re.S)
        if not match:
            raise ValueError("No JSON object found")
        parsed = json.loads(match.group(0))
    except Exception as e:
        print("[DEBUG] GPT reply not JSON –", e)
        parsed = EMPTY.copy()

    parsed["gpt_input_text"] = text[:500]
    parsed["gpt_output"]     = reply
    return parsed

# ─── CLI test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(json.dumps(
        extract_vc_info("https://3bfuturehealth.com"),
        indent=2, ensure_ascii=False
    ))
