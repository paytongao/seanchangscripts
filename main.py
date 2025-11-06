# main.py ─────────────────────────────────────────────────────────────
from flask import Flask, request, jsonify
from urllib.parse import urljoin, urlparse
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time, random, json, os
from dotenv import load_dotenv
from vc_extract import extract_vc_info

load_dotenv()
app = Flask(__name__)
MAX_LINKS_TO_EXTRACT = 9

BANNED_KEYWORDS = [
    "privacy", "policy", "disclaimer", "terms", "cookies", "cookie", "legal",
    "accessibility", "sitemap", "javascript", "wp-login", "feed", "rss", "blog",
    "newsletter", "press", "news", "careers", "jobs", "employment", "mailto:",
    "tel:", "login", "logout", "admin", "signup", "register", "account",
    "dashboard", "esg", "portfolio", "team", "internship", "perspectives",
    "contracts", "fellow", "companies", "resources", "media", "spotlight",
    "diversity", "sustainability", "inclusion", "resource", "cart", "announce",
    "chat", "contact", "reach out", "connect", "investors", "join-us", "who-we-are", "conduct", "history","portfolio-companies",
    # social / sharing
    "linkedin", "twitter", "facebook", "instagram", "social", "share",
    # IR / reports
    "investor-relations", "report", "annual-report", "sec-filings",
    # content hubs, press releases
    "insights", "stories", "whitepaper", "case-study", "webinar", "calendar",
    "podcast", "press-release", "pressrelease", "newsroom", "media-center", "featuring"
    # weird glyph hack that occasionally appears
    "d i v e r s i t y",
]

# ==== NEW: Filetype skip list and filter ====
SKIP_EXTENSIONS = (
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".rar",
    ".csv", ".txt", ".mp4", ".mp3", ".jpg", ".png", ".jpeg", ".gif", ".svg"
)

def is_filetype(url: str) -> bool:
    """Return True if the URL ends with a common non-HTML file extension."""
    return url.lower().split("?")[0].endswith(SKIP_EXTENSIONS)

# ── helpers ──────────────────────────────────────────────────────────
def is_banned(url: str) -> bool:
    path = urlparse(url).path.lower()
    scheme = urlparse(url).scheme
    if scheme in ("mailto", "tel", "javascript"):
        return True
    return any(bad in path for bad in BANNED_KEYWORDS)

def is_same_domain(base: str, test: str) -> bool:
    return urlparse(base).netloc == urlparse(test).netloc

def normalize_url(u: str) -> str:
    p = urlparse(u)
    return p._replace(path=p.path.rstrip("/").lower()).geturl()

def extract_snippet(driver, limit: int = 200) -> str:
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for t in soup(["script", "style", "nav", "footer", "header"]):
        t.decompose()
    lines = [ln for ln in soup.get_text(" ", strip=True).splitlines() if len(ln) > 20]
    return " ".join(lines)[:limit]

def aggregate_vc_fields(results: list[dict]) -> dict:
    keys = [
        "drug_modality", "disease_focus", "geography",
        "investment_stage", "investment_amount",
        "requires_startup_revenue_generation", "therapeutic_investor", "equity_investor"
    ]
    agg = {k: [] for k in keys}
    for r in results:
        info = r.get("extracted_info", {})
        for k in keys:
            val = info.get(k)
            # --- List fields
            if isinstance(val, list):
                agg[k].extend(val)
            # --- Boolean fields (True/False/None)
            elif isinstance(val, bool):
                agg[k].append("true" if val else "false")
            # --- Explicit null: skip, or add for traceability
            elif val is None:
                continue
            # --- String fields
            elif isinstance(val, str) and val.strip():
                agg[k].append(val.strip())
    # --- Clean up
    for k in keys:
        # For investment_amount, keep as string (if you want)
        if k == "investment_amount":
            uniq = sorted({v for v in agg[k] if v and v != "null"})
            agg[k] = ", ".join(uniq)
        else:
            agg[k] = [v for v in sorted(set(agg[k])) if v and v != "null"]
    return agg

# ── optional cookie-banner click ────────────────────────────────────
def dismiss_cookie_banner(driver):
    try:
        btn = driver.find_element(
            By.XPATH, "//button[contains(translate(., 'ACEIPT','aceipt'), 'accept')]"
        )
        btn.click()
        time.sleep(1)
    except Exception:
        pass  # no banner

# ── main route ───────────────────────────────────────────────────────
@app.route("/crawl_links", methods=["POST"])
def crawl_links():
    url = request.json.get("url")
    if not url:
        return jsonify({"error": "Missing 'url'"}), 400
    print(f"\n============  CRAWL START  {url}  ============\n")

    # ►► driver setup ◄◄
    opt = uc.ChromeOptions()
    opt.add_argument("--headless=new")         # modern headless
    driver = uc.Chrome(options=opt)

    visited, descriptors = set(), []
    try:
        driver.get(url)
        dismiss_cookie_banner(driver)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "a"))
            )
        except Exception:
            print("[WARN] No anchors after 10 s – page may be blocked")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        anchors = soup.find_all("a", href=True)
        print(f"[DEBUG] Found {len(anchors)} <a> tags on homepage")
        for a in anchors:
            raw_href   = a["href"]
            anchor_txt = a.get_text(" ", strip=True)[:60]
            full_url   = urljoin(url, raw_href)
            norm_url   = normalize_url(full_url)

            reason = None
            if not is_same_domain(url, full_url):
                reason = "external"
            elif norm_url in visited:
                reason = "duplicate"
            elif not full_url.startswith("http"):
                reason = "non-http"
            elif is_banned(full_url):
                reason = "banned"
            elif is_filetype(full_url):    # <---- NEW: Skip files!
                reason = "filetype"

            if reason:
                print(f"  SKIP [{reason:8}] {anchor_txt!r} → {full_url}")
                continue

            visited.add(norm_url)
            print(f"  VISIT              {anchor_txt!r} → {full_url}")
            t0 = time.time()
            try:
                driver.get(full_url)
                dismiss_cookie_banner(driver)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                dt = time.time() - t0
                snippet = extract_snippet(driver)
                descriptors.append({"url": full_url,
                                    "anchor_text": anchor_txt,
                                    "snippet": snippet})
                print(f"     ✓ added (load {dt:.1f}s)")
                if len(descriptors) >= MAX_LINKS_TO_EXTRACT:
                    print("     reached MAX_LINKS_TO_EXTRACT\n")
                    break
            except Exception as e:
                print(f"     ! error loading link: {e}")
                continue

        print(f"\n[DEBUG] Total collected links: {len(descriptors)}\n")

        # ── extraction loop ───────────────────────────────────
        vc_results = []
        for item in descriptors:
            page_url = item["url"]
            print(f"\n=======  EXTRACT  {page_url}  =======")
            try:
                info = extract_vc_info(page_url)
                print("Extracted JSON:", json.dumps(info, indent=2))
            except Exception as e:
                print(f"[EXTRACT ERROR] {e}")
                info = {"error": str(e)}
            vc_results.append({"url": page_url, "extracted_info": info})

        payload = {
            "base_url":               url,
            "links_found":            len(descriptors),
            "top_links":              descriptors,
            "vc_extraction_results":  vc_results,
            "aggregated":             aggregate_vc_fields(vc_results),
        }
        print("\n============  CRAWL DONE   ============\n")
        return jsonify(payload)

    except Exception as e:
        print("[ERROR]", e)
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    from waitress import serve
    print("🚀 Production server running on :8080")
    serve(app, host="0.0.0.0", port=8080)
