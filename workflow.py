# workflow.py  ──────────────────────────────────────────────────────
"""
Build/refresh VC profiles via crawler → OpenAI → Airtable
"""

import time, json, requests
from pyairtable import Table
from deduplicate_fields import deduplicate_with_llm
from vc_profile import generate_vc_profile_summary
import os
import re
from datetime import datetime
from portfolio_ss import run_portfolio_discovery
from website_discovery import run_website_discovery
from comprehensive_data_enrichment import run_enrichment
from aggregated_with_dedup import run_aggregation
from concurrent.futures import ThreadPoolExecutor, as_completed

def tri_state(val):
    """
    Accept list / str / bool and return the string
    'true', 'false', or None (blank) for Airtable.
    """
    if isinstance(val, list):
        flat = [str(v).strip().lower() for v in val]
        if "true" in flat:
            return "true"
        if "false" in flat:
            return "false"
        return None
    s = str(val).strip().lower()
    if s == "true":
        return "true"
    if s == "false":
        return "false"
    return None

def print_summary(vc_name: str, aggregated: dict, deduped: dict):
    line = "═" * 60
    print(f"\n{line}\nSUMMARY  —  {vc_name}\n{line}")
    ALL_FIELDS = [
        "drug_modality", "disease_focus", "geography", "investment_stage",
        "investment_amount", "requires_startup_revenue_generation", "therapeutic_investor", "equity_investor"
    ]
    print("• Aggregated (raw union from all pages):")
    for k in ALL_FIELDS:
        v = aggregated.get(k, [])
        print(f"  {k:30} {v}")
    print("\n• After LLM de-duplication:")
    for k in ALL_FIELDS:
        v = deduped.get(k, [])
        print(f"  {k:30} {v}")
    print(line + "\n")

def join_or_none(val):
    if isinstance(val, list):
        s = ", ".join(v for v in val if v and str(v).strip())
        return s or None
    return val or None

def fsafe(name):
    # Lowercase, replace spaces with underscores, remove non-alphanum/underscore
    return re.sub(r'[^a-zA-Z0-9_]', '', name.strip().replace(' ', '_')).lower()

# ── Airtable config ────────────────────────────────────────────────
AIRTABLE_API_KEY = "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
BASE_ID          = "app768aQ07mCJoyu8"
VC_TABLE_NAME    = "VC Database"

CRAWLER_ENDPOINT = "http://127.0.0.1:8080/crawl_links"
HEADERS          = {"Content-Type": "application/json"}

vc_table = Table(AIRTABLE_API_KEY, BASE_ID, VC_TABLE_NAME)
FIELDS_TO_FETCH = [
    "VC/Investor Name", "Website URL",
    "Drug Modality", "Disease Focus",
    "Geography", "Investment Stage", "Investment Amount",
    "Full Profile Generated?", "Full Profile Text",
    "Drug Modality (Portfolio)", "Disease Focus (Portfolio)", "Geography (Portfolio)"
]
vc_records = vc_table.all(fields=FIELDS_TO_FETCH + ["Crawled?", "Portfolio Scraper Applied?"])

# Sort all records for consistent processing
vc_records.sort(
    key=lambda r: (r["fields"].get("VC/Investor Name") or "").strip().lower()
)

print(f"🔎 Pulled {len(vc_records)} VCs for processing")

# ── Pipeline Native Orchestration ───────────────────────────────
from pathlib import Path

def process_vc_extraction(rec):
    """Process VC basic extraction only"""
    f         = rec["fields"]
    record_id = rec["id"]
    vc_name   = f.get("VC/Investor Name", "No name")
    website   = f.get("Website URL")

    # Check if already processed
    if f.get("Crawled?"):
        print(f"[{vc_name}] ✅ Basic extraction already done - skipping")
        return

    if not website:
        print(f"[{vc_name}] ⚠ no website – skipping extraction")
        return

    # 1) Crawl & extract
    print(f"[{vc_name}] Crawling for basic extraction …")
    try:
        resp = requests.post(
            CRAWLER_ENDPOINT,
            json={"url": website},
            headers=HEADERS,
            timeout=240, 
        )
    except Exception as e:
        print(f"[{vc_name}] ❌ crawl request error:", e)
        return

    if resp.status_code != 200:
        print(f"[{vc_name}] ❌ crawl error →", resp.text[:120])
        return

    data        = resp.json()
    aggregated  = data["aggregated"]

    # Immediately update "Crawled?" flag in Airtable
    try:
        vc_table.update(record_id, {"Crawled?": True}, typecast=True)
        print(f"[{vc_name}] ✅ Airtable: Crawled? set to True")
    except Exception as e:
        print(f"[{vc_name}] ⚠ Crawled? update failed:", e)

    deduped     = {k: deduplicate_with_llm(k, v) for k, v in aggregated.items()}
    print_summary(vc_name, aggregated, deduped)

    any_vals = any(deduped.get(k) for k in
                   ["drug_modality", "disease_focus",
                    "geography", "investment_stage",
                    "investment_amount"])

    profile_txt  = (generate_vc_profile_summary(deduped, vc_name)
                    if any_vals else "")
    profile_done = bool(profile_txt)

    update = {
        "Drug Modality"                        : join_or_none(deduped["drug_modality"]),
        "Disease Focus"                        : join_or_none(deduped["disease_focus"]),
        "Geography"                            : join_or_none(deduped["geography"]),
        "Investment Stage"                     : join_or_none(deduped["investment_stage"]),
        "Investment Amount"                    : join_or_none(deduped["investment_amount"]),
        "Full Profile Generated?"              : profile_done,
        "Full Profile Text"                    : profile_txt or None,
        "Requires Startup Revenue Generation?" : tri_state(
            deduped.get("requires_startup_revenue_generation")
        ),
        "Therapeutic Investor?" : tri_state(
            deduped.get("therapeutic_investor")
        ),
        "Equity Investor?"  : tri_state(
            deduped.get("equity_investor")
        )
    }

    try:
        at_resp = vc_table.update(record_id, update, typecast=True)
        stored = {k: at_resp["fields"].get(k) for k in update}
        print(f"[{vc_name}] ✅ Airtable basic extraction updated →", stored)
    except Exception as e:
        print(f"[{vc_name}] ⚠ Airtable basic extraction update failed:", e)

def process_portfolio_analysis(rec):
    """Process portfolio analysis only"""
    f         = rec["fields"]
    record_id = rec["id"]
    vc_name   = f.get("VC/Investor Name", "No name")
    website   = f.get("Website URL")

    # Check if already processed
    if f.get("Portfolio Scraper Applied?"):
        print(f"[{vc_name}] ✅ Portfolio analysis already done - skipping")
        return

    if not website:
        print(f"[{vc_name}] ⚠ no website – skipping portfolio analysis")
        return
    
    # Check if VC has less than 3 fields filled out
    # basic_fields = ["Drug Modality", "Disease Focus", "Geography", "Investment Stage", "Investment Amount"]
    # filled_fields = [field for field in basic_fields if f.get(field)]
    
    # if len(filled_fields) >= 3:
    #     print(f"[{vc_name}] ✅ Already has {len(filled_fields)} fields filled - skipping portfolio analysis")
    #     # Mark as analyzed to avoid future checks
    #     try:
    #         vc_table.update(record_id, {"Portfolio Scraper Applied?": "true"}, typecast=True)
    #         print(f"[{vc_name}] ✅ Airtable: Portfolio Scraper Applied? set to true (sufficient data)")
    #     except Exception as e:
    #         print(f"[{vc_name}] ⚠ Portfolio analysis flag update failed:", e)
    #     return

    # Normalize VC name ONCE per record
    vc_name_fs = fsafe(vc_name)

    print(f"[{vc_name}] 🚨 Running portfolio analysis pipeline…")
    output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # 1️⃣ Portfolio discovery
        portfolio_json = run_portfolio_discovery(website, vc_name_fs, output_dir)
        # 2️⃣ Website discovery
        websites_json = run_website_discovery(portfolio_json, vc_name_fs, output_dir=output_dir)
        # 3️⃣ Data enrichment
        enrich_json = run_enrichment(websites_json, vc_name_fs, output_dir=output_dir)
        # 4️⃣ Deduplication
        output_json = run_aggregation(enrich_json, vc_name_fs, output_dir=output_dir)
    except ValueError as e:
        if "No companies found in orchestrator input file!" in str(e):
            print(f"[{vc_name}] ⚠ No companies found after portfolio extraction. Marking as analyzed.")
            # Still mark as analyzed even if no companies found
            try:
                vc_table.update(record_id, {"Portfolio Scraper Applied?": "true"}, typecast=True)
                print(f"[{vc_name}] ✅ Airtable: Portfolio Scraper Applied? set to true (no companies)")
            except Exception as e2:
                print(f"[{vc_name}] ⚠ Portfolio analysis flag update failed:", e2)
            return
        else:
            raise
    
    print(f"[{vc_name}] Output file will be: {os.path.abspath(output_json)}")
    if not os.path.exists(output_json):
        print(f"[{vc_name}] ❌ Output file not found: {output_json}")
        print(f"[{vc_name}] Directory listing: {os.listdir(os.path.dirname(output_json))}")
        return
    
    print(f"[{vc_name}] Loading JSON from: {output_json}")
    print(f"[{vc_name}] File type check - output_json type: {type(output_json)}")
    
    try:
        with open(output_json, "r", encoding="utf-8") as json_file:
            print(f"[{vc_name}] File opened successfully, loading JSON...")
            dedup_summary = json.load(json_file)
            print(f"[{vc_name}] JSON loaded successfully, type: {type(dedup_summary)}")
            if isinstance(dedup_summary, dict):
                print(f"[{vc_name}] JSON keys: {list(dedup_summary.keys())[:5]}...")
    except Exception as e:
        print(f"[{vc_name}] ERROR loading JSON: {type(e).__name__}: {e}")
        raise
    
    # Debug check
    if hasattr(dedup_summary, 'read'):
        print(f"[{vc_name}] CRITICAL ERROR: dedup_summary is a file object!")
        raise TypeError("dedup_summary is a file object, not a dict")
    
    # Update Portfolio fields in Airtable
    portfolio_fields = dedup_summary.get("deduplicated_llm") or dedup_summary.get("deduplicated", {})
    def join(val):
        if isinstance(val, list):
            return ", ".join(v for v in val if v and str(v).strip())
        return val or None
    
    # Get therapeutic_investor_portfolio from top-level key in output JSON
    therapeutic_portfolio_val = dedup_summary.get("therapeutic_investor_portfolio")
    def tri_state_portfolio(val):
        if isinstance(val, bool):
            return "true" if val else "false"
        s = str(val).strip().lower()
        if s == "true":
            return "true"
        if s == "false":
            return "false"
        return None
    
    # Format sorted_analysis for Drug Modality and Disease Focus (Portfolio)
    def format_sorted_analysis(sa_key):
        sorted_analysis = dedup_summary.get("sorted_analysis", {}).get(sa_key, {})
        out = []
        for cat, vals in sorted_analysis.items():
            if vals:
                out.append(f"{cat}: {', '.join(vals)}")
        return "; ".join(out) if out else None

    # Refetch the record from Airtable to get the latest 'Therapeutic Investor?' value
    latest_fields = vc_table.get(record_id)["fields"]
    latest_therapeutic = latest_fields.get("Therapeutic Investor?")
    portfolio_update = {
        "Drug Modality (Portfolio)": format_sorted_analysis("Drug Modality"),
        "Disease Focus (Portfolio)": format_sorted_analysis("Disease Focus"),
        "Geography (Portfolio)": format_sorted_analysis("Geography"),
        "Portfolio Aggregated JSON": json.dumps(dedup_summary, indent=2, ensure_ascii=False),
        "Portfolio Scraper Applied?": tri_state_portfolio(True),
    }
    # Only update if field is missing, blank, 'none', or 'null' (never if set to any value)
    if latest_therapeutic is None or str(latest_therapeutic).strip().lower() in ("", "none", "null"):
        portfolio_update["Therapeutic Investor?"] = tri_state_portfolio(therapeutic_portfolio_val)
    
    try:
        at_resp = vc_table.update(record_id, portfolio_update, typecast=True)
        stored = {k: at_resp["fields"].get(k) for k in portfolio_update}
        print(f"[{vc_name}] ✅ Airtable (Portfolio fields) updated →", stored)
    except Exception as e:
        print(f"[{vc_name}] ⚠ Airtable (Portfolio fields) update failed:", e)
    
    time.sleep(0.25)

# Process each VC through both extractors
print(f"\n🚀 Processing {len(vc_records)} VCs through both extractors...")

for i, rec in enumerate(vc_records, 1):
    try:
        vc_name = rec["fields"].get("VC/Investor Name", "Unknown VC")
        print(f"\n[{i}/{len(vc_records)}] Processing: {vc_name}")
        
        # Step 1: Basic extraction (if not already done)
        process_vc_extraction(rec)
        
        # Step 2: Portfolio analysis (if not already done)
        process_portfolio_analysis(rec)
        
    except Exception as e:
        vc_name = rec["fields"].get("VC/Investor Name", "Unknown VC")
        print(f"[{vc_name}] ⚠ Error processing VC: {type(e).__name__}: {e}")
        import traceback
        print(f"[{vc_name}] Full traceback:")
        traceback.print_exc()

print("\nDone.")
