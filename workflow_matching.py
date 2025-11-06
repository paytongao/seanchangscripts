# workflow_matching_fixed.py
"""
FIXED VERSION: Similarity matching between startups with "Run Match" = True
and every VC that already has "Full Profile Generated? == True".

Pipeline for each startup with Run Match = True:
1. Pull startup row and build embeddings for 5 fields
2. Iterate over profiled VCs
3. GPT prescan (quick yes / no)
4. If prescan passes -> build VC embeddings
5. Compute per-field + overall similarity
6. Store results in "Startup-VC Matches (POST GPT PRE-SCAN)"
7. Reset "Run Match" to False when done
"""

import time, json, numpy as np, torch
from pyairtable import Table
from dotenv import load_dotenv
import os, openai, sys, argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback
import random

from embedsim import EmbeddingSim

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Airtable creds
AIRTABLE_API_KEY = "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"          # replace
BASE_ID          = "app768aQ07mCJoyu8"

STARTUP_TABLE    = "Startup Submissions"
VC_TABLE         = "VC Database"
MATCHES_TABLE    = "Startup-VC Matches (POST GPT PRE-SCAN)"

FIELDS = [
    "Drug Modality", "Disease Focus",
    "Investment Stage", "Geography", "Investment Amount"
]

# Airtable table handles (pyairtable)
startup_tbl  = Table(AIRTABLE_API_KEY, BASE_ID, STARTUP_TABLE)
vc_tbl       = Table(AIRTABLE_API_KEY, BASE_ID, VC_TABLE)
matches_tbl  = Table(AIRTABLE_API_KEY, BASE_ID, MATCHES_TABLE)

# Threading configuration
# Recommended settings based on server resources:
# - Local testing: 10-15 workers
# - AWS Lightsail $5-10: 20-25 workers  
# - AWS Lightsail $20: 30-40 workers
# - AWS Lightsail $40+: 50-60 workers
MAX_WORKERS = 25  # Increased for better performance (adjust based on your Lightsail instance)

# Rate limiting configuration
OPENAI_RATE_LIMIT_DELAY = 0.05  # Delay between GPT calls if needed (seconds)
AIRTABLE_RATE_LIMIT_DELAY = 0.2  # Delay between Airtable operations (5 req/sec limit)

print_lock = Lock()  # Thread-safe printing
match_records_lock = Lock()  # Thread-safe match collection
airtable_rate_limit_lock = Lock()  # Rate limit Airtable operations

# Airtable update configuration
REAL_TIME_UPDATES = True  # Set to True for immediate Airtable updates, False for batch at end

# Helper Functions

def retry_with_backoff(max_retries=3, base_delay=1.0):
    """Decorator for retrying operations with exponential backoff"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    safe_print(f"Retry {attempt + 1}/{max_retries} after {delay:.2f}s: {str(e)[:100]}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def safe_print(text, prefix=""):
    """Thread-safe print with optional prefix"""
    with print_lock:
        if prefix:
            text = f"{prefix} {text}"
        if isinstance(text, str):
            # Replace single % with %% for safe printing
            safe_text = text.replace('%', '%%')
            print(safe_text)
        else:
            print(repr(text))

def safe_text_extract(text_value):
    """Safely extract text fields and avoid format specifier issues"""
    if text_value is None:
        return None
    # Convert to string and replace % with 'percent' to avoid format issues
    if isinstance(text_value, str):
        return text_value.replace('%', 'percent')
    return str(text_value)

def is_run_match_enabled(value):
    """Check if Run Match field is set to true (accepts True, 'true', 1, '1')"""
    if value is True or value == 'true' or value == 1 or value == '1':
        return True
    if isinstance(value, str) and value.strip().lower() == 'true':
        return True
    return False

def portfolio_scraper_applied(record):
    """Check if Portfolio Scraper Applied? field is set to True"""
    fields = record.get('fields', {})
    val = fields.get('Portfolio Scraper Applied?')
    if val is None:
        return False
    if isinstance(val, bool):
        return val is True
    if isinstance(val, (int, float)):
        return val == 1
    if isinstance(val, str):
        return val.strip().lower() in ('true', '1')
    return False

def _is_true(val):
    """Helper to check if a value is true"""
    if isinstance(val, bool):
        return val is True
    if isinstance(val, (int, float)):
        return val == 1
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1")
    return False

def _to_bool(v): 
    """Convert string to boolean"""
    return str(v).strip().lower() in {"true", "yes", "1"}

def process_vc_for_startup(vc_row, startup_name, startup_fields, create_immediately=False):
    """
    Process a single VC for a given startup. This function is designed to be run in parallel.
    If create_immediately=True, creates the match in Airtable right away.
    Returns a tuple of (match_record, portfolio_update_data) or (None, None) if no match.
    """
    try:
        vc_name = vc_row["fields"].get("VC/Investor Name", "Unnamed VC")
        record_id = vc_row.get("id")
        website = vc_row["fields"].get("Website URL")
        
        # Filter VCs based on criteria
        req_text = str(vc_row["fields"].get("Requires Startup Revenue Generation?", "")).strip().lower()
        therap_text = str(vc_row["fields"].get("Therapeutic Investor?", "")).strip().lower()
        equity_text = str(vc_row["fields"].get("Equity Investor?", "")).strip().lower()
        crawled = vc_row["fields"].get("Crawled?", False)
        portfolio_applied = vc_row["fields"].get("Portfolio Scraper Applied?", False)
        
        # Only keep VCs where both 'Crawled?' and 'Portfolio Scraper Applied?' are true
        if not (_is_true(crawled) and _is_true(portfolio_applied)):
            safe_print(f"[{vc_name}] skipped - Crawled? and/or Portfolio Scraper Applied? not true")
            return None, None
        
        # Skip VCs that demand revenue
        if req_text == "true":
            safe_print(f"[{vc_name}] skipped - requires startup revenue")
            return None, None
        
        # Skip VCs that are NOT therapeutic investors
        if therap_text != "true":
            safe_print(f"[{vc_name}] skipped - not a therapeutic investor")
            return None, None
        
        # Skip debt-only financiers
        if equity_text == "false":
            safe_print(f"[{vc_name}] skipped - equity investor = false")
            return None, None
        
        # Check if VC has enough field groups (3 out of 5 field groups required)
        field_groups = 0
        
        # Drug Modality group (either stated or portfolio)
        drug_modality = vc_row["fields"].get("Drug Modality", "")
        drug_modality_portfolio = vc_row["fields"].get("Drug Modality (Portfolio)", "")
        if (drug_modality and str(drug_modality).strip()) or (drug_modality_portfolio and str(drug_modality_portfolio).strip()):
            field_groups += 1
        
        # Disease Focus group (either stated or portfolio)
        disease_focus = vc_row["fields"].get("Disease Focus", "")
        disease_focus_portfolio = vc_row["fields"].get("Disease Focus (Portfolio)", "")
        if (disease_focus and str(disease_focus).strip()) or (disease_focus_portfolio and str(disease_focus_portfolio).strip()):
            field_groups += 1
        
        # Geography group (either stated or portfolio)
        geography = vc_row["fields"].get("Geography", "")
        geography_portfolio = vc_row["fields"].get("Geography (Portfolio)", "")
        if (geography and str(geography).strip()) or (geography_portfolio and str(geography_portfolio).strip()):
            field_groups += 1
        
        # Investment Stage (standalone)
        investment_stage = vc_row["fields"].get("Investment Stage", "")
        if investment_stage and str(investment_stage).strip():
            field_groups += 1
        
        # Investment Amount (standalone)
        investment_amount = vc_row["fields"].get("Investment Amount", "")
        if investment_amount and str(investment_amount).strip():
            field_groups += 1
        
        if field_groups < 3:
            safe_print(f"[{vc_name}] skipped - only {field_groups} field groups with data (need 3)")
            return None, None
        
        # Build VC fields for GPT prescan
        vc_fields = {f: vc_row["fields"].get(f, "") for f in FIELDS}
        # Add portfolio fields for prescan
        vc_fields["Drug Modality (Portfolio)"] = drug_modality_portfolio
        vc_fields["Disease Focus (Portfolio)"] = disease_focus_portfolio
        vc_fields["Geography (Portfolio)"] = geography_portfolio
        
        # GPT prescan
        prescan = EmbeddingSim.gpt_prescan(startup_fields, vc_fields)
        if not prescan.get("overall_match"):
            safe_print(f"[{vc_name}] prescan FAIL - skipped")
            return None, None
        safe_print(f"[{vc_name}] prescan PASS")
        
        # Create match record
        match_record = {
            "Startup Name": startup_name,
            "VC Name": vc_name,
            "GPT fit?": True
        }
        
        # If create_immediately is True, save to Airtable right away
        airtable_record_id = None
        if create_immediately:
            try:
                # Rate limit Airtable operations
                with airtable_rate_limit_lock:
                    time.sleep(AIRTABLE_RATE_LIMIT_DELAY)
                    created_record = matches_tbl.create(match_record, typecast=True)
                    airtable_record_id = created_record['id']
                safe_print(f"[{vc_name}] MATCH CREATED IN AIRTABLE (ID: {airtable_record_id})")
            except Exception as e:
                safe_print(f"[{vc_name}] ERROR: Failed to create match in Airtable: {str(e)}")
                return None, None
        
        # Portfolio verification
        portfolio_update = None
        if website and record_id:
            try:
                safe_print(f"[{vc_name}] Running portfolio verification analysis...")
                
                # Portfolio data for analysis
                vc_fields_for_analysis = {
                    'Drug Modality': drug_modality,
                    'Disease Focus': disease_focus,
                    'Geography': geography,
                    'Drug Modality (Portfolio)': drug_modality_portfolio,
                    'Disease Focus (Portfolio)': disease_focus_portfolio,
                    'Geography (Portfolio)': geography_portfolio
                }
                
                # Get the portfolio analysis prompt
                portfolio_prompt = EmbeddingSim.get_portfolio_analysis_prompt(startup_fields, vc_fields_for_analysis)
                
                # Sanitize all data
                safe_startup_fields = {}
                for k, v in startup_fields.items():
                    safe_startup_fields[k] = safe_text_extract(v)
                
                startup_profile_json = json.dumps(safe_startup_fields, indent=2)
                
                # Build data section
                data_section = "\n\nDATA TO ANALYZE\n\n"
                data_section += "**STARTUP PROFILE:**\n"
                data_section += startup_profile_json + "\n\n"
                data_section += "**VC STATED INVESTMENT CRITERIA:**\n"
                data_section += "- Drug Modality: " + str(safe_text_extract(drug_modality)) + "\n"
                data_section += "- Disease Focus: " + str(safe_text_extract(disease_focus)) + "\n"
                data_section += "- Geography: " + str(safe_text_extract(geography)) + "\n\n"
                data_section += "**VC ACTUAL PORTFOLIO DATA:**\n"
                data_section += "- Drug Modality (Portfolio): " + str(safe_text_extract(drug_modality_portfolio)) + "\n"
                data_section += "- Disease Focus (Portfolio): " + str(safe_text_extract(disease_focus_portfolio)) + "\n"
                data_section += "- Geography (Portfolio): " + str(safe_text_extract(geography_portfolio)) + "\n\n"
                data_section += "ANALYSIS REQUEST\n"
                data_section += "Please provide your analysis following the guidelines above.\n"
                
                full_prompt = portfolio_prompt + data_section
                
                # Make the OpenAI API call
                resp = openai.chat.completions.create(
                    model="gpt-5-mini",
                    messages=[{"role": "user", "content": full_prompt}]
                )
                raw = resp.choices[0].message.content
                
                # Parse JSON response
                try:
                    if "{" in raw and "}" in raw:
                        json_start = raw.find("{")
                        json_end = raw.rfind("}") + 1
                        json_str = raw[json_start:json_end]
                        portfolio_result = json.loads(json_str)
                    else:
                        raise ValueError("No JSON structure found in response")
                        
                except Exception as parse_error:
                    safe_print(f"[{vc_name}] WARNING: Failed to parse portfolio analysis JSON: {parse_error}")
                    
                    # Try to extract a boolean decision from the text
                    raw_lower = raw.lower()
                    if any(word in raw_lower for word in ["good match", "strong match", "excellent match", "suitable", "fits well", "true"]):
                        portfolio_verified = True
                    elif any(word in raw_lower for word in ["not a match", "poor match", "doesn't fit", "unsuitable", "mismatch", "false"]):
                        portfolio_verified = False
                    else:
                        portfolio_verified = False
                    
                    portfolio_result = {
                        "verified_with_portfolio_analysis": portfolio_verified
                    }
                
                # Ensure boolean conversion
                if "verified_with_portfolio_analysis" in portfolio_result:
                    portfolio_result["verified_with_portfolio_analysis"] = _to_bool(portfolio_result["verified_with_portfolio_analysis"])
                else:
                    portfolio_result["verified_with_portfolio_analysis"] = False
                
                
                # Extract scoring breakdown
                scoring_breakdown = portfolio_result.get("scoring_breakdown", {})
                
                # Prepare portfolio update data
                portfolio_update_data = {
                    "Verified With Portfolio Analysis": portfolio_result.get("verified_with_portfolio_analysis", False),
                    "Overall Assessment": safe_text_extract(scoring_breakdown.get("overall_assessment")),
                    "Drug Modality Portfolio Score": portfolio_result.get("drug_modality_portfolio_score"),
                    "Disease Focus Portfolio Score": portfolio_result.get("disease_focus_portfolio_score"),
                    "Geography Portfolio Score": portfolio_result.get("geography_portfolio_score"),
                    "Overall Portfolio Alignment Score": portfolio_result.get("overall_portfolio_alignment_score")
                }
                
                # Remove None values
                portfolio_update_data = {k: v for k, v in portfolio_update_data.items() if v is not None}
                
                # If we created the record immediately, update it now
                if create_immediately and airtable_record_id:
                    try:
                        # Rate limit Airtable operations
                        with airtable_rate_limit_lock:
                            time.sleep(AIRTABLE_RATE_LIMIT_DELAY)
                            matches_tbl.update(airtable_record_id, portfolio_update_data, typecast=True)
                        safe_print(f"[{vc_name}] PORTFOLIO VERIFICATION UPDATED IN AIRTABLE")
                    except Exception as e:
                        safe_print(f"[{vc_name}] ERROR: Failed to update portfolio verification: {str(e)}")
                else:
                    # Return update data for batch processing
                    portfolio_update = {
                        "vc_name": vc_name,
                        "startup_name": startup_name,
                        "data": portfolio_update_data
                    }
                
                safe_print(f"[{vc_name}] SUCCESS: Portfolio verification completed -> Verified: {portfolio_result.get('verified_with_portfolio_analysis')}")
                
            except Exception as e:
                safe_print(f"[{vc_name}] WARNING: Portfolio analysis failed: {str(e)}")
                portfolio_update = None
        
        return match_record, portfolio_update
        
    except Exception as e:
        safe_print(f"ERROR processing VC {vc_row.get('fields', {}).get('VC/Investor Name', 'Unknown')}: {str(e)}")
        safe_print(traceback.format_exc())
        return None, None

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--startup-id', help='Specific startup record ID to process')
args = parser.parse_args()

# 1) Fetch startups - either specific ID or all with "Run Match" = True AND "Matching Done" != True

if args.startup_id:
    # Process specific startup by ID
    print(f"PROCESSING: Specific startup with ID: {args.startup_id}")
    try:
        startup_record = startup_tbl.get(args.startup_id)
        # Check if matching is already done
        if _is_true(startup_record.get("fields", {}).get("Matching Done?")):
            print(f"INFO: Startup {startup_record['fields'].get('Startup Name', 'Unknown')} already has 'Matching Done?' = True. Skipping to prevent duplicates.")
            exit(0)
        pending_startups = [startup_record]
    except Exception as e:
        print(f"ERROR: Could not fetch startup with ID {args.startup_id}: {e}")
        exit(1)
else:
    # Original behavior - process all with Run Match = True AND Matching Done? != True
    # Using AND formula to check both conditions
    pending_startups = startup_tbl.all(
        formula="AND({Run Match} = TRUE(), OR({Matching Done?} = FALSE(), {Matching Done?} = BLANK()))",
        fields=FIELDS + ["Startup Name", "Run Match", "Matching Done?"],
        sort=["-Created Time"]
    )
    
    if not pending_startups:
        print("INFO: No startup submissions with 'Run Match' = True and 'Matching Done?' != True found.")
        print("NOTICE: Set 'Run Match' to true for any startup in the Startup Submissions table to enable matching.")
        print("NOTICE: Startups with 'Matching Done?' = True will be skipped to prevent duplicates.")
        exit(0)

print(f"SEARCH: Found {len(pending_startups)} startup(s) to process")

# 2) Process each startup individually with parallel processing

for startup_rec in pending_startups:
    startup_name = startup_rec["fields"].get("Startup Name", "Startup")
    startup_fields = {f: startup_rec["fields"].get(f, "") for f in FIELDS}
    
    print(f"\nSTARTUP: Processing startup: {startup_name}")
    print(json.dumps(startup_fields, indent=2))

    # 3) Get all VCs that have a finished profile
    print("Fetching VC database...")
    vc_rows = vc_tbl.all(sort=["VC/Investor Name"])
    print(f"Found {len(vc_rows)} total VCs in database")
    
    # Collections for batch operations
    match_records_to_create = []
    portfolio_updates_to_apply = []
    
    # Debug counters
    vcs_filtered_out = 0
    vcs_passed_to_gpt = 0
    vcs_passed_prescan = 0
    
    # Process VCs in parallel using ThreadPoolExecutor
    print(f"Starting parallel processing with {MAX_WORKERS} workers...")
    if REAL_TIME_UPDATES:
        print("NOTE: Matches will be created in Airtable in real-time as they are found.")
    else:
        print("NOTE: Matches will be collected and batch uploaded at the end.")
    start_time = time.time()
    
    # Use the global configuration setting
    CREATE_IMMEDIATELY = REAL_TIME_UPDATES
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all VC processing tasks
        futures = {
            executor.submit(process_vc_for_startup, vc_row, startup_name, startup_fields, CREATE_IMMEDIATELY): vc_row
            for vc_row in vc_rows
        }
        
        # Track progress
        completed = 0
        total = len(futures)
        
        # Process completed futures as they finish
        for future in as_completed(futures):
            completed += 1
            
            # Progress update every 10 VCs
            if completed % 10 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0
                print(f"Progress: {completed}/{total} VCs processed ({completed*100/total:.1f}%) - "
                      f"Rate: {rate:.1f} VCs/sec - ETA: {eta:.0f}s")
            
            try:
                match_record, portfolio_update = future.result()
                
                # Only collect matches if not creating immediately
                if match_record and not CREATE_IMMEDIATELY:
                    with match_records_lock:
                        match_records_to_create.append(match_record)
                
                # Only collect portfolio updates if not creating immediately
                if portfolio_update and not CREATE_IMMEDIATELY:
                    with match_records_lock:
                        portfolio_updates_to_apply.append(portfolio_update)
                        
            except Exception as e:
                vc_row = futures[future]
                vc_name = vc_row.get("fields", {}).get("VC/Investor Name", "Unknown")
                safe_print(f"ERROR: Failed to process VC {vc_name}: {str(e)}")
    
    # Batch create all matches at once
    if match_records_to_create:
        print(f"\nBatch creating {len(match_records_to_create)} matches...")
        created_count = 0
        try:
            # Airtable batch create (max 10 records per request)
            # Using batch_create for actual batch operations
            batch_size = 10
            for i in range(0, len(match_records_to_create), batch_size):
                batch = match_records_to_create[i:i+batch_size]
                try:
                    # Try batch creation if available
                    if hasattr(matches_tbl, 'batch_create'):
                        matches_tbl.batch_create(batch, typecast=True)
                        created_count += len(batch)
                    else:
                        # Fall back to individual creation
                        for record in batch:
                            matches_tbl.create(record, typecast=True)
                            created_count += 1
                    print(f"  Created batch {i//batch_size + 1}: {len(batch)} records")
                except Exception as batch_error:
                    print(f"  ERROR in batch {i//batch_size + 1}: {str(batch_error)}")
                    # Try individual creation as fallback
                    for record in batch:
                        try:
                            matches_tbl.create(record, typecast=True)
                            created_count += 1
                        except Exception as e2:
                            print(f"    ERROR: Failed to create match for {record.get('VC Name')}: {str(e2)}")
            
            print(f"Successfully created {created_count}/{len(match_records_to_create)} matches")
        except Exception as e:
            print(f"ERROR: Unexpected error during match creation: {str(e)}")
    else:
        print("\nNo matches to create (all VCs filtered out or failed prescan)")
    
    # Apply portfolio updates
    if portfolio_updates_to_apply:
        print(f"\nApplying {len(portfolio_updates_to_apply)} portfolio verifications...")
        for update in portfolio_updates_to_apply:
            try:
                # Find the match record to update
                formula = f"AND({{Startup Name}} = '{update['startup_name']}', {{VC Name}} = '{update['vc_name']}')"
                all_matches = matches_tbl.all(formula=formula)
                if all_matches:
                    match_record_id = all_matches[-1]['id']
                    matches_tbl.update(match_record_id, update['data'], typecast=True)
            except Exception as e:
                print(f"ERROR: Failed to update portfolio data for {update['vc_name']}: {str(e)}")

    # Calculate final statistics
    elapsed_total = time.time() - start_time
    print(f"\nSUCCESS: Finished processing {startup_name}")
    print(f"  - Matches created: {len(match_records_to_create)}")
    print(f"  - Portfolio verifications: {len(portfolio_updates_to_apply)}")
    print(f"  - Total time: {elapsed_total:.1f} seconds")
    print(f"  - Average rate: {len(vc_rows)/elapsed_total:.1f} VCs/second")

    # 6) Set "Matching Done?" = True and optionally reset "Run Match" field after successful completion
    try:
        # Always set Matching Done? = True to prevent re-processing
        update_fields = {"Matching Done?": True}
        
        # Reset Run Match field if not called with specific startup ID
        # NOTE: Run Match is usually handled by auto_workflow_trigger.py BEFORE processing starts
        # This prevents race conditions and duplicate processing
        if not args.startup_id:
            update_fields["Run Match"] = False
            
        startup_tbl.update(startup_rec['id'], update_fields, typecast=True)
        print(f"SUCCESS: Set 'Matching Done?' = True for {startup_name}")
        if not args.startup_id:
            print(f"SUCCESS: Reset 'Run Match' field for {startup_name}")
    except Exception as e:
        print(f"WARNING: Could not update fields for {startup_name}: {e}")

print(f"\nCOMPLETE: All startups processed successfully!")