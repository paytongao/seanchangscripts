
"""
Aggregation Double-Check Utility (refactored to match aggregated_with_dedup.py output)
- Loads a comprehensive_data_enrichment output file (JSON)
- Aggregates/deduplicates fields (like aggregated_with_dedup.py)
- Performs LLM sorting if OpenAI API key is available
- Updates the relevant Airtable VC record with portfolio fields
- Usage: python aggregation_doublecheck.py <enriched_json_path> <vc_name> <vc_website>
"""

import sys
import json
import os
from pyairtable import Table
from datetime import datetime
from dotenv import load_dotenv

# --- Aggregation and LLM sorting logic (mirrored from aggregated_with_dedup.py) ---
FIELDS = [
    ("drug_modality", "Drug Modality"),
    ("disease_focus", "Disease Focus"),
    ("geography", "Geography"),
]

DRUG_MODALITY_CATEGORIES = [
    "Small Molecules",
    "Genetic Medicines",
    "Biologics",
    "Peptides & Proteins",
    "Cell Therapies",
    "Immunotherapies",
    "Microbiome-Based Therapies",
    "Neuromodulators",
    "Oncolytic Viruses",
    "Vaccines",
    "Antibiotics / Anti-Infectives",
    "Other / Miscellaneous Modalities"
]

DISEASE_FOCUS_CATEGORIES = [
    "Infectious Diseases",
    "Cancer / Oncology",
    "Cardiovascular Diseases",
    "Neurological Diseases",
    "Genetic & Rare Diseases",
    "Autoimmune & Inflammatory Diseases",
    "Respiratory Diseases",
    "Gastrointestinal (GI) Diseases",
    "Hematological Disorders",
    "Musculoskeletal Diseases",
    "Dermatological Diseases",
    "Psychiatric & Behavioral Disorders",
    "Ophthalmic Diseases",
    "Endocrine & Metabolic Diseases",
    "Reproductive & Women's Health",
    "Pediatric Diseases",
    "Other"
]

GEOGRAPHY_CATEGORIES = [
    "North America",
    "South America",
    "Europe",
    "Asia",
    "Africa",
    "Oceania"
]

def aggregate(companies):
    agg = {dst: [] for _, dst in FIELDS}
    for comp in companies:
        if isinstance(comp, str):
            continue
        if not isinstance(comp, dict):
            continue
        for src, dst in FIELDS:
            field_value = comp.get(src)
            if field_value is None:
                continue
            # Handle both list and string values
            if isinstance(field_value, str):
                # If it's a string, treat it as a single item
                if field_value.strip():
                    agg[dst].append(field_value.strip())
            elif isinstance(field_value, list):
                # If it's a list, iterate through items
                for val in field_value:
                    if val and str(val).strip():
                        agg[dst].append(str(val).strip())
    return agg

def create_empty_categories(field_type):
    if field_type == "Drug Modality":
        categories = DRUG_MODALITY_CATEGORIES
    elif field_type == "Disease Focus":
        categories = DISEASE_FOCUS_CATEGORIES
    elif field_type == "Geography":
        categories = GEOGRAPHY_CATEGORIES
    else:
        return {}
    return {f"{cat} (0%)": [] for cat in categories}

def ensure_all_categories_present(field_type, categorized):
    if field_type == "Drug Modality":
        all_categories = DRUG_MODALITY_CATEGORIES
    elif field_type == "Disease Focus":
        all_categories = DISEASE_FOCUS_CATEGORIES
    elif field_type == "Geography":
        all_categories = GEOGRAPHY_CATEGORIES
    else:
        return categorized
    for category in all_categories:
        if category not in categorized:
            categorized[category] = []
    return categorized

def sort_with_llm(field_type, entries, openai_client):
    if not openai_client or not entries:
        return create_empty_categories(field_type)
    prompt = create_sorting_prompt(field_type, entries)
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0,
        ).choices[0].message.content
        import re
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            try:
                categorized = json.loads(match.group(0))
            except json.JSONDecodeError as json_err:
                print(f"JSON parsing error for {field_type}: {json_err}")
                print(f"Raw response: {response[:500]}...")  # Show first 500 chars
                return create_empty_categories(field_type)
            categorized = ensure_all_categories_present(field_type, categorized)
            total_entries = len(entries)
            result = {}
            for category, category_entries in categorized.items():
                percentage = round((len(category_entries) / total_entries) * 100) if total_entries > 0 else 0
                category_with_percentage = f"{category} ({percentage}%)"
                result[category_with_percentage] = category_entries
            return result
        else:
            print(f"No JSON found in LLM response for {field_type}")
            print(f"Raw response: {response[:500]}...")
            return create_empty_categories(field_type)
    except Exception as e:
        print(f"LLM sorting error for {field_type}: {e}")
        return create_empty_categories(field_type)
    return create_empty_categories(field_type)

def create_sorting_prompt(field_type, entries):
    if field_type == "Drug Modality":
        categories = DRUG_MODALITY_CATEGORIES
        examples = "Small Molecules, Genetic Medicines, Biologics, etc."
    elif field_type == "Disease Focus":
        categories = DISEASE_FOCUS_CATEGORIES
        examples = "Cancer/Oncology, Cardiovascular Diseases, Neurological Diseases, etc."
    elif field_type == "Geography":
        categories = GEOGRAPHY_CATEGORIES
        examples = "North America, Europe, Asia, etc."
    else:
        return "Categorize the entries."
    
    prompt = f"""Please categorize the following {field_type} entries into the appropriate categories.

Categories available:
{', '.join(categories)}

Entries to categorize:
{', '.join(entries)}

Return ONLY a valid JSON object with each category as a key and an array of matching entries as the value.
Example format:
{{
  "{categories[0]}": ["entry1", "entry2"],
  "{categories[1]}": ["entry3"]
}}

IMPORTANT: Return ONLY the JSON object, no additional text."""
    return prompt

def build_summary(companies, input_data=None, openai_client=None):
    aggregated = aggregate(companies)
    result = {"aggregated": aggregated}
    if openai_client:
        sorted_analysis = {}
        for field_name, entries in aggregated.items():
            if entries:
                sorted_result = sort_with_llm(field_name, entries, openai_client)
                sorted_analysis[field_name] = sorted_result
            else:
                sorted_analysis[field_name] = create_empty_categories(field_name)
        result["sorted_analysis"] = sorted_analysis
    # Add company names
    company_names = []
    for company in companies:
        if isinstance(company, dict):
            company_names.append(company.get("company_name", "Unknown"))
        elif isinstance(company, str):
            company_names.append(company)
    result["companies"] = company_names
    # Pass through therapeutic_investor_portfolio if present
    if input_data and isinstance(input_data, dict) and "therapeutic_investor_portfolio" in input_data:
        result["therapeutic_investor_portfolio"] = input_data["therapeutic_investor_portfolio"]
    return result

# --- Airtable config ---
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY") or "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
BASE_ID = os.environ.get("AIRTABLE_BASE_ID") or "app768aQ07mCJoyu8"
VC_TABLE_NAME = os.environ.get("AIRTABLE_VC_TABLE") or "VC Database"

def format_for_airtable(sorted_analysis):
    """Format sorted_analysis data for Airtable fields with percentages and individual entries"""
    def format_category_with_entries(categories_dict):
        if not categories_dict:
            return None
        # Create a formatted string with categories, percentages, and their entries
        formatted_items = []
        for category_name, items in categories_dict.items():
            if category_name and items:  # Only include non-empty categories
                # Add category with percentage
                formatted_items.append(f"{category_name}: {', '.join(items)}")
        return " | ".join(formatted_items) if formatted_items else None
    
    # If we have sorted_analysis structure, format with percentages and entries
    if isinstance(sorted_analysis, dict) and all(isinstance(v, dict) for v in sorted_analysis.values()):
        return {
            "Drug Modality (Portfolio)": format_category_with_entries(sorted_analysis.get("Drug Modality", {})),
            "Disease Focus (Portfolio)": format_category_with_entries(sorted_analysis.get("Disease Focus", {})),
            "Geography (Portfolio)": format_category_with_entries(sorted_analysis.get("Geography", {})),
        }
    # Fallback to simple list formatting (shouldn't happen with sorted_analysis)
    else:
        def join(val):
            if isinstance(val, list):
                # Fix: Ensure we're not splitting strings into characters
                clean_items = []
                for v in val:
                    if v and str(v).strip():
                        # Check if item looks like it was character-split
                        if len(str(v)) == 1 and clean_items and len(clean_items[-1]) == 1:
                            # Skip single characters that look like they're part of a word
                            continue
                        clean_items.append(str(v).strip())
                return ", ".join(clean_items)
            return val or None
        return {
            "Drug Modality (Portfolio)": join(sorted_analysis.get("Drug Modality")),
            "Disease Focus (Portfolio)": join(sorted_analysis.get("Disease Focus")),
            "Geography (Portfolio)": join(sorted_analysis.get("Geography")),
        }

def main():
    if len(sys.argv) < 4:
        print("Usage: python aggregation_doublecheck.py <enriched_json_path> <vc_name> <vc_website>")
        sys.exit(1)
    enriched_path, vc_name, vc_website = sys.argv[1:4]
    load_dotenv()
    with open(enriched_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Robustly extract companies list
    if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
        companies = data["results"]
    elif isinstance(data, dict) and "companies" in data and isinstance(data["companies"], list):
        companies = data["companies"]
    elif isinstance(data, list):
        companies = data
    else:
        raise ValueError("Input JSON must be a dict with 'results' or 'companies' key or a list of company dicts.")

    # Always try to setup OpenAI client for LLM sorting
    openai_client = None
    try:
        import openai
        openai_client = openai.OpenAI()
        print("OpenAI client initialized for LLM sorting")
    except Exception as e:
        print(f"Warning: Could not initialize OpenAI client: {e}")
        print("Running aggregation without LLM sorting")

    summary = build_summary(companies, input_data=data, openai_client=openai_client)

    # Save the aggregated output to the VC's folder in the runs directory
    from pathlib import Path
    vc_folder = Path("output/runs") / vc_name.strip().replace(" ", "_").replace("/", "_")
    vc_folder.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_suffix = "_sorted" if openai_client else ""
    agg_filename = f"{vc_name.strip().replace(' ', '_').replace('/', '_')}_aggregated{filename_suffix}_{ts}.json"
    agg_path = vc_folder / agg_filename
    with open(agg_path, "w", encoding="utf-8") as outf:
        json.dump(summary, outf, indent=2, ensure_ascii=False)
    print(f"Aggregated output saved to: {agg_path}")

    # Prepare Airtable fields
    # Use sorted_analysis if available (when OpenAI client is initialized), otherwise use aggregated
    if "sorted_analysis" in summary:
        airtable_fields = format_for_airtable(summary["sorted_analysis"])
    else:
        airtable_fields = format_for_airtable(summary["aggregated"])
    airtable_fields["Portfolio Aggregated JSON"] = json.dumps(summary, indent=2, ensure_ascii=False)
    airtable_fields["Portfolio Scraper Applied?"] = "true"

    # Find VC record in Airtable
    table = Table(AIRTABLE_API_KEY, BASE_ID, VC_TABLE_NAME)
    records = table.all(fields=["VC/Investor Name", "Website URL"])
    match = None
    for rec in records:
        f = rec["fields"]
        if (f.get("VC/Investor Name", "").strip().lower() == vc_name.strip().lower() or
            f.get("Website URL", "").strip().lower() == vc_website.strip().lower()):
            match = rec
            break
    if not match:
        print(f"No VC found in Airtable for name '{vc_name}' or website '{vc_website}'")
        sys.exit(2)
    record_id = match["id"]
    print(f"Updating Airtable record: {record_id} ({vc_name})")
    resp = table.update(record_id, airtable_fields, typecast=True)
    print("Airtable update response:", resp)

if __name__ == "__main__":
    main()
