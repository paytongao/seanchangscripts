import os, json, sys, re
from datetime import datetime
from dotenv import load_dotenv

FIELDS = [
    ("drug_modality", "Drug Modality"),
    ("disease_focus", "Disease Focus"),
    ("geography", "Geography"),
]

# Predefined categories for LLM sorting
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
        # Handle case where comp might be a string instead of a dict
        if isinstance(comp, str):
            print(f"Warning: Skipping string company name '{comp}' - expected dict with fields")
            continue
        if not isinstance(comp, dict):
            print(f"Warning: Skipping non-dict company object of type {type(comp)}")
            continue
        for src, dst in FIELDS:
            for val in comp.get(src) or []:
                if val and val.strip():
                    agg[dst].append(val.strip())
    return agg

def safe_vc_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '', name.strip().replace(' ', '_')).lower()

def create_sorting_prompt(field_type, entries):
    """Create LLM prompt for categorizing entries into predefined categories"""
    
    if field_type == "Drug Modality":
        categories = DRUG_MODALITY_CATEGORIES
        prompt = f"""You are categorizing therapeutic drug modalities into 12 predefined categories. 

CATEGORIES WITH DEFINITIONS:
1. Small Molecules - Low molecular weight compounds (PROTACs, covalent inhibitors, conventional drugs)
2. Genetic Medicines - Gene therapy (AAV, lentivirus), gene editing (CRISPR), RNA therapies (mRNA, siRNA, ASOs)
3. Biologics - Monoclonal antibodies, bispecific antibodies, ADCs, fusion proteins, therapeutic enzymes
4. Peptides & Proteins - Peptide therapeutics, protein replacement, hormone therapies (insulin, growth hormone)
5. Cell Therapies - CAR-T, CAR-NK, stem cells, engineered immune cells, iPSC-derived therapies
6. Immunotherapies - Checkpoint inhibitors, cytokines, immune agonists, tumor vaccines
7. Microbiome-Based Therapies - Live biotherapeutic products, engineered bacteria, postbiotics, FMT
8. Neuromodulators - Botulinum toxin, neuropeptides, bioelectronics, neural pathway modulators
9. Oncolytic Viruses - Engineered viruses that lyse cancer cells
10. Vaccines - Prophylactic vaccines, therapeutic cancer vaccines, mRNA/DNA vaccines
11. Antibiotics / Anti-Infectives - Traditional antibiotics, antimicrobial peptides, phage therapy
12. Other / Miscellaneous Modalities - Exosomes, nanoparticles, radioimmunotherapy, any uncategorizable modalities

TASK: Sort each entry into exactly one category. Every entry must be assigned.

"""
    
    elif field_type == "Disease Focus":
        categories = DISEASE_FOCUS_CATEGORIES  
        prompt = f"""You are categorizing disease focus areas into 17 predefined categories.

CATEGORIES WITH DEFINITIONS:
1. Infectious Diseases - Viral, bacterial, fungal, parasitic infections, AMR, vaccines
2. Cancer / Oncology - Solid tumors, hematologic cancers, oncology, any cancer-related
3. Cardiovascular Diseases - Heart disease, stroke, hypertension, vascular disorders
4. Neurological Diseases - Neurodegenerative (Alzheimer's, Parkinson's), epilepsy, CNS diseases, neuromuscular
5. Genetic & Rare Diseases - Monogenic disorders, orphan diseases, inherited conditions
6. Autoimmune & Inflammatory Diseases - RA, lupus, IBD, psoriasis, inflammatory conditions
7. Respiratory Diseases - Asthma, COPD, pulmonary fibrosis, lung diseases
8. Gastrointestinal (GI) Diseases - IBD, IBS, GERD, liver diseases, GI disorders
9. Hematological Disorders - Blood disorders, anemias, clotting disorders, blood cancers
10. Musculoskeletal Diseases - Osteoarthritis, muscular dystrophies, bone/muscle disorders
11. Dermatological Diseases - Skin conditions, dermatitis, skin cancers
12. Psychiatric & Behavioral Disorders - Depression, anxiety, mental health conditions
13. Ophthalmic Diseases - Eye diseases, retinal disorders, vision problems
14. Endocrine & Metabolic Diseases - Diabetes, obesity, thyroid, metabolic disorders
15. Reproductive & Women's Health - PCOS, endometriosis, menopause, reproductive health
16. Pediatric Diseases - Childhood diseases, congenital disorders, pediatric conditions
17. Other - Very niche or uncategorizable diseases

TASK: Sort each entry into exactly one category. Every entry must be assigned.

"""
        
    elif field_type == "Geography":
        categories = GEOGRAPHY_CATEGORIES
        prompt = f"""You are categorizing geographic locations into 6 continental regions.

CATEGORIES WITH DEFINITIONS:
1. North America - United States, Canada, Mexico
2. South America - Brazil, Argentina, Chile, Colombia, Peru, etc.
3. Europe - United Kingdom, Germany, France, Switzerland, Netherlands, etc.
4. Asia - Japan, China, India, Singapore, South Korea, etc.
5. Africa - South Africa, Kenya, Nigeria, Egypt, etc.
6. Oceania - Australia, New Zealand, Pacific Islands

TASK: Sort each entry into exactly one continental category. Every entry must be assigned.

"""
    
    prompt += f"""
INPUT ENTRIES: {json.dumps(entries, ensure_ascii=False)}

CRITICAL INSTRUCTIONS:
1. Every single entry must be categorized (no entry left unassigned)
2. Each entry goes to exactly one category 
3. Include duplicates - if "small molecules" appears twice, list it twice in the same category
4. Preserve original entry text exactly as provided

REQUIRED OUTPUT FORMAT:
{{
  "Category Name 1": ["entry1", "entry2", "entry3"],
  "Category Name 2": ["entry4", "entry5"],
  "Category Name 3": [],
  ...
}}

Return a JSON object where:
- Keys are the exact category names from the 12/17/6 predefined categories
- Values are arrays of entries assigned to that category
- ALL {len(categories)} categories must be present as keys (even if empty)
- Total entries in output must equal input count: {len(entries)}
"""
    
    return prompt

def sort_with_llm(field_type, entries, openai_client):
    """Sort entries into predefined categories using LLM"""
    if not openai_client or not entries:
        return create_empty_categories(field_type)
    
    prompt = create_sorting_prompt(field_type, entries)
    
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4.1",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0,
        ).choices[0].message.content
        
        # Parse JSON response
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            categorized = json.loads(match.group(0))
            
            # Ensure all categories are present
            categorized = ensure_all_categories_present(field_type, categorized)
            
            # Add percentages to category names
            total_entries = len(entries)
            result = {}
            for category, category_entries in categorized.items():
                percentage = round((len(category_entries) / total_entries) * 100) if total_entries > 0 else 0
                category_with_percentage = f"{category} ({percentage}%)"
                result[category_with_percentage] = category_entries
                
            return result
            
    except Exception as e:
        print(f"LLM sorting error for {field_type}: {e}")
        return create_empty_categories(field_type)
    
    return create_empty_categories(field_type)

def create_empty_categories(field_type):
    """Create empty category structure with 0% for all categories"""
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
    """Ensure all predefined categories are present in the result"""
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

# --- Main Entrypoint ---
def build_summary(companies, input_data=None, openai_client=None):
    # Debug: Check what we're receiving
    print(f"build_summary received companies of type: {type(companies)}")
    if hasattr(companies, 'read'):
        print("ERROR: Received file object instead of data!")
        raise TypeError("Expected list of companies, got file object")
    
    aggregated = aggregate(companies)
    result = {"aggregated": aggregated}
    
    # Add LLM sorting if OpenAI client is available
    if openai_client:
        print("🧠 Running LLM sorting analysis...")
        sorted_analysis = {}
        
        for field_name, entries in aggregated.items():
            if entries:  # Only process fields that have data
                print(f"🔄 Sorting {field_name}: {len(entries)} entries")
                sorted_result = sort_with_llm(field_name, entries, openai_client)
                sorted_analysis[field_name] = sorted_result
                print(f"✅ {field_name} sorted into {len(sorted_result)} categories")
            else:
                print(f"⚠️ {field_name}: No entries to sort")
                sorted_analysis[field_name] = create_empty_categories(field_name)
        
        result["sorted_analysis"] = sorted_analysis
        print("🎉 LLM sorting complete!")
    else:
        print("ℹ️ OpenAI client not available - skipping LLM sorting")
    
    # Add clean company names list for reference
    try:
        company_names = []
        for i, company in enumerate(companies):
            if isinstance(company, dict):
                company_names.append(company.get("company_name", "Unknown"))
            elif isinstance(company, str):
                company_names.append(company)
            else:
                print(f"WARNING: Company {i} is of unexpected type: {type(company)}")
                if hasattr(company, 'read'):
                    print("ERROR: Found file object in companies list!")
                    raise TypeError(f"Company {i} is a file object, not a dict or string")
        result["companies"] = company_names
        print(f"📋 Included {len(company_names)} company names in output for reference")
    except Exception as e:
        print(f"ERROR in company name extraction: {e}")
        print(f"Companies type: {type(companies)}")
        if companies:
            print(f"First item type: {type(companies[0])}")
        raise
    
    # Pass through therapeutic_investor_portfolio if present
    if input_data and isinstance(input_data, dict) and "therapeutic_investor_portfolio" in input_data:
        result["therapeutic_investor_portfolio"] = input_data["therapeutic_investor_portfolio"]
    return result

def run_aggregation(input_path, vc_name_fs, output_dir=None):
    """
    Run aggregation on enriched company data with LLM sorting by default.
    Args:
        input_path (str): Path to input JSON (from enrichment)
        vc_name_fs (str): Filesystem-safe VC name
        output_dir (str, optional): Output directory. Defaults to output/runs/<vc_name_fs>/
    Returns:
        str: Path to output JSON file
    """
    import os, json
    from datetime import datetime
    from dotenv import load_dotenv
    
    # Always load environment variables for OpenAI
    load_dotenv()
    
    # Load input and handle dict/list
    print(f"Loading data from: {input_path}")
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)
    
    # Debug: Show data structure
    print(f"Data type: {type(data)}")
    if isinstance(data, dict):
        print(f"Data keys: {list(data.keys())}")
    
    # Robustly extract companies list
    if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
        companies = data["results"]
        print(f"Extracted {len(companies)} companies from 'results' key")
    elif isinstance(data, dict) and "companies" in data and isinstance(data["companies"], list):
        companies = data["companies"]
        print(f"Extracted {len(companies)} companies from 'companies' key")
    elif isinstance(data, list):
        companies = data
        print(f"Data is already a list with {len(companies)} items")
    else:
        raise ValueError("Input JSON must be a dict with 'results' or 'companies' key or a list of company dicts.")
    
    # Debug: Check first company structure
    if companies and len(companies) > 0:
        print(f"First company type: {type(companies[0])}")
        if isinstance(companies[0], dict):
            print(f"First company keys: {list(companies[0].keys())[:5]}...")  # Show first 5 keys
    
    # Always try to setup OpenAI client for LLM sorting
    openai_client = None
    try:
        import openai
        openai_client = openai.OpenAI()
        print("🔑 OpenAI client initialized for LLM sorting")
    except Exception as e:
        print(f"⚠️ Warning: Could not initialize OpenAI client: {e}")
        print("📝 Running aggregation without LLM sorting")
    
    # Extra safety check before calling build_summary
    if hasattr(companies, 'read'):
        print("CRITICAL ERROR: 'companies' is a file object!")
        raise TypeError("companies variable is a file object, not a list")
    
    print(f"About to call build_summary with {len(companies)} companies")
    summary = build_summary(companies, input_data=data, openai_client=openai_client)
    if not output_dir:
        output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Include "sorted" in filename if LLM sorting was used
    filename_suffix = "_sorted" if openai_client else ""
    outpath = os.path.join(output_dir, f"{vc_name_fs}_aggregated{filename_suffix}_{ts}.json")
    
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"[OK] aggregation complete: {outpath}")
    return outpath

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: aggregated_with_dedup.py <input_path> <vc_name_fs>")
        print("  LLM sorting will run automatically if OpenAI API key is available")
        sys.exit(1)
    
    input_path = sys.argv[1]
    vc_name_fs = sys.argv[2]
    
    with open(input_path, encoding="utf-8") as f:
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
    from dotenv import load_dotenv
    load_dotenv()
    try:
        import openai
        openai_client = openai.OpenAI()
        print("🔑 OpenAI client initialized for LLM sorting")
    except Exception as e:
        print(f"⚠️ Warning: Could not initialize OpenAI client: {e}")
        print("📝 Running aggregation without LLM sorting")
    
    # Extra safety check before calling build_summary
    if hasattr(companies, 'read'):
        print("CRITICAL ERROR: 'companies' is a file object!")
        raise TypeError("companies variable is a file object, not a list")
    
    print(f"About to call build_summary with {len(companies)} companies")
    summary = build_summary(companies, input_data=data, openai_client=openai_client)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    runs_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(runs_dir, exist_ok=True)
    
    # Include "sorted" in filename if LLM sorting was used
    filename_suffix = "_sorted" if openai_client else ""
    outpath = os.path.join(runs_dir, f"{vc_name_fs}_aggregated{filename_suffix}_{ts}.json")
    
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"✔ Saved → {outpath}")
    print(f"OUTPUT_FILE: {outpath}")
