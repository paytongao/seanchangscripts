import os, json, re, openai
from dotenv import load_dotenv

# ─────────────────────────────
#  OpenAI setup
# ─────────────────────────────
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI()

# ─────────────────────────────
#  Normalisers
# ─────────────────────────────


def normalize_value(val: str) -> str:
    val = val.strip().lower()
    if val in ("series a", "series-a", "series - a"):
        return "series a"
    if val in ("series b", "series-b", "series - b"):
        return "series b"
    if val in ("rare diseases", "rare disease"):
        return "rare disease"
    if val in ("small molecules", "small molecule"):
        return "small molecule"
    return val

def _prep_input(values):
    """Ensure list-of-strings and strip empties."""
    if isinstance(values, str):          # guard against old bug
        values = [values]
    return [normalize_value(v) for v in values if v and v.strip()]

# ─────────────────────────────
#  Main LLM helper
# ─────────────────────────────
def _chat(prompt: str):
    return client.chat.completions.create(
        model="gpt-4.1-2025-04-14",
        messages=[
            {
                "role": "system",
                "content": (
                    "You deduplicate category lists for a database. "
                    "Merge only truly identical items; keep related but distinct ones separate."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=300,
        temperature=0,
    ).choices[0].message.content

# ─────────────────────────────
#  Public entry-point
# ─────────────────────────────
def deduplicate_with_llm(field_name: str, values):
    print(f"DEBUG: deduplicate_with_llm called with field_name='{field_name}', values count={len(values)}")
    
    unique_normalized = []
    seen = set()
    for v in _prep_input(values):
        if v not in seen:
            unique_normalized.append(v)
            seen.add(v)

    if len(unique_normalized) < 2:
        return unique_normalized

    # ── tailored prompts ────────────────────────────────────
    if field_name == "modality":
        prompt = (
            "You are deduplicating values extracted from a venture capital website for the "
            "field 'modality'.\n"
            "Merge semantically similar terms such as cancer and oncology. Think about each decision before you merge terms and understand its meaning." \
            "If a term is a more specific version of another, keep the more specific one.\n"
            "A *therapeutic drug modality* directly treats or cures disease in humans. "
            "Examples: gene therapy, cell therapy, small molecule, biologic, antibody, "
            "RNA-based therapy, cancer vaccine, peptide, microbiome, targeted therapies, "
            "protein therapy.\n"
            "DO NOT include diagnostics, digital health, devices, research tools, etc.\n"
            "Merge identical modalities; keep distinct ones separate. "
            "If an umbrella like 'cell and gene therapy' appears *and* both components are "
            "present, drop the umbrella.\n\n"
            "Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n"
            f"{json.dumps(unique_normalized)}"
        )

    elif field_name == "geography" or field_name == "Geography":
        print(f"DEBUG: Processing geography field with {len(unique_normalized)} locations")
        prompt = (
        f"You MUST return EXACTLY {len(unique_normalized)} geographic locations from this list.\n"
        "CRITICAL: Do NOT remove any locations. Return ALL of them.\n"
        "The ONLY acceptable merges are identical country names like 'US' = 'United States'.\n"
        "Seattle ≠ Austin ≠ Cambridge - these are DIFFERENT cities, keep ALL separate.\n"
        "University addresses are DIFFERENT from city names - keep BOTH.\n"
        "Street addresses are UNIQUE - never remove them.\n\n"
        f"INPUT LIST ({len(unique_normalized)} locations - return ALL {len(unique_normalized)}):\n"
        f"{json.dumps(unique_normalized)}\n\n"
        "Return a JSON array with ALL locations from the input list."
        )

    elif field_name == "investment_stage":
        prompt = (
        "You are deduplicating values for the field 'investment_stage', extracted from venture capital websites.\n"
        "RULES:\n"
        "1. Only merge items that are true duplicates (case, spacing, hyphen/pluralization differences, e.g. 'Series A' vs 'series-a').\n"
        "2. Do NOT add or infer new stages that are not present in the input list—NO expansions.\n"
        "3. Do NOT assume full coverage of all investment stages—return only what is present in the input list.\n"
        "4. If both an umbrella and all components are present (e.g. 'seed' and 'pre-seed'), keep both unless one is a direct duplicate of the other.\n"
        "5. Do NOT add stages based on common sense, typical VC practices, or background knowledge. If only 'Series A' is present, do NOT add 'Series B', etc.\n\n"
        "Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n"
        f"{json.dumps(unique_normalized)}"
        )


    elif field_name == "investment_amount":
        prompt = (
        "You are deduplicating investment-amount values extracted from venture capital websites.\n"
        "Rules:\n"
        "1. Normalize all units (e.g., k=thousand, m=million, $250k = $0.25m).\n"
        "2. If two entries represent the same numeric range (e.g., '$250k-$1m' and '$250,000-$1 million'), keep only the first.\n"
        "3. If a single value (e.g., '$30m') falls within a range (e.g., '$15-30m'), drop the single value and keep the range.\n"
        "4. If two ranges overlap, keep only the broadest range that covers all values.\n"
        "5. Keep non-overlapping values/ranges separate.\n"
        "6. Do NOT invent, infer, or expand values. Only use items from the INPUT list.\n"
        "Examples:\n"
        "INPUT: ['$250k-$1m', '$250,000-$1 million', '$30m', '$15-30m', '$2m-$5m']\n"
        "OUTPUT: ['$250k-$1m', '$15-30m', '$2m-$5m']\n\n"
        f"Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n{json.dumps(unique_normalized)}"
    )

    elif field_name == "disease_focus":
        prompt = (
            "You are deduplicating values for the field 'disease_focus'.\n\n"
            "RULES:\n"
            "1. If two or more items differ *only* by qualifiers in "
            "{emerging, neglected, high-burden, pandemic}, merge them into a single entry "
            "and concatenate qualifiers with '/'. Example: "
            "['emerging infectious diseases', 'high-burden infectious diseases'] → "
            "'emerging/high-burden infectious diseases'.\n"
            "2. For umbrellas of the form 'X and Y health': if the umbrella and either "
            "'X health' or 'Y health' exist, KEEP ONLY the umbrella (e.g. keep "
            "'maternal and child health', drop 'child health'). Same for "
            "'reproductive and sexual health' vs 'sexual health'.\n"
            "3. Do NOT merge otherwise distinct diseases (HIV vs tuberculosis).\n\n"
            "Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n"
            f"{json.dumps(unique_normalized)}"
        )
    
    elif field_name in (
            "requires_startup_revenue_generation", 
            "therapeutic_investor",
            "equity_investor"):
        normalized = [str(v).strip().lower() for v in _prep_input(values)]
        
        if "true" in normalized:
            return ["true"]
        elif "false" in normalized:
            return ["false"]
        else:
            return []

    else:  # generic prompt
        prompt = (
            f"You are deduplicating values for the field '{field_name}'. "
            "Merge only true duplicates (case, spacing, plural/singular, typos). "
            "Do NOT merge distinct categories even if related. "
            "For umbrellas like 'cell and gene therapy', drop the umbrella if all "
            "components exist. Do NOT hallucinate and add any additional information. \n\n"
            f"Return a JSON list of deduplicated values containing only items drawn from the INPUT list; do NOT invent or expand.\n{json.dumps(unique_normalized)}"
        )

    # ── LLM call and parse ─────────────────────────────────
    content = _chat(prompt)
    print(f"DEBUG: LLM response for field '{field_name}': {content}")
    
    try:
        match = re.search(r"\[.*\]", content, re.S)
        if match:
            data = json.loads(match.group(0))
            
            normalized_set = {normalize_value(v) for v in unique_normalized}
            filtered = [
                v for v in data
                if normalize_value(v) in normalized_set and v and v.strip()
            ]
            print(f"DEBUG: Filtered result for '{field_name}': {len(filtered)} items")
            return filtered
    except Exception as e:
        print(f"LLM deduplication parse error: {e}")

    # Fallback: return initial unique list
    return unique_normalized

# ── quick CLI test ─────────────────────────────────────────
if __name__ == "__main__":
    test = [
        "emerging infectious diseases",
        "high-burden infectious diseases",
        "infectious diseases",
        "child health",
        "maternal and child health",
        "sexual health",
        "reproductive and sexual health",
    ]
    print(deduplicate_with_llm("disease_focus", test))