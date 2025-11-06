import re
import openai
import json
import numpy as np

class EmbeddingSim:
    def __init__(self, device=None):
        pass  # Embedding models removed

    # SIMILARITY & NUMERIC
    @staticmethod
    def cosine_similarity(vec1, vec2):
        return 0.0  # No-op, embeddings removed

    @staticmethod
    def parse_amount_regex(amount_str):
        if not amount_str:
            return None
        s = amount_str.lower().replace(',', '').replace('$', '').strip()
        if '-' in s or 'to' in s:
            parts = re.split(r'-|to', s)
            nums = [EmbeddingSim.parse_amount_regex(x.strip()) for x in parts if x.strip()]
            nums = [x for x in nums if x is not None]
            if nums:
                return float(np.mean(nums))
            else:
                return None
        if 'billion' in s or 'b' in s:
            num = float(re.findall(r'[\d.]+', s)[0])
            return num * 1_000_000_000
        if 'million' in s or 'm' in s:
            num = float(re.findall(r'[\d.]+', s)[0])
            return num * 1_000_000
        found = re.findall(r'[\d.]+', s)
        if found:
            return float(found[0])
        return None

    @staticmethod
    def gpt_parse_amount(amount_str):
        prompt = f"""Extract and return ONLY the total USD amount (as a number, no commas or symbols, and as a float) from this funding string:
        "{amount_str}"
If it is a range, return the midpoint.
If it is ambiguous, make your best reasonable guess.
Return only the number."""
        response = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        extracted = response.choices[0].message.content.strip()
        try:
            value = float(extracted)
            return value
        except Exception:
            return None

    @classmethod
    def best_parse_amount(cls, amount_str):
        num = cls.parse_amount_regex(amount_str)
        if num is not None:
            return num
        return cls.gpt_parse_amount(amount_str)

    @staticmethod
    def numeric_similarity(a1, a2, max_log_range=3):
        a1 = max(a1, 1)
        a2 = max(a2, 1)
        log_diff = abs(np.log10(a1) - np.log10(a2))
        similarity = 1 - (log_diff / max_log_range)
        return max(0.0, min(1.0, similarity))

    # ─────────────────────────  GPT PRESCAN (simple)  ─────────────────────────
    @staticmethod
    def gpt_prescan(startup: dict, vc: dict) -> dict:
        
        # Extract data safely to avoid format specifier issues
        startup_drug_modality = str(startup.get('Drug Modality', '')).replace('%', 'percent').replace('$', '')
        startup_disease_focus = str(startup.get('Disease Focus', '')).replace('%', 'percent').replace('$', '')
        startup_investment_stage = str(startup.get('Investment Stage', '')).replace('%', 'percent').replace('$', '')
        startup_geography = str(startup.get('Geography', '')).replace('%', 'percent').replace('$', '')
        startup_investment_amount = str(startup.get('Investment Amount', '')).replace('%', 'percent').replace('$', '')
        
        vc_drug_modality = str(vc.get('Drug Modality', '')).replace('%', 'percent').replace('$', '')
        vc_drug_modality_portfolio = str(vc.get('Drug Modality (Portfolio)', '')).replace('%', 'percent').replace('$', '')
        vc_disease_focus = str(vc.get('Disease Focus', '')).replace('%', 'percent').replace('$', '')
        vc_disease_focus_portfolio = str(vc.get('Disease Focus (Portfolio)', '')).replace('%', 'percent').replace('$', '')
        vc_investment_stage = str(vc.get('Investment Stage', '')).replace('%', 'percent').replace('$', '')
        vc_geography = str(vc.get('Geography', '')).replace('%', 'percent').replace('$', '')
        vc_geography_portfolio = str(vc.get('Geography (Portfolio)', '')).replace('%', 'percent').replace('$', '')
        vc_investment_amount = str(vc.get('Investment Amount', '')).replace('%', 'percent').replace('$', '')

        prompt = f"""
You are an expert assistant evaluating whether a venture capital firm's investment criteria are COMPATIBLE with a startup's needs. Assess compatibility SEMANTICALLY, not just by keywords, for the five main categories below. For every category, compare semantically and carefully review every possible match between the startup and VC preferences, including portfolio fields where present.

───────────────────────── RULES ─────────────────────────
1. **Blank = True** - If the VC value is blank / "n/a" / missing, treat that field as automatically **true**
2. **Vertical filter (therapeutics only)**
    Ignore any criterion tied **solely** to non-therapeutic verticals (SaaS, fintech, med-device, diagnostics, digital-health, AI tools, etc.).
    Accept only (a) clauses that explicitly refer to therapeutics / drug development / biotech **or** (b) firm-wide clauses with **no** vertical qualifier.
3. **Prioritize VC stated criteria** over portfolio data when both exist, but accept either as sufficient for a match. 

───────────────────────── CATEGORIES ─────────────────────────

1. **Drug Modality:**  
   • Compare BOTH the VC's Drug Modality and Drug Modality (Portfolio) fields to the startup's Drug Modality, using all matching guidelines below.
   • If EITHER the VC field OR the Portfolio field matches the startup, return true for Drug Modality.
   • In the reasoning bullet, specify which field(s) matched (VC, Portfolio, or both).
   **THERAPEUTIC MODALITY TAXONOMY & MATCHING RULES:**
   A. Exact & Direct Matches:
   - Identical terms (case-insensitive), plural/singular variations, and common abbreviations.
   B. 12-Category Drug Modality Taxonomy (match parent to any child, and recognize cross-category relationships):
   1. Small Molecules: conventional small molecules, covalent inhibitors, allosteric modulators, PROTACs, molecular glues
   2. Genetic Medicines: gene therapies (viral/non-viral), gene editing (CRISPR/Cas9, base/prime editors, TALENs, ZFNs), RNA therapies (mRNA, siRNA, miRNA, ASOs, saRNA, circRNA)
   3. Biologics: monoclonal antibodies (mAbs), bispecific antibodies, ADCs, fusion proteins, therapeutic enzymes, Fc-modified antibodies
   4. Peptides & Proteins: peptide therapeutics, protein replacement, hormone therapies (insulin, EPO, GH)
   5. Cell Therapies: CAR-T, TCR-T, CAR-NK, iPSC-derived, stem cell therapies
   6. Immunotherapies: checkpoint inhibitors, cytokine therapies, oncolytic viruses, tumor vaccines, immune agonists, engineered T cell/APC platforms
   7. Microbiome-Based Therapies: live biotherapeutics, postbiotics, FMT
   8. Neuromodulators: botulinum toxin, neuropeptides, vagus nerve stimulation, bioelectronic medicine, neural pathway modulators
   9. Oncolytic Viruses: engineered viruses targeting cancer, immunostimulatory viruses
   10. Vaccines: prophylactic (traditional, mRNA, viral vector), therapeutic cancer vaccines, DNA/RNA-based vaccines
   11. Antibiotics / Anti-Infectives: traditional antibiotics, novel agents, antimicrobial peptides, phage therapy, host-directed therapies
   12. Other / Miscellaneous: exosome-based, nanoparticle delivery, protein stabilization, supramolecular drugs, digital therapeutics (if drug-combo), radioimmunotherapy, photodynamic therapy, gene circuit-based therapies
   C. Cross-Category Semantic Relationships:
   Recognize when different categories have semantic overlap:
   - "Gene therapy" ≈ "gene editing" (both genetic medicines)
   - "Protein therapeutics" ≈ "biologics" (protein-based drugs)
   - "Cell therapy" ≈ "immunotherapy" (when discussing CAR-T, engineered cells)
   - "Oncolytic viruses" can match either "immunotherapy" or "gene therapy"
   - "Antibodies" ≈ "immunotherapy" (mAbs often used in immune modulation)
   - "RNA therapeutics" ≈ "gene therapy" (both nucleic acid-based)
   D. Modality-Agnostic Matching:
   • If VC states "modality-agnostic", "platform-agnostic", "across all modalities", or "technology-agnostic" = match to ANY startup modality
   • Broad terms like "therapeutics", "drug development", "pharma" without modality qualifier = match all
   E. Exclusions (Non-Therapeutic):
   • EXCLUDE and return FALSE for matches with: diagnostics, medical devices, CRO services, AI tools, SaaS platforms, digital health apps, biomarkers (unless therapeutic), research tools
   • Focus only on molecules/biologics intended for therapeutic intervention
   F. Mismatch Scenarios (Return FALSE):
   • Explicit contradictions: startup "gene therapy" vs VC "small molecules only"
   • Mutually exclusive focus: startup "biologics" vs VC "synthetic chemistry only"
   • Technology conflicts: startup "viral delivery" vs VC "non-viral approaches only"
   G. Default to TRUE Philosophy:
   • If there's ANY plausible scientific or commercial connection between modalities, return TRUE
   • Only return FALSE for clear, explicit mismatches or non-therapeutic categories
   • When in doubt, favor matching - better to over-include than miss strategic opportunities

2. **Disease Focus**  
   • Compare BOTH the VC's Disease Focus and Disease Focus (Portfolio) fields to the startup's Disease Focus, using all matching guidelines below.  
   • If EITHER the VC field OR the Portfolio field matches the startup, return true for Disease Focus.  
   • In the reasoning bullet, specify which field(s) matched (VC, Portfolio, or both).
   **THERAPEUTIC DISEASE TAXONOMY & MATCHING GUIDELINES:**
   A. Disease Taxonomy & Hierarchy (parent-child and cross-category relationships):
   1. Infectious Diseases: viral (HIV, Hepatitis, COVID-19, RSV, etc.), bacterial (TB, MRSA, C. diff), fungal, parasitic, AMR, vaccines
   2. Cancer / Oncology: solid tumors (lung, breast, colorectal, etc.), hematologic (AML, ALL, NHL, MM), rare/pediatric cancers, immuno-oncology, precision oncology
   3. Cardiovascular Diseases: CAD, heart failure, hypertension, arrhythmias, stroke, congenital heart defects
   4. Neurological Diseases: neurodegenerative (Alzheimer’s, Parkinson’s), MS, epilepsy, neuromuscular, TBI, rare CNS
   5. Genetic & Rare Diseases: monogenic (CF, sickle cell), chromosomal, ultrarare, undiagnosed, expanded carrier, overlaps with metabolic/neurological/pediatric/hematologic
   6. Autoimmune & Inflammatory: RA, SLE, psoriasis, IBD, T1D, celiac, vasculitis, autoinflammatory syndromes
   7. Respiratory: asthma, COPD, ILD, CF, ARDS, respiratory infections
   8. Gastrointestinal: IBD, IBS, GERD, GI cancers, liver diseases (NAFLD/NASH, hepatitis), pancreatitis
   9. Hematological: anemias, thalassemias, hemophilia, MPNs, bone marrow failure, transfusion complications
   10. Musculoskeletal: osteoarthritis, RA, osteoporosis, muscular dystrophies, tendon/ligament, sarcopenia
   11. Dermatological: psoriasis, eczema, vitiligo, acne, rosacea, alopecia, skin infections/cancers
   12. Psychiatric & Behavioral: MDD, bipolar, anxiety, schizophrenia, PTSD, ADHD, SUD, ASD
   13. Ophthalmic: AMD, diabetic retinopathy, glaucoma, retinitis pigmentosa, dry eye, inherited retinal, uveitis
   14. Endocrine & Metabolic: diabetes, obesity, hyperlipidemia, thyroid/adrenal, inborn errors, GH deficiency
   15. Reproductive & Women’s Health: PCOS, endometriosis, fibroids, infertility, menstrual/menopause, sexual health, gynecologic cancers
   16. Pediatric: congenital, neonatal, rare pediatric, pediatric cancers, developmental, pediatric autoimmune/neurological

   B. Hierarchy & Inclusion Guidelines:
   • Parent umbrellas match any child indication (e.g., “rare diseases” covers “SMA”; “oncology” covers “leukemia”).
   • Recognize cross-category and overlapping relationships (e.g., “autoimmune” ≈ “inflammatory”; “genetic” overlaps with “pediatric” and “neurological” in some cases).
   • True when the VC lists:
     – The exact indication (e.g., “spinal muscular atrophy”).
     – A parent umbrella (see above).
     – A recognised synonym or abbreviation (e.g., “NASH” ≈ “non-alcoholic steatohepatitis”).
   • If the VC focus is “indication agnostic / disease agnostic”, treat as **true**.
   • **Default to TRUE Philosophy:** If there is any plausible clinical, scientific, or commercial connection between the startup’s disease and the VC’s focus, return TRUE. Only return FALSE for clear, explicit mismatches or when the startup’s disease is NOT inside any VC umbrella. When in doubt, favor matching—inclusion is preferred over exclusion.

3. **Investment Stage**  
   • Normalise common phrases → canonical buckets:  
       “pre-seed / angel” → “pre-seed”; “growth equity / expansion” → “late-stage”;  
       “IPO participation / public markets” → “public equity”.  
   • **STAGE HIERARCHY TABLE (use as reference for all matching and context):**
     1. Pre-seed / Angel
     2. Seed
     3. Series A
     4. Series B
     5. Series C
     6. Growth / Late-stage
     7. Pre-IPO
     8. Public
   • **CLINICAL/REGULATORY STAGE MAPPING (use as reference for all matching and context):**
     - "Preclinical" ≈ Pre-seed / Seed
     - "IND-enabling" ≈ Seed / Series A
     - "Phase I/II/III" ≈ Series B / Series C / Growth
     - "Approved / Commercial" ≈ Late-stage / Public
   • Instruct the LLM: Use the above Stage Hierarchy Table and Clinical/Regulatory Stage Mapping as reference for all matching and context. When comparing startup and VC stages, always map clinical/regulatory language to the appropriate funding stage(s) using these tables.
   • Ranges match if the startup’s stage sits *anywhere inside* (“Seed–Series B” covers Series A).  
   • “Early-stage VC” matches *pre-seed → Series A*; “late-stage” matches Series C +.  
   • Mixed debt+equity sentences: use the equity portion only. Pure debt clauses are ignored.  
   • If VC gives a **minimum stage** and the startup is earlier (e.g. “Series B+” vs Series A) ⇒ **false**.  

4. **Geography**  
   • Compare BOTH the VC's Geography and Geography (Portfolio) fields to the startup's Geography, using all matching guidelines below.  
   • If EITHER the VC field OR the Portfolio field matches the startup, return true for Geography.  
   • In the reasoning bullet, specify which field(s) matched (VC, Portfolio, or both).
   • **GEOGRAPHIC PRIORITIZATION & SPECIFICITY ANALYSIS**:
     – **Specificity Hierarchy**: When evaluating matches, recognize that geographic specificity creates strategic value. A VC with demonstrated investment in "Massachusetts" biotech is more strategically relevant to a Boston startup than a VC with only "United States" criteria.
     – **Portfolio Data Primacy**: When a startup specifies a specific region/state/city (e.g., "Massachusetts", "California", "Ontario", "Cambridge"), prioritize VCs whose **Portfolio** field shows actual investment in that specific region over VCs with only broad country-level stated criteria.
     – **Strategic Reasoning**: VCs with regional portfolio concentration often have deeper ecosystem knowledge, local networks, regulatory expertise, and operational support capabilities specific to that geography.
     – **Dual Acceptance Model**: Still accept both specific and broad matches as valid, but indicate the specificity level in your reasoning (e.g., "Strong regional alignment via portfolio" vs "Broad country-level compatibility").
     – **Examples of Prioritization**:
       * Startup: "Massachusetts" → VC Portfolio: "Massachusetts, Boston area" > VC Stated: "United States"
       * Startup: "San Francisco" → VC Portfolio: "Bay Area, California" > VC Stated: "North America"
       * Startup: "Ontario" → VC Portfolio: "Toronto, Ontario" > VC Stated: "Canada"
     – Exact country match.  
     – Country inside VC region (“France” ∈ “Europe”; “California” ∈ “USA”).  
     – VC says “global / worldwide / geography-agnostic”.  
     – **If either the VC or the startup specifies a city and the other specifies the corresponding country, count it as a match. This applies in both directions: a VC investing in a country matches a startup in a city within that country, and a VC investing in a city matches a startup in that city or country.**
   • If VC geography is blank, treat as **true**.

5. **Investment Amount**  
   • Parse VC check sizes as USD even when “$” isn’t stated if the currency context is obvious.  
   • Ranges: “$5–$300 m” → lower = 5 m, upper = 300 m.  
   • “Up to $X” → upper = X, lower = 0.  
   • “At least $Y” → lower = Y, upper = ∞ — startup ask must be ≥ Y.  
   • Startup ask fits when: **lower VC ≤ ask ≤ upper VC**.  
   • Ignore: total fund size, cumulative “invested to date”, portfolio-raise figures, currency without magnitude (“multi-million”).  
   • If VC amount is blank ⇒ **true**; if startup gives no ask, assume it *needs* VC’s stated range.  
   • Conflicting or unclear units/currencies ⇒ **false**.

───────────────────────── OUTPUT ─────────────────────────
For each field, return true/false. Only return true for "overall_match" if ALL five are true.
Respond with VALID JSON only (no markdown, comments, or extra keys), using lower-case true/false literals.

───────────────────────── DATA ─────────────────────────
Startup:
  Drug Modality      : {startup_drug_modality}
  Disease Focus      : {startup_disease_focus}
  Investment Stage   : {startup_investment_stage}
  Geography          : {startup_geography}
  Investment Amount  : {startup_investment_amount}

VC:
  Drug Modality                : {vc_drug_modality}
  Drug Modality (Portfolio)    : {vc_drug_modality_portfolio}
  Disease Focus                : {vc_disease_focus}
  Disease Focus (Portfolio)    : {vc_disease_focus_portfolio}
  Investment Stage             : {vc_investment_stage}
  Geography                    : {vc_geography}
  Geography (Portfolio)        : {vc_geography_portfolio}
  Investment Amount            : {vc_investment_amount}

RETURN JSON EXAMPLE
{{
  "Drug Modality": true/false,
  "Disease Focus": true/false,
  "Investment Stage": true/false,
  "Geography": true/false,
  "Investment Amount": true/false,
  "overall_match": true/false
}}
""".strip()

        resp = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.choices[0].message.content

        # ---- parse JSON block ----
        try:
            json_str = raw[raw.find("{"): raw.rfind("}") + 1]
            result   = json.loads(json_str)
        except Exception:
            return {"overall_match": False}

        # coerce strings like "true"/"false" → real bools
        def _to_bool(v): return str(v).strip().lower() in {"true", "yes", "1"}
        fields = ["Drug Modality", "Disease Focus",
                  "Investment Stage", "Geography", "Investment Amount"]
        for k in fields:
            if k in result:
                result[k] = _to_bool(result[k])
        result["overall_match"] = all(result.get(k, False) for k in fields)
        # Only include reasoning if overall_match is True
        if not result["overall_match"]:
            result.pop("reasoning", None)
        else:
            if "reasoning" not in result:
                result["reasoning"] = []
        return result

    # ─────────────────────────  GPT PORTFOLIO ANALYSIS PROMPT  ─────────────────────────
    @staticmethod
    def get_portfolio_analysis_prompt(startup: dict, vc: dict) -> str:
        """
        Generate a GPT prompt for portfolio analysis that compares VC's stated criteria
        with their actual portfolio data to determine if they would be a good match for the startup.
        
        This function only returns the prompt string - it does NOT make any API calls.
        """

        prompt = f"""
You are an expert venture capital analyst evaluating whether a VC firm's actual investment portfolio aligns with their stated investment criteria, and whether this portfolio analysis suggests they would be a good match for a specific startup.

───────────────────────── OVERVIEW ─────────────────────────

Provide a quantitative and qualitative assessment using four scoring dimensions:
1. Drug Modality Portfolio Score (0-100)
2. Disease Focus Portfolio Score (0-100)
3. Geography Portfolio Score (0-100)
4. Overall Portfolio Alignment Score (0-100)

Use the detailed scoring methodology below for each section, and return the specified JSON output format at the end.

───────────────────────── MATCHING GUIDELINES ─────────────────────────

**Drug Modality Matching:**
- Match when exact, plural/singular, close synonym, or umbrella→child (e.g., "gene editing" ≈ "gene therapy"; "biologics" ⊃ "antibodies")
- If the VC is listed as "Modality-agnostic / across all modalities" = match to every startup modality
- If there's a clear mismatch (e.g., startup: "gene therapy", VC: "small molecules only"), return false
- Exclude non-therapeutic buckets (diagnostics, devices, CRO services, AI tools)
- Utilize a conservative approach: It is better to miss a potential match than to create false positives
- Semantic matches (e.g., "Gene therapy" ≈ "Gene editing", "Biologics" ⊃ "Antibodies")
- Umbrella terms (e.g., "Biologics" covers "mAbs", "Therapeutics" covers specific modalities)
- Cross-platform similarities (e.g., "Cell therapy" and "Gene therapy" both work with cellular mechanisms)

**Disease Focus Matching:**
- Exact indication matches (e.g., "Alzheimer's disease" = "Alzheimer's")
- Therapeutic area matches (e.g., "Oncology" ⊃ "Breast cancer", "Rare diseases" ⊃ "SMA")
- A parent umbrella matches (e.g., "Neurological disorders" ⊃ "Parkinson's disease")
- Recognized synonyms or abbreviations (e.g., "NASH" ≈ "non-alcoholic steatohepatitis")
- If the focus is "indication/disease agnostic", treat as true
- Related conditions (e.g., "Autoimmune diseases" ≈ "Inflammatory diseases")
- Disease mechanism overlap (e.g., "Neurodegeneration" covers multiple brain disorders)
- Adjacent therapeutic areas (e.g., "Oncology" and "Immunology" often overlap)

**Geography Matching:**
- Exact location matches (e.g., "United States" = "USA")
- Regional inclusion (e.g., "North America" ⊃ "USA", "Europe" ⊃ "Germany")
- City-country relationships (e.g., "Boston" ∈ "United States")
- Country inside VC region ("France" ∈ "Europe"; "California" ∈ "USA")
- VC says "global / worldwide / geography-agnostic"
- If either the VC or the startup specifies a city and the other specifies the corresponding country, count it as a match. This applies in both directions: a VC investing in a country matches a startup in a city within that country, and a VC investing in a city matches a startup in that city or country
- Global/worldwide designations match everything

**GEOGRAPHIC PRIORITIZATION & SPECIFICITY ANALYSIS:**
- **Specificity Hierarchy**: When evaluating matches, recognize that geographic specificity creates strategic value. A VC with demonstrated investment in "Massachusetts" biotech is more strategically relevant to a Boston startup than a VC with only "United States" criteria
- **Portfolio Data Primacy**: When a startup specifies a specific region/state/city (e.g., "Massachusetts", "California", "Ontario", "Cambridge"), prioritize VCs whose Portfolio field shows actual investment in that specific region over VCs with only broad country-level stated criteria
- **Strategic Reasoning**: VCs with regional portfolio concentration often have deeper ecosystem knowledge, local networks, regulatory expertise, and operational support capabilities specific to that geography
- **Dual Acceptance Model**: Still accept both specific and broad matches as valid, but indicate the specificity level in your reasoning (e.g., "Strong regional alignment via portfolio" vs "Broad country-level compatibility")
- **Examples of Prioritization**:
  * Startup: "Massachusetts" → VC Portfolio: "Massachusetts, Boston area" > VC Stated: "United States"
  * Startup: "San Francisco" → VC Portfolio: "Bay Area, California" > VC Stated: "North America"
  * Startup: "Ontario" → VC Portfolio: "Toronto, Ontario" > VC Stated: "Canada"

───────────────────────── SCORING METHODOLOGY ─────────────────────────

**1. Drug Modality Portfolio Score (0-100)**
- 90-100: Specialist focus - 50%+ of portfolio or >5 companies in exact/highly related modalities
- 70-89: Strong alignment - 35-40% of portfolio or 3-5 companies in same modality category
- 50-69: Moderate alignment - Some portfolio overlap in broader category
- 0-49: Limited alignment - Minimal portfolio evidence in related modalities

Modality Scoring Factors:
- Portfolio Concentration: Percentage of companies in related modalities
- Recent Investment Pattern: Weight last 3 years more heavily (2x multiplier)
- Platform vs Application: Platform companies score higher for related applications
- Technology Evolution: Credit VCs evolving within same category (small molecules → PROTACs)
- Stage Consistency: Do they follow modality companies through multiple rounds?

**2. Disease Focus Portfolio Score (0-100)**
- 90-100: Therapeutic area specialist - 50%+ of portfolio or >5 companies in startup's disease area
- 70-89: Strong therapeutic focus - 35-40% of portfolio or 3-5 companies in same/adjacent areas
- 50-69: Moderate alignment - Some investments in broader therapeutic category
- 0-49: Limited alignment - Minimal focus on startup's therapeutic area

Disease Scoring Factors:
- Therapeutic Area Concentration: Depth in specific disease areas
- Cross-Modal Consistency: Same disease focus across different drug modalities
- Clinical Development Support: Evidence of following companies through trials
- Adjacent Areas: Investment in related therapeutic areas (oncology + immunology overlap)
- Rare Disease Premium: Higher scores for demonstrated rare disease investment patterns

**3. Geography Portfolio Score (0-100)**
- 90-100: Regional specialist - 50%+ of portfolio or >5 companies in startup's region
- 70-89: Strong regional focus - 35-40% of portfolio or 3-5 companies in startup's area
- 50-69: Moderate alignment - Some investments in startup's region or broader area
- 0-49: Limited alignment - Minimal presence in startup's geographic region

Geography Scoring Factors:
- Regional Concentration: Percentage of portfolio in specific biotech hubs
- Ecosystem Depth: Multiple investments in same city/state showing local expertise
- Cross-Border Capability: Demonstrated ability to invest internationally if needed
- Hub Strategy: Focus on major biotech clusters (Boston, Bay Area, San Diego, etc.)
- Regulatory Expertise: Portfolio evidence of navigating local regulatory environments

**4. Overall Portfolio Alignment Score (0-100)**
Composite score reflecting holistic startup-VC fit based on portfolio analysis

Calculation Method:
Overall Score = (Drug Modality * 0.3) + (Disease Focus * 0.3) + (Geography * 0.40)

Adjustment Factors:
- Portfolio Data Quality Bonus/Penalty: +/-10 points based on portfolio comprehensiveness
- Investment Pattern Consistency: +/-5 points for clear vs scattered investment thesis

───────────────────────── MATCH THRESHOLDS ─────────────────────────

Overall Portfolio Alignment Interpretation:
- 85-100: Excellent Match - Portfolio strongly indicates VC would be interested
- 70-84: Good Match - Clear alignment with some minor gaps
- 55-69: Moderate Match - Mixed signals, worth exploring but not obvious fit
- 40-54: Weak Match - Limited alignment, low probability of interest
- 0-39: Poor Match - Portfolio suggests low likelihood of investment

───────────────────────── SCORING ADVANTAGES ─────────────────────────

**1. Dimensional Clarity**: See exactly where alignment is strong vs weak
- High modality + low geography = great tech fit, wrong location
- High disease + low modality = right market, wrong approach

**2. Strategic Prioritization**:
- 95 geography + 90 modality + 60 disease = prioritize and emphasize local network value
- 90 disease + 85 modality + 40 geography = emphasize remote/virtual collaboration capability

**3. Pitch Optimization**:
- High modality score → lead with technology differentiation
- High disease score → lead with market opportunity and clinical need
- High geography score → emphasize local ecosystem benefits

**4. Risk Assessment**:
- Consistent high scores across all dimensions = high confidence match
- One very low score = identify specific risk to address
- All moderate scores = decent fit but not compelling

**5. Portfolio Quality Weighting**:
- Rich portfolio data (15+ companies) = high confidence in scores
- Sparse portfolio data (under 5 companies) = lower confidence, flag uncertainty
- New fund = rely more on stated criteria, indicate early-stage assessment

For Startups: Clear prioritization of VC outreach based on multi-dimensional fit
For VCs: Better deal flow filtering and self-awareness of investment patterns
For Platforms: Rich matching data for improved recommendations and success rates
For Analysis: Detailed insights into VC behavior patterns and market dynamics

──────────────────────── OUTPUT REQUIREMENTS ─────────────────────────

Provide the following values:
**1. drug_modality_portfolio_score**: 0-100 score based on the modality scoring criteria
**2. disease_focus_portfolio_score**: 0-100 score based on the disease focus scoring criteria
**3. geography_portfolio_score**: 0-100 score based on the geography scoring criteria
**4. overall_portfolio_alignment_score**: Weighted average of the three portfolio scores
**5. verified_with_portfolio_analysis**: true/false boolean (true ONLY if overall_portfolio_alignment_score >= 60 AND drug_modality_portfolio_score >= 20 AND disease_focus_portfolio_score >= 20 AND geography_portfolio_score >= 20, otherwise false)
**6. scoring_breakdown**: A detailed breakdown of how each score was calculated, including specific examples from the portfolio that support the scores:
- **overall_assessment**: Concise strategic assessment in EXACTLY this format with 5 numbered sections (keep each section to 2-3 sentences max):
  "1) Portfolio-Investment Pattern Analysis: [Describe VC's investment concentration by modality percentages, geographic distribution percentages, and disease area breakdown. Note whether pattern shows focused thesis or diversified approach.]
  2) Strategic Alignment Strengths: [Identify top 2-3 alignment strengths (exact modality match, regional presence, disease expertise). Explain specific value these bring (e.g., local network effects, domain expertise).]
  3) Key Limitations and Caveats: [State principal gaps - limited disease exposure percentage, missing portfolio data, stage misalignment. Include specific concerns about domain expertise or operational support.]
  4) Overall Strategic Fit Summary: [One sentence verdict (strong/good/moderate/weak fit) with primary drivers. One sentence on what startup must demonstrate to convert interest.]"
  
  
───────────────────────── JSON RESPONSE FORMAT ─────────────────────────

Respond with VALID JSON only (no markdown, comments, or extra keys):

{{
  "drug_modality_portfolio_score": 88,
  "disease_focus_portfolio_score": 72,
  "geography_portfolio_score": 95,
  "overall_portfolio_alignment_score": 85,
  "verified_with_portfolio_analysis": true,
  "scoring_breakdown": {{
    "overall_assessment": "Portfolio-Investment Pattern Analysis: The VC displays concentrated small-molecule focus (31%) combined with heavy North American presence (89%) and diversified other modalities (biologics 23%, peptides/cell therapies 15%). Disease exposure is distributed with neurology/oncology larger than infectious disease (9%), indicating modality-led thesis with geographic concentration.\n\n2) Strategic Alignment Strengths: Exact modality fit (small molecules) and strong Massachusetts/New York presence provide local operational support and network effects. VC's modern small-molecule platforms (PROTACs, molecular glues) suggest capacity to evaluate sophisticated programs.\n\n3) Key Limitations and Caveats: Limited infectious-disease concentration (9% of portfolio) raises questions about domain expertise. Stage preferences unspecified - if VC prefers Series A, pre-seed fit could be weaker.\n\n4) Positioning Recommendations: Emphasize technical differentiation within small molecules, highlight early translational/efficacy data, stress MA/NY local ties. Demonstrate infectious-disease credibility through advisory board or academic partnerships.\n\n5) Overall Strategic Fit Summary: Good-to-strong strategic fit driven by excellent modality alignment and geographic overlap. Startup must proactively demonstrate infectious-disease domain traction to convert interest into investment."
  }}
}}

───────────────────────── STARTUP & VC DATA ─────────────────────────

Startup:
  Drug Modality      : {startup.get('Drug Modality', 'N/A')}
  Disease Focus      : {startup.get('Disease Focus', 'N/A')}
  Investment Stage   : {startup.get('Investment Stage', 'N/A')}
  Geography          : {startup.get('Geography', 'N/A')}
  Investment Amount  : {startup.get('Investment Amount', 'N/A')}

VC:
  Drug Modality                : {vc.get('Drug Modality', 'N/A')}
  Drug Modality (Portfolio)    : {vc.get('Drug Modality (Portfolio)', 'N/A')}
  Disease Focus                : {vc.get('Disease Focus', 'N/A')}
  Disease Focus (Portfolio)    : {vc.get('Disease Focus (Portfolio)', 'N/A')}
  Investment Stage             : {vc.get('Investment Stage', 'N/A')}
  Geography                    : {vc.get('Geography', 'N/A')}
  Geography (Portfolio)        : {vc.get('Geography (Portfolio)', 'N/A')}
  Investment Amount            : {vc.get('Investment Amount', 'N/A')}

**PORTFOLIO COMPANIES:** {vc.get('Portfolio Companies', 'N/A')}

"""

        
        return prompt
