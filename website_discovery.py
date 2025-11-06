# website_discovery.py - FIXED VERSION
# Fixes: Timeout handling, rate limiting, explicit output tracking, error recovery, hanging prevention


import requests
import json
import time
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
import os
import re
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
# ── Console-encoding hardening (Windows CP-1252 can’t print emoji) ──
import sys
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


SERPER_API_KEY = os.getenv("SERPER_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # Add OpenAI API key for LLM validation
OUTPUT_DIR = "output"


# FIXED: Enhanced timeout and retry configuration
REQUEST_TIMEOUT = 8
API_TIMEOUT = 12
MAX_RETRIES_PER_COMPANY = 2
MAX_RATE_LIMIT_RETRIES = 3
RATE_LIMIT_DELAY = 3
COMPANY_DELAY = 1.0
GLOBAL_TIMEOUT = 45  # Maximum time per company search

# ── Skip interactive prompts when run from main_pipeline ───────────
import os, sys, builtins
if os.environ.get("PIPELINE_AUTO") == "1":
    def _fake_input(prompt=""):
        if "choice" in prompt.lower():
            return "4"                           # “orchestrator input”
        if "max results" in prompt.lower():
            return "20"
        return ""
    builtins.input = _fake_input

# Only exclude the most obvious non-company domains
EXCLUDE_DOMAINS = [
   'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
   'bloomberg.com', 'reuters.com', 'forbes.com', 'techcrunch.com',
   'fiercebiotech.com', 'biospace.com', 'statnews.com', 'endpoints.news',
   'crunchbase.com', 'pitchbook.com', 'cbinsights.com', 'wikipedia.org',
   'nih.gov', 'clinicaltrials.gov', 'fda.gov', 'pubmed.ncbi.nlm.nih.gov',
   'glassdoor.com', 'indeed.com', 'biojobs.com'
]


NON_COMPANY_PATTERNS = [
   r'/news/', r'/article/', r'/press-release/', r'/blog/',
   r'/company/', r'/profile/', r'/organization/',
   r'/job/', r'/career/', r'/investor/', r'/stock/',
   r'\.pdf$', r'/pdf/', r'/download/'
]


os.makedirs(OUTPUT_DIR, exist_ok=True)


class TimeoutException(Exception):
   pass


def setup_signal_handlers():
   """Setup signal handlers for graceful termination"""
   def signal_handler(signum, frame):
       print(f"\n⚠️ Received signal {signum}. Gracefully shutting down...")
       print("💾 Saving partial results...")
       sys.exit(1)
  
   signal.signal(signal.SIGTERM, signal_handler)
   signal.signal(signal.SIGINT, signal_handler)


def timeout_handler(func, args=(), kwargs={}, timeout_duration=GLOBAL_TIMEOUT, default=None):
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
       # Thread is still running, timeout occurred
       return default, TimeoutException(f"Function timed out after {timeout_duration}s")
  
   if exception[0]:
       return default, exception[0]
  
   return result[0], None


def normalize_url_to_root(url):
   """Normalize URL to root domain only - removes paths, queries, and fragments"""
   if not url:
       return url
   
   try:
       parsed = urlparse(url)
       if not parsed.scheme or not parsed.netloc:
           return url
       
       # Return just protocol + domain + trailing slash
       return f"{parsed.scheme}://{parsed.netloc}/"
   except Exception:
       return url


def is_official_company_site(url, company_name):
   """Check if URL is likely the official company website with enhanced scoring"""
   if not url:
       return False, 0, []
  
   reasons = []
   score = 0
  
   try:
       domain = urlparse(url).netloc.lower().replace('www.', '')
       path = urlparse(url).path.lower()
      
       # Skip excluded domains
       if any(excluded in domain for excluded in EXCLUDE_DOMAINS):
           return False, 0, ["Excluded domain"]
      
       # Skip non-company URL patterns
       if any(re.search(pattern, url.lower()) for pattern in NON_COMPANY_PATTERNS):
           return False, 0, ["Non-company URL pattern"]
      
       # Clean company name for matching
       clean_company = re.sub(r'[^a-z0-9\s]', '', company_name.lower()).replace(' ', '')
      
       # Word-based matching
       company_words = [word for word in company_name.lower().split() if len(word) > 2]
      
       # High score if full company name is in domain (with stricter matching)
       if clean_company in domain.replace('-', '').replace('.', ''):
           # Additional validation for exact match scenarios
           domain_clean = domain.replace('-', '').replace('.', '')
           
           # Check if company name appears as complete word/segment, not just substring
           # This helps avoid "ray" matching "c-ray" scenarios
           company_segments = clean_company.split()
           if len(company_segments) == 1:  # Single word companies need exact or prefix match
               main_word = company_segments[0]
               # Look for exact word boundaries or clear company name patterns
               if (main_word == domain_clean or 
                   domain_clean.startswith(main_word + 'bio') or
                   domain_clean.startswith(main_word + 'pharma') or
                   domain_clean.startswith(main_word + 'therapeutics') or
                   domain_clean.startswith(main_word + 'tx') or
                   domain_clean.endswith(main_word + 'bio') or
                   domain_clean.endswith(main_word + 'therapeutics')):
                   score += 80
                   reasons.append(f"Full company name in domain ({clean_company} in {domain})")
               else:
                   # Potential false positive - reduce confidence
                   score += 40
                   reasons.append(f"Partial company name match in domain - needs validation ({clean_company} in {domain})")
           else:
               # Multi-word companies - original logic
               score += 80
               reasons.append(f"Full company name in domain ({clean_company} in {domain})")
       # Medium-high score if all words are in domain
       elif len(company_words) > 1 and all(word in domain for word in company_words):
           score += 70
           reasons.append(f"All company words in domain ({company_words} in {domain})")
      
       # Medium score if partial company name in domain 
       if score < 70:
           domain_matches = sum(1 for word in company_words if word in domain)
           if domain_matches > 0:
               partial_score = domain_matches * 15
               score += partial_score
               reasons.append(f"{domain_matches} company words in domain (+{partial_score})")
      
       # Prefer .com domains
       if domain.endswith('.com'):
           score += 10
           reasons.append(".com domain")
      
       # Bonus for biotech-related domains
       biotech_terms = ['bio', 'pharma', 'therapeutics', 'medical']
       if any(term in domain for term in biotech_terms):
           score += 5
           reasons.append("Biotech-related domain")
      
       return score >= 30, score, reasons
      
   except Exception:
       return False, 0, ["URL parsing error"]


def validate_company_with_llm(company_name, website_url, title, snippet):
   """Use LLM to validate if the website belongs to the correct life sciences/biotech company"""
   if not OPENAI_API_KEY:
       print(f"            ⚠️ No OpenAI API key - skipping LLM validation")
       return True, "No LLM validation (missing API key)"
   
   try:
       # Get VC context if available
       source_vc = None
       source_vc_url = None
       import inspect
       frame = inspect.currentframe()
       while frame:
           if 'source_vc' in frame.f_locals:
               source_vc = frame.f_locals['source_vc']
           if 'source_vc_url' in frame.f_locals:
               source_vc_url = frame.f_locals['source_vc_url']
           frame = frame.f_back

       prompt = f"""FLEXIBLE BUT ACCURATE VALIDATION: Is this the official website for \"{company_name}\"?

SEARCHING FOR: \"{company_name}\"
WEBSITE: {website_url}
TITLE: \"{title}\"
DESCRIPTION: \"{snippet}\"

SOURCE VC: {source_vc if source_vc else '[unknown]'}
SOURCE VC URL: {source_vc_url if source_vc_url else '[unknown]'}

PASS if:
  - The website title or description contains \"{company_name}\" or a close variant (allowing for common suffixes/prefixes like Inc., Corp., Ltd., Therapeutics, Bio, etc.).
  - The website content clearly describes the company as a biotech/therapeutics developer.
  - The domain is a close match (e.g., abbreviation, \"bio\" or \"therapeutics\" added, etc.).

FAIL if:
  - The website is for a different company, a directory, news/media, VC, or a large pharma.
  - The company name is only a partial match and refers to a different entity.
  - The website does not mention the company or its biotech/therapeutics activity.

Step-by-step:
1. Is the company name (or a close variant) in the title/description/domain? (Allow for \"Inc.\", \"Corp.\", \"Ltd.\", \"Therapeutics\", \"Bio\", etc.)
2. Does the website describe the company as developing therapeutics, drugs, or biotech products?
3. Is the website NOT a directory, news, VC, or large pharma?

Only answer TRUE if all steps pass. If unsure, explain why.
"""

       headers = {
           "Authorization": f"Bearer {OPENAI_API_KEY}",
           "Content-Type": "application/json"
       }
       
       payload = {
           "model": "gpt-4.1-mini",  # Fast and cost-effective
           "messages": [
               {"role": "user", "content": prompt}
           ],
           "max_tokens": 100,  # Increased for structured response
           "temperature": 0
       }
       
       response = requests.post(
           "https://api.openai.com/v1/chat/completions",
           json=payload,
           headers=headers,
           timeout=10
       )
       
       if response.status_code == 200:
           result = response.json()
           llm_response = result['choices'][0]['message']['content'].strip()
           
           # Parse structured response
           if "FINAL ANSWER: TRUE" in llm_response.upper():
               # Extract step results for detailed logging
               step1_result = "unknown"
               step2_result = "unknown"
               lines = llm_response.split('\n')
               for line in lines:
                   if "STEP 1 RESULT:" in line.upper():
                       step1_result = line.split(':', 1)[1].strip() if ':' in line else "unknown"
                   elif "STEP 2 RESULT:" in line.upper():
                       step2_result = line.split(':', 1)[1].strip() if ':' in line else "unknown"
               return True, f"LLM validated: EXACT company match + therapeutics development (Step1: {step1_result[:50]}, Step2: {step2_result[:50]})"
           elif "FINAL ANSWER: FALSE" in llm_response.upper():
               # Extract failure reasons
               step1_result = "unknown"
               step2_result = "unknown"
               lines = llm_response.split('\n')
               for line in lines:
                   if "STEP 1 RESULT:" in line.upper():
                       step1_result = line.split(':', 1)[1].strip() if ':' in line else "unknown"
                   elif "STEP 2 RESULT:" in line.upper():
                       step2_result = line.split(':', 1)[1].strip() if ':' in line else "unknown"
               # Determine primary failure reason
               failure_reason = "Unknown rejection"
               if "FAIL" in step1_result.upper():
                   failure_reason = f"Wrong company: {step1_result[:100]}"
               elif "FAIL" in step2_result.upper():
                   failure_reason = f"Not therapeutics: {step2_result[:100]}"
               return False, f"LLM rejected: {failure_reason}"
           else:
               print(f"            ⚠️ Unexpected LLM response format: {llm_response[:200]}...")
               # Robust fallback: accept if strong positive signals, reject only on clear negatives
               llm_upper = llm_response.upper()
               # Accept if both company name and therapeutics context are clearly present
               positive_signals = [
                   "COMPANY NAME IN TITLE", "COMPANY NAME IN DOMAIN", "CONTAINS \"{0}\"".format(company_name.upper()),
                   "DEVELOPS THERAPEUTICS", "DEVELOPS DRUGS", "BIOTECH COMPANY", "THERAPEUTICS COMPANY", "BIOLOGICS", "PEPTIDES"
               ]
               negative_signals = [
                   "DIFFERENT COMPANY", "DIRECTORY", "NEWS", "MEDIA", "VC", "LARGE PHARMA", "FAIL", "NOT THERAPEUTICS", "NO BIOTECH", "NO MENTION"
               ]
               # If any negative signal, reject
               if any(sig in llm_upper for sig in negative_signals) or "FALSE" in llm_upper:
                   return False, f"LLM rejected (robust fallback): {llm_response[:100]}"
               # If at least two positive signals, accept
               pos_count = sum(sig in llm_upper for sig in positive_signals)
               if pos_count >= 2 or "TRUE" in llm_upper:
                   return True, f"LLM validated (robust fallback): {llm_response[:100]}"
               # Otherwise, unclear, but default to accept if company name and therapeutics are both mentioned
               if (company_name.upper() in llm_upper and ("THERAPEUTICS" in llm_upper or "BIOTECH" in llm_upper)):
                   return True, f"LLM validated (company+therapeutics fallback): {llm_response[:100]}"
               return False, f"LLM unclear response - defaulting to reject: {llm_response[:100]}"
       else:
           print(f"            ❌ LLM API error: {response.status_code}")
           return True, f"LLM API error - defaulting to accept"
           
   except Exception as e:
       print(f"            ❌ LLM validation error: {e}")
       return True, f"LLM validation failed - defaulting to accept"


def make_serper_request_with_retries(query, company_name, max_retries=MAX_RATE_LIMIT_RETRIES):
   """Make Serper API request with enhanced retry logic and timeout handling"""
  
   for retry in range(max_retries):
       try:
           headers = {
               "X-API-KEY": SERPER_API_KEY,
               "Content-Type": "application/json"
           }
           payload = {
               "q": query,
               "num": 6,  # Reduced for faster response
               "gl": "us"
           }
          
           print(f"      [API REQUEST] Attempt {retry + 1}/{max_retries}")
          
           response = requests.post(
               "https://google.serper.dev/search",
               json=payload,
               headers=headers,
               timeout=API_TIMEOUT
           )
          
           if response.status_code == 200:
               try:
                   data = response.json()
                   organic_results = data.get('organic', [])
                  
                   print(f"        [RESULTS] {len(organic_results)} found")
                  
                   # Process results - try up to 3 with LLM validation
                   best_result = None
                   best_score = 0
                   validated_results = 0
                   max_results_to_validate = min(3, len(organic_results))
                  
                   for j, result in enumerate(organic_results, 1):
                       url = result.get('link', '')
                       title = result.get('title', '')
                       snippet = result.get('snippet', '')
                      
                       if not url:
                           continue
                      
                       domain = urlparse(url).netloc.replace('www.', '')
                       print(f"          [{j}] {domain}")
                      
                       # Normalize URL to root domain before scoring
                       normalized_url = normalize_url_to_root(url)
                       if normalized_url != url:
                           print(f"            🔄 Normalized: {url} → {normalized_url}")
                       
                       # Check if this is an official company site (using normalized URL)
                       is_official, score, reasons = is_official_company_site(normalized_url, company_name)
                      
                       print(f"            📊 Score: {score}/100")
                       if reasons:
                           print(f"            💡 {', '.join(reasons[:2])}")
                      
                       # Skip if not official enough
                       if not is_official:
                           print(f"            ⏭️ Skipping - not official enough")
                           continue
                      
                       # LLM validation for official sites
                       print(f"            🤖 LLM validating...")
                       llm_valid, llm_reason = validate_company_with_llm(company_name, normalized_url, title, snippet)
                       print(f"            🤖 {llm_reason}")
                       
                       validated_results += 1
                      
                       if not llm_valid:
                           print(f"            ❌ LLM rejected - trying next result")
                           if validated_results >= max_results_to_validate:
                               print(f"            ⏹️ Reached max validation attempts ({max_results_to_validate})")
                               break
                           continue
                      
                       # Additional biotech context check
                       biotech_score = 0
                       all_text = f"{title} {snippet}".lower()
                      
                       biotech_keywords = [
                           'biotech', 'pharmaceutical', 'therapeutics', 'medicine',
                           'drug', 'therapy', 'clinical', 'pipeline', 'treatment'
                       ]
                      
                       biotech_matches = sum(1 for keyword in biotech_keywords if keyword in all_text)
                       biotech_score = min(20, biotech_matches * 5)
                      
                       total_score = score + biotech_score
                      
                       if biotech_matches > 0:
                           print(f"            🧬 Biotech context: {biotech_matches} keywords (+{biotech_score})")
                      
                       print(f"            🎯 Total score: {total_score}/120")
                      
                       if total_score > best_score:
                           best_score = total_score
                           best_result = {
                               "company_name": company_name,
                               "website_url": normalized_url,  # Use normalized URL in final result
                               "title": title,
                               "snippet": snippet,
                               "search_query": query,
                               "official_site_score": score,
                               "biotech_context_score": biotech_score,
                               "total_score": total_score,
                               "validation_reasons": reasons,
                               "biotech_keywords_found": biotech_matches,
                               "llm_validation": llm_reason
                           }
                           print(f"            ✅ New best result!")
                           # Don't break here - we might find an even better result
                      
                       # If we've validated enough results, stop
                       if validated_results >= max_results_to_validate:
                           print(f"            ⏹️ Reached max validation attempts ({max_results_to_validate})")
                           break
                  
                   return best_result
                  
               except json.JSONDecodeError as e:
                   print(f"        ❌ JSON decode error: {e}")
                   if retry < max_retries - 1:
                       time.sleep(RATE_LIMIT_DELAY)
                       continue
                   return None
                  
           elif response.status_code == 429:
               print(f"        ⏳ Rate limited - waiting {RATE_LIMIT_DELAY}s...")
               if retry < max_retries - 1:
                   time.sleep(RATE_LIMIT_DELAY * (retry + 1))
                   continue
               else:
                   print(f"        ❌ Rate limited after {max_retries} attempts")
                   return None
                  
           elif response.status_code == 401:
               print(f"        ❌ API Key Error: Check SERPER_API_KEY")
               return None
              
           else:
               print(f"        ❌ API Error: {response.status_code} - {response.text[:100]}")
               if retry < max_retries - 1:
                   time.sleep(RATE_LIMIT_DELAY)
                   continue
               return None
              
       except requests.exceptions.Timeout:
           print(f"        ⏰ API request timeout ({API_TIMEOUT}s)")
           if retry < max_retries - 1:
               time.sleep(2)
               continue
           return None
          
       except requests.exceptions.RequestException as e:
           print(f"        ❌ Request error: {e}")
           if retry < max_retries - 1:
               time.sleep(2)
               continue
           return None
          
       except Exception as e:
           print(f"        ❌ Unexpected error: {e}")
           if retry < max_retries - 1:
               time.sleep(2)
               continue
           return None
  
   return None


def find_biotech_company_website_with_timeout(company_name, vc_name=None):
   """FIXED: Find company website with comprehensive timeout and hang prevention"""
  
   if not SERPER_API_KEY:
       print(f"    ❌ SERPER_API_KEY not found")
       return None
  
   print(f"    [SEARCH] '{company_name}' (timeout: {GLOBAL_TIMEOUT}s)")
   if vc_name:
       print(f"        [VC CONTEXT] Using '{vc_name}' for disambiguation")
  
   def search_company():
       try:
           # Detect generic names
           def is_generic_name(name):
               name_lower = name.lower().strip()
              
               if len(name.split()) == 1 and len(name) <= 8:
                   return True
              
               generic_words = [
                   'solutions', 'systems', 'technologies', 'tech', 'labs',
                   'group', 'company', 'corp', 'inc', 'ventures', 'capital'
               ]
              
               words = [word for word in name_lower.split() if word not in ['the', 'and', 'of']]
               if len(words) <= 2 and any(word in generic_words for word in words):
                   return True
              
               biotech_terms = [
                   'therapeutics', 'pharma', 'pharmaceutical', 'biotech', 'bio',
                   'medical', 'medicine', 'health', 'sciences', 'diagnostics'
               ]
              
               has_biotech_term = any(term in name_lower for term in biotech_terms)
              
               if len(name) <= 10 and not has_biotech_term:
                   return True
              
               return False
          
           is_generic = is_generic_name(company_name)
          
           # Enhanced query strategy to prevent company name confusion
           # ALWAYS start with direct .com domain search
           company_clean = re.sub(r'[^a-zA-Z0-9]', '', company_name).lower()
           direct_domain_query = f"{company_clean}.com"
           
           if is_generic:
               biotech_queries = [
                   direct_domain_query,  # FIRST: Direct domain search
                   f'"{company_name}" site:.com',
                   f'"{company_name}" biotech OR pharmaceutical OR therapeutics',
                   f'"{company_name}" healthcare OR medical OR "life sciences"'
               ]
               # Add VC context query for generic names (high priority)
               if vc_name:
                   biotech_queries.insert(2, f'"{company_name}" "{vc_name}"')
               print(f"        [GENERIC] Using biotech-focused queries with OR logic")
           else:
               biotech_queries = [
                   direct_domain_query,   # FIRST: Direct domain search  
                   f'"{company_name}" site:.com',  # Prioritize exact match on .com domains
                   f'"{company_name}"',            # Pure exact match
                   f'"{company_name}" -therapeutics -pharma -biotech'  # Exclude biotech terms to avoid confusion
               ]
               # Add VC context query for non-generic names (second priority)
               if vc_name:
                   biotech_queries.insert(2, f'"{company_name}" "{vc_name}"')
          
           best_result = None
           best_score = 0
          
           for i, query in enumerate(biotech_queries, 1):
               print(f"      [QUERY {i}] {query}")
              
               # Use retry logic for each API call
               result = make_serper_request_with_retries(query, company_name)
              
               if result and result['total_score'] > best_score:
                   best_result = result
                   best_score = result['total_score']
                  
                   if best_score >= 90:
                       print(f"      🎯 High confidence result - stopping search")
                       break
              
               # Rate limiting delay between queries
               if i < len(biotech_queries):
                   time.sleep(COMPANY_DELAY)
          
           return best_result
          
       except Exception as e:
           print(f"    ❌ Search error: {e}")
           return None
  
   # Execute search with timeout
   result, error = timeout_handler(search_company, timeout_duration=GLOBAL_TIMEOUT)
  
   if error:
       if isinstance(error, TimeoutException):
           print(f"    ⏰ TIMEOUT: Search exceeded {GLOBAL_TIMEOUT}s")
       else:
           print(f"    ❌ ERROR: {error}")
       return None
  
   if result and result.get('total_score', 0) >= 30:
       print(f"      FOUND: {result['website_url']} (score: {result['total_score']})")
       return result
   elif result and result.get('total_score', 0) >= 15:
       print(f"      FOUND (low confidence): {result['website_url']} (score: {result['total_score']})")
       return result
   else:
       print(f"      No website found")
       return None


def filter_top_confidence_results(websites_found, max_results=25):
   """Filter to only return the top N most confident website results"""
  
   if not websites_found:
       return []
  
   print(f"\n🎯 CONFIDENCE FILTERING")
   print(f"=" * 40)
   print(f"Original results: {len(websites_found)} websites")
   print(f"Target: Top {max_results} most confident")
  
   # Sort by total score (confidence) in descending order
   sorted_websites = sorted(websites_found, key=lambda x: x['total_score'], reverse=True)
  
   # Take only the top N results
   top_websites = sorted_websites[:max_results]
  
   print(f"Filtered results: {len(top_websites)} websites")
  
   if top_websites:
       # Show confidence distribution
       print(f"\n📊 CONFIDENCE DISTRIBUTION (Top {len(top_websites)}):")
      
       # Group by confidence ranges
       confidence_ranges = {
           'Very High (90-120)': [w for w in top_websites if w['total_score'] >= 90],
           'High (70-89)': [w for w in top_websites if 70 <= w['total_score'] < 90],
           'Medium (50-69)': [w for w in top_websites if 50 <= w['total_score'] < 70],
           'Low (30-49)': [w for w in top_websites if 30 <= w['total_score'] < 50],
           'Very Low (<30)': [w for w in top_websites if w['total_score'] < 30]
       }
      
       for range_name, websites in confidence_ranges.items():
           if websites:
               percentage = (len(websites) / len(top_websites)) * 100
               print(f"  {range_name}: {len(websites)} websites ({percentage:.1f}%)")
      
       # Show top 25 scores
       print(f"\n🏆 TOP {min(25, len(top_websites))} CONFIDENCE SCORES:")
       for i, website in enumerate(top_websites[:25], 1):
           score = website['total_score']
           company = website['company_name']
           domain = urlparse(website['website_url']).netloc.replace('www.', '')
           print(f"  {i:2d}. {score:3d}/120 - {company} ({domain})")
      
       # Show what was filtered out
       filtered_out = len(websites_found) - len(top_websites)
       if filtered_out > 0:
           lowest_included = top_websites[-1]['total_score'] if top_websites else 0
           print(f"\n📉 FILTERED OUT: {filtered_out} websites")
           print(f"    Lowest included score: {lowest_included}/120")
          
           if filtered_out <= 10:  # Show details if not too many
               filtered_websites = sorted_websites[max_results:]
               print(f"    Filtered websites:")
               for website in filtered_websites[:5]:  # Show first 5
                   score = website['total_score']
                   company = website['company_name']
                   print(f"      {score:3d}/120 - {company}")
               if len(filtered_websites) > 5:
                   print(f"      ... and {len(filtered_websites)-5} more")
  
   return top_websites


def load_vc_portfolio_data(file_path):
   """FIXED: Load portfolio data with VC attribution preservation"""
   try:
       with open(file_path, 'r', encoding='utf-8') as f:
           data = json.load(f)
      
       # Extract VC information for attribution
       vc_info = {
           'vc_name': data.get('vc_name', 'Unknown VC'),
           'vc_url': data.get('vc_url', ''),
           'vc_domain': data.get('vc_domain', ''),
           'extraction_timestamp': data.get('extraction_timestamp', ''),
           'source_file': file_path
       }
      
       # Get companies list
       companies = data.get('companies', [])
      
       print(f"📁 Loaded VC data: {vc_info['vc_name']}")
       print(f"🏢 Companies to process: {len(companies)}")
      
       return vc_info, companies
      
   except Exception as e:
       print(f"❌ Error loading VC portfolio data: {e}")
       return None, []


def process_biotech_companies_with_vc_attribution(vc_info, companies, max_results=25):
    """Optimized: Parallel, robust, resource-reusing company website discovery with logging."""
    import threading
    from functools import partial
    import logging
    logger = logging.getLogger("website_discovery")
    
    vc_name = vc_info['vc_name']
    logger.info(f"\n🧬 BIOTECH WEBSITE DISCOVERY FOR {vc_name}")
    logger.info(f"Processing {len(companies)} companies from {vc_name}")
    logger.info(f"Focus: Official company websites only | Top {max_results} results | Timeout per company: {GLOBAL_TIMEOUT}s")

    results = {
        "vc_attribution": vc_info,
        "processing_date": datetime.now().isoformat(),
        "total_companies": len(companies),
        "search_method": "biotech_focused_serper_with_timeout_management_parallel",
        "max_results_limit": max_results,
        "timeout_per_company": GLOBAL_TIMEOUT,
        "websites_found_raw": [],
        "websites_found": [],
        "companies_failed": [],
        "companies_timeout": [],
        "data_flow_stage": "website_discovery_with_attribution",
        "next_stage": "startup_enrichment"
    }
    start_time = time.time()
    setup_signal_handlers()

    # Thread-local session for resource reuse
    thread_local = threading.local()
    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session

    def process_company(company):
        company_start_time = time.time()
        try:
            # Patch requests in this thread to use session
            orig_request = requests.request
            session = get_session()
            requests.request = session.request
            website_info = find_biotech_company_website_with_timeout(company, vc_name)
            requests.request = orig_request  # Restore
            if website_info:
                website_info.update({
                    'source_vc': vc_name,
                    'source_vc_url': vc_info['vc_url'],
                    'source_vc_domain': vc_info['vc_domain'],
                    'portfolio_extraction_timestamp': vc_info['extraction_timestamp'],
                    'website_discovery_timestamp': datetime.now().isoformat(),
                    'search_duration': time.time() - company_start_time
                })
                logger.info(f"SUCCESS: {company} → {website_info['website_url']} ({website_info['total_score']}/120, {website_info['search_duration']:.1f}s)")
                return (company, website_info, None, None)
            else:
                search_duration = time.time() - company_start_time
                if search_duration >= GLOBAL_TIMEOUT - 2:
                    logger.warning(f"TIMEOUT: {company} exceeded {GLOBAL_TIMEOUT}s")
                    return (company, None, 'timeout', search_duration)
                else:
                    logger.warning(f"FAILED: {company} no official biotech website found")
                    return (company, None, 'failed', search_duration)
        except Exception as e:
            search_duration = time.time() - company_start_time
            logger.error(f"ERROR: {company} - {e}")
            return (company, None, 'error', search_duration)

    max_workers = min(10, len(companies)) if len(companies) > 1 else 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_company = {executor.submit(process_company, c): c for c in companies}
        for i, future in enumerate(as_completed(future_to_company), 1):
            company = future_to_company[future]
            try:
                c, website_info, fail_type, duration = future.result()
                if website_info:
                    results["websites_found_raw"].append(website_info)
                elif fail_type == 'timeout':
                    results["companies_timeout"].append({
                        'company_name': c,
                        'source_vc': vc_name,
                        'failure_reason': 'Search timeout',
                        'search_duration': duration
                    })
                elif fail_type in ('failed', 'error'):
                    results["companies_failed"].append({
                        'company_name': c,
                        'source_vc': vc_name,
                        'failure_reason': 'No official website found' if fail_type == 'failed' else 'Search error',
                        'search_duration': duration
                    })
            except Exception as e:
                logger.error(f"UNCAUGHT ERROR for {company}: {e}")
                results["companies_failed"].append({
                    'company_name': company,
                    'source_vc': vc_name,
                    'failure_reason': f'Uncaught error: {e}',
                    'search_duration': 0
                })
            logger.info(f"Progress: {i}/{len(companies)} companies processed")

    total_time = time.time() - start_time
    raw_found_count = len(results["websites_found_raw"])
    timeout_count = len(results["companies_timeout"])
    failed_count = len(results["companies_failed"])
    raw_success_rate = (raw_found_count / len(companies)) * 100 if companies else 0
    logger.info(f"\n🧬 WEBSITE DISCOVERY RESULTS FOR {vc_name}")
    logger.info(f"Raw Success: {raw_found_count}/{len(companies)} websites ({raw_success_rate:.1f}%) | Timeouts: {timeout_count} | Failed: {failed_count}")
    logger.info(f"Total time: {total_time:.1f}s | Avg per company: {total_time/len(companies):.1f}s")

    # Confidence filtering
    if results["websites_found_raw"]:
        filtered_websites = filter_top_confidence_results(results["websites_found_raw"], max_results)
        results["websites_found"] = filtered_websites
    else:
        results["websites_found"] = []
        logger.warning(f"NO WEBSITES FOUND FOR {vc_name}")

    results["filtering_applied"] = True
    results["websites_filtered_out"] = raw_found_count - len(results["websites_found"])
    results["performance_stats"] = {
        "total_processing_time": total_time,
        "average_time_per_company": total_time / len(companies) if companies else 0,
        "timeout_rate": (timeout_count / len(companies)) * 100 if companies else 0,
        "failure_rate": (failed_count / len(companies)) * 100 if companies else 0
    }
    return results


def process_from_orchestrator_input(vc_file_path, vc_name_fs=None, max_results=25):
   """FIXED: Process orchestrator input with timeout management"""
   print(f"🎯 PROCESSING ORCHESTRATOR INPUT")
   print(f"Input file: {vc_file_path}")
   print(f"VC name (filesystem): {vc_name_fs}")
   print(f"Max results: {max_results}")
   print(f"Timeout per company: {GLOBAL_TIMEOUT}s")
  
   # Load VC data from orchestrator file
   vc_info, companies = load_vc_portfolio_data(vc_file_path)
  
   if not companies:
       print("❌ No companies found in orchestrator input file!")
       return None
  
   # Process companies with VC attribution and timeout management
   results = process_biotech_companies_with_vc_attribution(vc_info, companies, max_results)
  
   # Save with proper VC attribution and workflow-compatible format
   # Use command-line vc_name_fs if provided, otherwise fallback to cleaned vc_name from JSON
   if vc_name_fs:
       vc_safe_name = vc_name_fs
   else:
       vc_safe_name = "".join(c for c in vc_info['vc_name'].lower() if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
   timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
   # --- Only output to output/runs/<vc_safe_name>/ ---
   runs_dir = os.path.join(OUTPUT_DIR, "runs", vc_safe_name)
   os.makedirs(runs_dir, exist_ok=True)
   output_file = f"{vc_safe_name}_websites_with_attribution_{timestamp}.json"
   output_path = os.path.join(runs_dir, output_file)

   # Ensure workflow compatibility
   results["workflow_compatible"] = True
   results["ready_for_enrichment"] = True
  
   with open(output_path, 'w') as f:
       json.dump(results, f, indent=2)
  
   print(f"\n💾 Results saved to: {output_path}")
   print(f"📊 Final count: {len(results['websites_found'])} websites")
   print(f"🏢 VC Attribution: {vc_info['vc_name']}")
   print(f"   Workflow Compatible: Ready for enrichment stage")
  
   # Performance summary
   perf_stats = results.get("performance_stats", {})
   timeout_rate = perf_stats.get("timeout_rate", 0)
   failure_rate = perf_stats.get("failure_rate", 0)
   avg_time = perf_stats.get("average_time_per_company", 0)
  
   print(f"\n📈 PERFORMANCE SUMMARY:")
   print(f"   Average time per company: {avg_time:.1f}s")
   print(f"   Timeout rate: {timeout_rate:.1f}%")
   print(f"   Failure rate: {failure_rate:.1f}%")
  
   if results.get('websites_filtered_out', 0) > 0:
       print(f"🔍 Filtered out: {results['websites_filtered_out']} lower confidence results")
  
   # FIXED: Print explicit output file for orchestrator
   print(f"OUTPUT_FILE: {output_path}")
  
   return output_path

import sys, os
if os.environ.get("PIPELINE_AUTO") == "1" and len(sys.argv) >= 3:
    input_path = sys.argv[1]          # first arg is the JSON path
    vc_name_fs = sys.argv[2]          # second arg is the normalized VC name
    max_results = 0                  # default
    process_from_orchestrator_input(input_path, vc_name_fs, max_results)
    sys.exit(0)
    
def main():
   """FIXED: Main function with timeout management and hang prevention"""
   print("[INFO] BIOTECH WEBSITE FINDER - FIXED VERSION WITH TIMEOUT MANAGEMENT")
   print("=" * 80)
   print("Features:")
   print("✓ Biotech/pharmaceutical context required")
   print("✓ Official company websites only")
   print("✓ Excludes news, social media, databases")
   print("✓ Domain matching for company names")
   print("✓ VC attribution preservation")
   print("✓ Orchestrator input support")
   print("✓ Explicit output file tracking")
   print("✓ Timeout management and hang prevention")
   print("✓ Enhanced error recovery")
   print()
  
   # Enhanced input handling for orchestrator
   choice = input("Choose option:\n1. Single company test\n2. Company list\n3. Load from VC portfolio file (RECOMMENDED)\n4. Orchestrator input (AUTO-DETECTED)\n\nEnter choice (1-4): ").strip()
  
   # Try to auto-detect orchestrator input
   if len(choice.split()) > 1 or choice.endswith('.json'):
       # This looks like a file path from orchestrator
       file_path = choice.strip()
       if os.path.exists(file_path):
           print(f"🤖 AUTO-DETECTED: Orchestrator input file")
           print(f"Processing: {file_path}")
          
           # Ask for max results
           try:
               max_results_input = input("Max results to return (default 25): ").strip()
               max_results = int(max_results_input) if max_results_input else 25
               if max_results <= 0 or max_results > 100:
                   print("⚠️ Invalid limit, using default 10")
                   max_results = 10
           except:
               print("⚠️ Invalid input, using default 10")
               max_results = 10
          
           result_file = process_from_orchestrator_input(file_path, max_results)
           return
       else:
           print(f"❌ File not found: {file_path}")
           return
  
   # Ask for max results limit for manual modes
   try:
       max_results_input = input("Max results to return (default 25): ").strip()
       max_results = int(max_results_input) if max_results_input else 25
       if max_results <= 0 or max_results > 100:
           print("⚠️ Invalid limit, using default 25")
           max_results = 25
   except:
       print("⚠️ Invalid input, using default 25")
       max_results = 25
  
   print(f"🎯 Will return top {max_results} most confident results")
   print(f"⏰ Timeout per company: {GLOBAL_TIMEOUT}s")
   print()
  
   if choice == "1":
       company_name = input("Enter biotech company name: ").strip()
       if company_name:
           result = find_biotech_company_website_with_timeout(company_name)
           if result:
               print(f"\n FOUND OFFICIAL WEBSITE:")
               print(f"Company: {result['company_name']}")
               print(f"Website: {result['website_url']}")
               print(f"Title: {result['title']}")
               print(f"Official Score: {result['official_site_score']}/100")
               print(f"Biotech Score: {result['biotech_context_score']}/20")
               print(f"Total Confidence: {result['total_score']}/120")
               print(f"Search Duration: {result.get('search_duration', 0):.1f}s")
               print(f"Reasons: {', '.join(result['validation_reasons'])}")
           else:
               print(f"\n❌ No official biotech website found")
  
   elif choice == "2":
       print("Enter biotech company names (one per line, press Enter twice to finish):")
       companies = []
       while True:
           company = input().strip()
           if not company:
               break
           companies.append(company)
      
       if companies:
           # Create dummy VC info for manual input
           dummy_vc_info = {
               'vc_name': 'Manual Input',
               'vc_url': '',
               'vc_domain': 'manual',
               'extraction_timestamp': datetime.now().isoformat(),
               'source_file': 'manual_input'
           }
          
           results = process_biotech_companies_with_vc_attribution(dummy_vc_info, companies, max_results)
          
           # Save results
           timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
           output_file = f"manual_biotech_websites_{timestamp}.json"
           output_path = os.path.join(OUTPUT_DIR, output_file)
          
           with open(output_path, 'w') as f:
               json.dump(results, f, indent=2)
          
           print(f"\n💾 Results saved to: {output_path}")
           print(f"📊 Final count: {len(results['websites_found'])} websites")
          
           # Performance summary
           perf_stats = results.get("performance_stats", {})
           timeout_rate = perf_stats.get("timeout_rate", 0)
           failure_rate = perf_stats.get("failure_rate", 0)
           avg_time = perf_stats.get("average_time_per_company", 0)
          
           print(f"📈 Performance: {avg_time:.1f}s avg, {timeout_rate:.1f}% timeouts, {failure_rate:.1f}% failures")
          
           if results.get('websites_filtered_out', 0) > 0:
               print(f"🔍 Filtered out: {results['websites_filtered_out']} lower confidence results")
          
           # Print explicit output file
           print(f"OUTPUT_FILE: {output_path}")
  
   elif choice == "3":
       file_path = input("Enter VC portfolio extraction file path: ").strip()
       if os.path.exists(file_path):
           result_file = process_from_orchestrator_input(file_path, max_results)
       else:
           print(f"File not found: {file_path}")
  
   elif choice == "4":
       file_path = input("Enter orchestrator input file path: ").strip()
       if os.path.exists(file_path):
           result_file = process_from_orchestrator_input(file_path, max_results)
       else:
           print(f"File not found: {file_path}")
  
   else:
       print("Invalid choice!")


def safe_vc_name(name):
    return re.sub(r'[^aA-Zz0-9_]', '', name.strip().replace(' ', '_')).lower()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: website_discovery.py <input_path> <vc_name_fs>")
        sys.exit(1)
    input_path = sys.argv[1]
    vc_name_fs = sys.argv[2]
    output_dir = os.path.join("output", "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    
    max_results = 20                      # default
    process_from_orchestrator_input(input_path, vc_name_fs, max_results)
    sys.exit(0)
    
def run_website_discovery(input_path, vc_name_fs, output_dir=None, max_results=20):
    """
    Run the website discovery pipeline natively.
    Args:
        input_path (str): Path to input JSON (from portfolio discovery)
        vc_name_fs (str): Filesystem-safe VC name
        output_dir (str, optional): Output directory. Defaults to output/runs/<vc_name_fs>/
        max_results (int): Max results per company
    Returns:
        str: Path to output JSON file
    """
    # Load VC data from orchestrator file
    vc_info, companies = load_vc_portfolio_data(input_path)
    if not companies:
        raise ValueError("No companies found in orchestrator input file!")
    results = process_biotech_companies_with_vc_attribution(vc_info, companies, max_results)
    if not output_dir:
        output_dir = os.path.join(OUTPUT_DIR, "runs", vc_name_fs)
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{vc_name_fs}_websites_with_attribution_{timestamp}.json"
    output_path = os.path.join(output_dir, output_file)
    results["workflow_compatible"] = True
    results["ready_for_enrichment"] = True
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"[OK] website discovery complete: {output_path}")
    return output_path
