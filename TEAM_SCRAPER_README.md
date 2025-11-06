# Team Member Scraper

A web scraper for extracting team member names and roles from VC firm websites and storing them in Airtable.

## Overview

The team scraper follows the same architecture as `contact_extract_updated.py` but focuses on extracting team member information instead of contact emails.

## How It Works

### 1. **Page Discovery**
- Visits the VC firm's homepage
- Finds links containing team-related keywords:
  - Core: "team", "people", "about", "leadership", "partners"
  - Staff: "advisors", "staff", "our-team", "meet-the-team", "our-people"
  - Leadership: "who-we-are", "board", "management", "executives"
  - Extended: "founders", "bios", "crew", "founding-team", "founding-partners"
  - Compound: "meet-our", "meet-us", "about-us", "profiles", "personnel"
  - Specific: "investment-team", "team-members", "our-partners", "leadership-team"
- Filters out banned pages (careers, jobs, portfolio companies, etc.)

### 2. **Parallel Page Processing**
- Processes up to 3 unique pages per VC firm
- Priority order:
  1. Homepage
  2. Team/people pages
  3. Other relevant pages

### 3. **GPT-4 Extraction**
- Sends page HTML to GPT-4 with a structured prompt
- Extracts team member data:
  - **Name**: Full name (First Last or First Middle Last)
  - **Role**: Job title (Partner, Principal, Associate, etc.)
  - **Section**: Where found on page (Leadership Team, Partners, etc.)

### 4. **Deduplication**
- Removes duplicate team members (case-insensitive name matching)
- Keeps the most complete/accurate role for each person

### 5. **Airtable Integration**
- Updates three fields:
  - `Team Members`: Formatted list (Name (Role), one per line)
  - `Team Members JSON`: Full structured data
  - `Team Extractor Applied?`: Checkpoint flag (prevents re-processing)

## Usage

### Single VC Extraction
```bash
python team_extract.py '<VC Name>' <Website URL>
```

Example:
```bash
python team_extract.py 'Andreessen Horowitz' https://a16z.com
```

### Batch Processing
```bash
python team_extract.py --batch [max_vcs] [parallel_vcs]
```

Examples:
```bash
# Process first 10 VCs, 3 at a time
python team_extract.py --batch 10 3

# Process all unprocessed VCs, 3 at a time
python team_extract.py --batch
```

## Architecture

### Key Components

1. **Selenium + Undetected ChromeDriver**
   - Avoids bot detection
   - Handles JavaScript-rendered pages
   - Creates unique Chrome profiles per VC to prevent race conditions

2. **GPT-4 Extraction**
   - Uses `gpt-4-1106-preview` model
   - Structured prompts with clear rules
   - Returns JSON with name, role, and section

3. **Parallel Processing**
   - ThreadPoolExecutor for concurrent VC processing
   - Default: 3 VCs processed simultaneously
   - Each VC processes up to 3 pages in parallel

4. **Airtable Integration**
   - PyAirtable API
   - Checkpoint flags to avoid re-processing
   - Stores both formatted text and structured JSON

### File Structure

```
team_extract.py
├── sanitize_for_filename()       # Clean VC names for file paths
├── ensure_driver_initialized()   # Pre-init Chrome driver
├── create_driver_with_retry()    # Create driver with retry logic
├── is_team_link()                # Identify team-related pages
├── extract_team_with_gpt()       # GPT-4 extraction logic
├── deduplicate_team_members()    # Remove duplicate members
├── crawl_and_extract_team()      # Main crawl logic
├── update_airtable_team()        # Airtable update logic
├── process_single_vc()           # Single VC processor
└── batch_process_vcs()           # Batch processor
```

## Configuration

### Environment Variables
```bash
AIRTABLE_API_KEY=your_airtable_api_key
AIRTABLE_BASE_ID=your_base_id
AIRTABLE_VC_TABLE=VC Database
OPENAI_API_KEY=your_openai_api_key
```

### Airtable Fields
The scraper expects/updates these fields in your VC Database table:
- `VC/Investor Name` (input)
- `Website URL` (input)
- `Team Members` (output)
- `Team Members JSON` (output)
- `Team Extractor Applied?` (checkpoint flag)

## Comparison with Other Scrapers

### vs. `contact_extract_updated.py`
| Feature | Team Scraper | Contact Scraper |
|---------|--------------|-----------------|
| **Target Pages** | Team/people/about pages | Contact pages |
| **Extracted Data** | Names + Roles | Email addresses |
| **GPT Prompt** | Extracts team member info | Extracts contact emails |
| **Output Fields** | Team Members, Team Members JSON | contact emails, Contact Extractor JSON |
| **Checkpoint Flag** | Team Extractor Applied? | Contact Extractor Applied? |

### vs. `workflow.py`
- `workflow.py` orchestrates multiple scrapers (basic extraction + portfolio analysis)
- `team_extract.py` is a standalone scraper focused only on team members
- Can be integrated into `workflow.py` as an additional pipeline step

## Team Member Types Extracted

The scraper identifies these roles:
- Partners (General Partners, Managing Partners, Limited Partners)
- Principals
- Associates
- Venture Partners
- Advisors
- Board Members
- Executive Team (CEO, CFO, CTO, etc.)
- Investment Team members
- Operating Partners

## Filtering Logic

### Pages to INCLUDE:
- Team/people/about pages
- Leadership pages
- Partner/advisor pages

### Pages to EXCLUDE:
- Careers/jobs pages
- Portfolio company pages
- Press/news pages
- Blog posts
- Social media links
- Privacy/legal pages

## Example Output

### Formatted Text (Team Members field):
```
John Smith (Managing Partner)
Jane Doe, PhD (General Partner)
Robert Johnson (Principal)
Sarah Williams (Venture Partner)
```

### Structured JSON (Team Members JSON field):
```json
[
  {
    "name": "John Smith",
    "role": "Managing Partner",
    "section": "Leadership Team"
  },
  {
    "name": "Jane Doe, PhD",
    "role": "General Partner",
    "section": "Partners"
  },
  {
    "name": "Robert Johnson",
    "role": "Principal",
    "section": "Investment Team"
  },
  {
    "name": "Sarah Williams",
    "role": "Venture Partner",
    "section": "Venture Partners"
  }
]
```

## Performance

- **Average time per VC**: ~15-30 seconds
- **Parallel processing**: 3 VCs simultaneously (configurable)
- **Pages per VC**: Up to 3 pages
- **GPT tokens**: ~500-2000 tokens per page

## Error Handling

- Driver creation retries (up to 3 attempts)
- Automatic cleanup of temp Chrome profiles
- Graceful handling of missing pages
- Continues batch processing even if individual VCs fail

## Dependencies

```bash
pip install selenium
pip install undetected-chromedriver
pip install beautifulsoup4
pip install pyairtable
pip install openai
pip install python-dotenv
```

## Troubleshooting

### Chrome driver issues
- The scraper pre-initializes the driver to avoid race conditions
- Unique temp directories prevent profile conflicts
- Retry logic handles transient errors

### No team members found
- Check if the website has a team page
- Verify the page isn't blocked by banned keywords
- Check GPT response in logs for extraction issues

### Airtable update failures
- Verify API key and base ID
- Ensure fields exist in the table
- Check field permissions
