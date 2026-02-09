# GradCafe Data Scraper - Module Assignment

**Name:** Sireesha Sankuratripati
**JHED ID:** ssankur1  
**Module Info:** Module 2 - Web Scraping Assignment  

---

## Approach
* Before scraping, the robots.txt file for The GradCafe was reviewed. The /survey/ path is not disallowed for general user-agents, and a 1-second crawl delay was implemented to remain consistent with ethical scraping standards.
* This project implements a modular, functional solution to extract and structure 30,000 records from The GradCafe survey database. The architecture is split into three distinct modules to separate concerns: network handling, data cleaning, and execution logic.

### 1. Data Extraction (`scrape_data`)
The extraction logic resides in `scrape.py`. It utilizes the `urllib3` library's `PoolManager` to handle connection pooling. 
* **Targeting:** The function constructs URLs using the `pp=25` parameter to ensure a consistent 25 results per page.
* **Headers:** A custom `User-Agent` string is included to mimic a standard browser request, which is necessary to bypass basic automated traffic filters.

### 2. Data Transformation (`clean_data`)
The cleaning logic in `clean.py` transforms raw HTML into a structured list of dictionaries using `BeautifulSoup4`.
* **Parsing Strategy:** The script identifies all `<tr>` elements and maps specific `<td>` cells to JSON keys. 
* **Private Methods:** A private method `_clean_text()` is used to normalize strings, removing extra whitespace and newline characters via `.split()` and `.join()`.
* **String Manipulation:** The "Program" field is parsed using string splitting to extract the "Degree" type from parentheses, and the "Date Added" field is manually prefixed with "Added on" to match the required schema.

### 3. Storage and Orchestration
The `main.py` script manages the lifecycle of the data.
* **`save_data()`**: Serializes the final list into `applicant_data.json` with a 4-space indent for readability.
* **`load_data()`**: Provides a mechanism to read existing JSON data back into a Python list.
* **Rate Limiting:** To reach the 30,000-record goal (1,200 pages), a 1-second `time.sleep()` is enforced between requests to minimize the risk of IP blocking.

### 4. LLM Standardization & Batch Processing Logic
To process 30,000+ records, the `app.py` script implements an incremental processing pipeline:

* **Streaming I/O (JSON Lines):** To prevent memory exhaustion (OOM), the script is designed to read the input JSON and write to the output file row-by-row using `JSONL` (JSON Lines) format. This ensures that even if the process is interrupted, all previously processed rows are saved.
* **Prompt Templating:** For each row, a specific instruction prompt is generated for the TinyLlama model. It passes the raw `program` string and asks the model to return a structured JSON response identifying the `standardized_program` and `standardized_university`.
* **Fuzzy Matching Post-Processor:** * After the LLM provides a suggestion, the script uses Python's `difflib.get_close_matches` to compare the result against `canon_universities.txt`. 
    * It uses a similarity threshold (default 0.9) to "snap" the LLM's output to the official canonical name.
* **Error Handling & Retries:** The script includes a precompiled regex (`JSON_OBJ_RE`) to extract the JSON block from the LLM's response, effectively ignoring any "chatter" or conversational text the model might generate.
---

## Known Bugs & Limitations

### 1. Unparsed Data Fields (GPA and US/International)
The output currently contains empty strings for "GPA" and "US/International". 
* **Cause:** These data points are not consistently tagged in the source HTML; they are often buried within a single unstructured text block in the "Program" column. 
* **Proposed Fix:** Use the `re` (regex) library to define patterns like `\d\.\d{2}` to isolate numerical GPA values from the surrounding text.

### 2. Term Constant
The "term" field is hardcoded to "Fall 2024". 
* **Cause:** The summary table on GradCafe does not always explicitly state the term in a separate cell.
* **Proposed Fix:** The scraper would need to follow the "extra url" link for every single record to scrape the individual result page where the term is explicitly meta-tagged.

### 3. Server Blocking (Rate Limiting)
During large-scale scrapes (1,200+ pages), the server may return a `403 Forbidden` or `503 Service Unavailable` error.
* **Cause:** Rapid sequential requests trigger Cloudflare or server-side anti-scraping measures.
* **Proposed Fix:** Implement exponential backoff logic where the script pauses for longer durations if a request fails, or rotate User-Agent strings.

### 4. Critical Performance Issue (Intel MacBook Air)
**Status:** Incomplete processing due to hardware constraints.  
**Details:** The standardization phase was executed on a **MacBook Air (1.1 GHz Dual-Core Intel Core i3)**. 
* **Observation:** Because this processor lacks the performance cores and GPU acceleration (Metal) found in Apple Silicon (M-series) chips, the LLM inference speed was extremely slow (averaging several minutes per dozen rows).
* **Result:** Due to the time required for 30,000 rows exceeding several days of continuous processing, the script was manually stopped to meet the assignment submission deadline.
* **Output:** The submitted `standardized_output.json` contains approximately **500KB** of processed data. This represents the initial successful batch of standardized records.
* 
### 5. Proposed Fix for Scale
To complete the full 30,000-row standardization on this hardware, a non-LLM approach (pure Regex and fuzzy matching) would be required to bypass the high computational cost of local inference on a Dual-Core i3.

---
## Execution Instructions
1. Navigate to `llm_hosting/`
2. Run `pip install -r requirements.txt`
3. Run `python app.py --file applicant_data.json --out standardized_output.json`