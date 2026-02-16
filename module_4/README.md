# Grad School Cafe Data Analysis — Module 4

## Overview

This project implements a complete data ingestion, database, analytics, and web application pipeline for Grad School Cafe applicant data.

It includes:

- Data scraping
- PostgreSQL database loading
- Analytical SQL queries
- Flask web application
- Unit and integration testing
- 100% automated test coverage

Module 4 builds upon Module 3 and includes architectural improvements and fixes based on failed test cases and coverage requirements.

---

## Project Structure

```
module_4
│
├── pytest.ini
├── load_data.log
│
├── src
│   ├── load_data.py
│   ├── pull_data.py
│   ├── query_data.py
│   ├── run.py
│   ├── requirements.txt
│   ├── README.md
│   │
│   ├── app
│   │   ├── __init__.py
│   │   ├── db.py
│   │   └── pages
│   │       ├── analysis.py
│   │       └── pull_state.py
│   │
│   ├── templates
│   │   ├── base.html
│   │   └── analysis.html
│   │
│   └── static
│       └── styles.css
│
└── tests
    ├── conftest.py
    ├── test_*.py
```

---

## Features

### Data Pipeline

- `pull_data.py` — Scrapes and loads data  
- `load_data.py` — Inserts rows into PostgreSQL  
- `query_data.py` — Executes analytical SQL queries  

Database inserts are idempotent using:

```sql
ON CONFLICT DO NOTHING
```

---

### Flask Web Application

Run locally:

```bash
export PYTHONPATH=src
python src/run.py
```

Open:

```
http://127.0.0.1:5000/analysis
```

#### Available Routes

| Route | Method | Description |
|--------|--------|------------|
| `/` | GET | Redirects to analysis |
| `/analysis` | GET | Displays analytics |
| `/pull-data` | POST | Runs scrape + load |
| `/update-analysis` | POST | Refreshes analytics |

---

### Dependency Injection (Module 4 Upgrade)

Module 4 introduces dependency injection via:

```python
app.extensions["deps"]
```

This enables:

- Mocked scraper functions
- Mocked database functions
- True unit test isolation
- Integration testing without rewriting routes

This refactor was required to fix Module 3 test failures.

---

### Busy-State Locking

To prevent concurrent pull operations:

- `pull_state.py` manages a lock file.
- If busy:
  - `/pull-data` returns HTTP 409
  - `/update-analysis` returns HTTP 409
  - UI disables Update button

---

## Analytical Questions Implemented

The `/analysis` page computes:

1. Fall 2026 applicant count  
2. Percentage of International students (two decimal places)  
3. Average GPA, GRE, GRE V, GRE AW  
4. Average GPA of American students (Fall 2026)  
5. Acceptance percentage (Fall 2025)  
6. Average GPA of Accepted applicants (Fall 2026)  
7. JHU Masters Computer Science applicants  
8. Top University PhD CS acceptances (LLM-generated fields)  
9. Same query using raw downloaded fields  
10. Custom:
   - Top 5 programs (Fall 2026)
   - Average GPA International students  

All percentage outputs are formatted using `_fmt_pct()` to guarantee:

```
XX.XX%
```

---

## Testing

The project includes:

- Unit tests
- Route tests
- Button behavior tests
- Database helper tests
- Integration tests (PostgreSQL required)
- End-to-end web tests
- Main-block execution tests

### Run Tests

From `module_4` root:

```bash
pytest -q
```

Expected result:

```
61 passed
Required test coverage of 100% reached.
Total coverage: 100.00%
```

---

## Known Limitation

When clicking **Pull Data**:

- The endpoint may display raw JSON in the browser.
- Clicking the browser back button returns to the page.
- The button may remain greyed out due to client-side JavaScript behavior.

This occurs because the endpoint supports both:

- JSON responses (for automated tests)
- HTML redirects (for browser usage)

A future improvement would be:

- Separate API and browser endpoints  
- Or use AJAX for pull operations  

This limitation does not affect correctness or test coverage.

---

## Improvements Over Module 3

Module 4 includes:

- Full dependency injection refactor
- Correct blueprint endpoint naming (`pull_data_route`)
- Busy-state locking
- Strict percentage formatting
- Idempotent database inserts
- Removal of implicit DB imports in routes
- Improved route testability
- 100% test coverage

---

## Database Configuration

Set environment variable:

```bash
export DATABASE_URL=postgresql://username:password@localhost:5432/dbname
```

Integration tests require a running PostgreSQL instance.

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 📚 Sphinx Documentation

Full project documentation generated using Sphinx is available here:

https://github.com/ssankura/jhu_software_concepts/blob/main/module_4/docs/build/html

The documentation includes:

• Overview & Setup instructions (environment variables, running app & tests)  
• Architecture description (Web / ETL / Database layers)  
• API Reference generated using autodoc  
• Testing Guide (markers, fixtures, selectors)  
• Operational Notes (busy-state policy, idempotency strategy)  
• Troubleshooting guide  

The documentation was generated using:

    cd module_4/docs
    make clean
    make html

Generated HTML files are located under:

    module_4/docs/build/html/

## Read the Docs Hosting Note

The Github repository is private as required for course submission.

The free Read the Docs (Community) service does not support building documentation from private GitHub repositories. Because of this limitation, automated publishing to readthedocs.org was not possible.

However, the complete Sphinx-generated HTML documentation is included in this repository under:

module_4/docs/build/html/

This satisfies the Module 4 documentation requirement.


---

## Final Status

- All routes working
- Integration tests passing
- Database inserts idempotent
- Web UI functional
- 100% test coverage achieved
- Known limitation documented

