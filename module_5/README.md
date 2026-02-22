# Module 5 – Secure GradCafe Data Analysis Web Application

## Overview

This project extends the GradCafe Data Analysis Web Application by incorporating security hardening, dependency management, packaging, and reproducibility best practices.

The application loads GradCafe applicant data into a PostgreSQL database, performs SQL analytics through parameterized queries, and displays results on a stylized Flask web interface.

Module 5 strengthens the system by:

* Enforcing SQL injection defenses
* Implementing least-privilege database access
* Enforcing explicit query limits
* Achieving 100% test coverage
* Ensuring Pylint 10/10 compliance
* Generating a dependency graph
* Integrating security scanning

---

## Features Implemented

### Database Integration

* Loads cleaned GradCafe applicant data into PostgreSQL using `psycopg`
* Uses a structured relational schema for applicant data
* Prevents duplicate entries using a unique URL constraint
* Implements least-privilege access using a read-only database user
* Supports configuration via `DATABASE_URL` or DB_* environment variables

---

### SQL Data Analysis

The application performs the following analytical queries:

1. Number of applicants applying for Fall 2026
2. Percentage of international applicants
3. Average GPA, GRE Quantitative, GRE Verbal, and GRE Analytical Writing
4. Average GPA of American applicants for Fall 2026
5. Acceptance rate for Fall 2025
6. Average GPA of accepted applicants for Fall 2026
7. Count of applicants applying to Johns Hopkins University MS Computer Science
8. Count of accepted PhD Computer Science applicants to selected universities
9. Comparison of raw scraped data versus LLM-enhanced data
10. Custom analysis:
   * Top 5 most common programs for Fall 2026
   * Average GPA of international applicants

All SQL queries:

* Use parameterized execution
* Avoid f-string SQL construction
* Avoid string concatenation
* Enforce explicit `LIMIT` clauses
* Validate and clamp user-provided limits between 1 and 100

---

### Flask Web Application

* Uses Flask Blueprints for modular routing
* Uses HTML templates and CSS styling
* Displays SQL analytics dynamically
* Integrates securely with PostgreSQL
* Maintains Pull Data concurrency control

---

### Pull Data Functionality

#### Pull Data Button

* Executes Module 2 scraping logic
* Loads new GradCafe entries into PostgreSQL
* Uses subprocess execution
* Uses file-based locking to prevent concurrent scraping

#### Update Analysis Button

* Refreshes the webpage with the latest database results
* Disabled while data scraping is running

---

## Pull Data Concurrency Control

To prevent multiple simultaneous scraping operations, the application uses a file-based locking mechanism implemented in:

    src/app/pages/pull_state.py

This module:

* Creates a lock file when Pull Data begins
* Prevents duplicate Pull Data execution
* Disables Pull Data and Update Analysis buttons while scraping runs
* Automatically removes the lock file when scraping completes

This ensures safe database updates and prevents concurrent scraper execution.

---

## Security Enhancements

### SQL Injection Protection

* All SQL queries use psycopg parameter binding (`%s`)
* Safe dynamic SQL composition using `sql.SQL()` and `sql.Placeholder()`
* No direct interpolation of user input
* No f-string SQL queries
* No string concatenation in query construction

---

### Least Privilege Database User

A read-only PostgreSQL user (`grad_ro`) was created with:

    CREATE USER grad_ro WITH PASSWORD 'your_password';
    GRANT CONNECT ON DATABASE gradcafe TO grad_ro;
    GRANT USAGE ON SCHEMA public TO grad_ro;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO grad_ro;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grad_ro;

This ensures:

* No schema modification privileges
* No DROP or ALTER permissions
* No superuser access
* Principle of least privilege enforced

---

## Dependency Management and Packaging

### setup.py

The project includes a `setup.py` file allowing installation via:

    pip install -e .

This ensures:

* Clean module imports
* No manual PYTHONPATH adjustments
* CI compatibility
* Reproducible installations

---

### Dependency Graph

A full dependency graph was generated using:

    PYTHONPATH=src python -m pydeps src/app/__init__.py --noshow -T svg -o dependency.svg

The generated file `dependency.svg` visualizes module relationships and dependency structure.

---

## Installation Instructions

### 1. Create Virtual Environment

    python3 -m venv .venv
    source .venv/bin/activate

---

### 2. Install Dependencies

    pip install -r requirements.txt
    pip install -e .

---

### 3. Configure PostgreSQL Connection

Option A (Legacy)

    export DATABASE_URL="postgresql://username:password@localhost:5432/gradcafe"

Option B (Module 5 Required Style)

    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_NAME=gradcafe
    export DB_USER=grad_ro
    export DB_PASSWORD=your_password

---

### 4. Load Applicant Data

    python load_data.py --file applicant_data.json

---

### 5. Run SQL Queries

    python query_data.py

---

### 6. Run Flask Application

    python -m src.run

Open browser:

    http://127.0.0.1:5000

---

## Testing and Static Analysis

### Run Tests (100% Coverage Required)

    pytest --cov=src --cov-fail-under=100

---

### Run Pylint (10/10 Required)

    python -m pylint src --fail-under=10

---

## Security Scanning

Security scanning was performed using Snyk:

    snyk test

Optional static code scan:

    snyk code test

---

## Technologies Used

* Python 3.14
* Flask
* PostgreSQL
* psycopg
* pytest
* pytest-cov
* pylint
* pydeps
* graphviz
* Snyk
* uv

---

## Database Schema

The PostgreSQL database contains a single table named `applicants` with the following fields:

* Program
* Comments
* Date Added
* URL (unique)
* Status
* Term
* Citizenship Status
* GPA
* GRE Quantitative
* GRE Verbal
* GRE Analytical Writing
* Degree
* LLM Generated Program
* LLM Generated University

---

## Known Limitations

### Read-Only User Restriction

* The `grad_ro` user cannot insert or modify data.
* Data loading must be performed using a higher-privileged user.

### Scraping Data Accuracy

* GradCafe data is user-submitted and anonymous.
* Entries may be incomplete, inconsistent, or inaccurate.

---
## Documentation (Read the Docs)

The full Sphinx documentation is available at:

https://jhu-software-concepts-module5.readthedocs.io/en/latest/


## Academic Integrity

All code was implemented following assignment requirements using only permitted libraries and methodologies. Security enhancements and testing rigor were implemented using best practices aligned with secure software development standards.