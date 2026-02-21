"""
load_data.py

Loads cleaned GradCafe applicant data from a JSON file into a PostgreSQL database
using psycopg.

Module 3/4 Requirements Satisfied:
----------------------------------
- Uses Python + psycopg to load data into PostgreSQL (no manual psql import)
- Creates the `applicants` table if it does not exist
- Inserts records in batches for performance and determinism
- Prevents duplicate entries using a UNIQUE constraint on `url`
  (idempotent re-runs and repeated Pull Data do not create duplicates)

Run:
----
  export DATABASE_URL="postgresql://<user>:<pass>@localhost:5432/gradcafe"
  python3 load_data.py --file applicant_data.json

Operational Notes:
------------------
- Writes timestamped logs to both console and load_data.log
- Skips records without a URL because url is the uniqueness key
- Uses `ON CONFLICT (url) DO NOTHING` for safe idempotency
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg




# --- Path setup: allow running as script without installing the package ---
ROOT = Path(__file__).resolve().parents[1]  # points to src/
sys.path.insert(0, str(ROOT))

# ============================================================================
# Logging Configuration
# ----------------------------------------------------------------------------
# Configure one logger for this module and attach both:
#   1) File handler (persisted logs for debugging after runs)
#   2) Console handler (immediate feedback while running)
#
# The "if not logger.handlers" guard prevents duplicate handlers if the module
# is imported multiple times (common during tests or Flask reloads).
# ============================================================================

logger = logging.getLogger("db_loader")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler("load_data.log")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ============================================================================
# Data cleaning helpers
# ----------------------------------------------------------------------------
# These functions normalize values so database inserts are consistent and safe.
# They are defensive because scraped JSON often has:
# - empty strings
# - inconsistent key names
# - mixed numeric formats
# - missing values
# ============================================================================

def clean_text(value: Any) -> Optional[str]:
    """
    Normalize text fields.

    Returns:
        - Stripped string for non-empty input
        - None for None, empty, or whitespace-only input

    Why:
        Treating "" and "   " as None prevents storing meaningless values in DB.
    """
    if value is None:
        return None
    text = str(value).strip()
    return None if text == "" else text


def clean_float(value: Any) -> Optional[float]:
    """
    Normalize numeric fields as floats.

    Returns:
        float if conversion succeeds, otherwise None.

    Why:
        Scraped numeric fields can arrive as strings ("320") or be malformed.
        Returning None avoids insert failures and keeps downstream SQL stable.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_date(value: Any) -> Optional[datetime.date]:
    """
    Parse a date string into a `datetime.date`.

    Supports common GradCafe-style formats. Returns None if parsing fails.

    Why:
        Dates often appear in multiple formats depending on scraper version or site.
        Standardizing to DATE ensures consistent queries and schema correctness.
    """
    if value is None:
        return None

    text = str(value).strip()
    if text == "":
        return None

    formats = (
        "%B %d, %Y",  # February 01, 2026
        "%b %d, %Y",  # Feb 01, 2026
        "%Y-%m-%d",   # 2026-02-01
        "%m/%d/%Y",   # 02/01/2026
    )

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


# ============================================================================
# Schema management
# ----------------------------------------------------------------------------
# The loader is responsible for ensuring the required table exists.
# UNIQUE on url enforces idempotency (duplicate pulls do not duplicate rows).
# ============================================================================

def create_table(conn: psycopg.Connection) -> None:
    """
    Create the applicants table and a UNIQUE index on url if missing.

    Args:
        conn: Active psycopg connection (transaction managed by context manager).

    Why:
        Ensures the loader can run on a fresh database (CI, new dev machine)
        without requiring manual setup.
    """
    logger.info("Ensuring applicants table exists...")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applicants (
            p_id SERIAL PRIMARY KEY,
            program TEXT,
            comments TEXT,
            date_added DATE,
            url TEXT,
            status TEXT,
            term TEXT,
            us_or_international TEXT,
            gpa DOUBLE PRECISION,
            gre DOUBLE PRECISION,
            gre_v DOUBLE PRECISION,
            gre_aw DOUBLE PRECISION,
            degree TEXT,
            llm_generated_program TEXT,
            llm_generated_university TEXT
        );
        """
    )

    # Prevent duplicates across reloads and repeated Pull Data runs.
    # This supports the assignment's "idempotency/constraints" requirement.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS applicants_url_uniq
        ON applicants(url);
        """
    )

    logger.info("Table and UNIQUE index verified/created.")


# ============================================================================
# Record mapping
# ----------------------------------------------------------------------------
# Converts a raw JSON record into the DB-ready structure.
# Accepts multiple possible input keys to be robust across pipeline versions.
# ============================================================================

def map_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a raw JSON record into a normalized dict matching DB columns.

    Args:
        record: One JSON object representing an applicant row.

    Returns:
        dict with keys matching INSERT statement placeholders.

    Notes:
        URL is treated as the uniqueness key.
        Records missing URL are skipped by the loader.
    """
    # Accept multiple possible key names (robust across pipeline versions)
    url = (
        record.get("url")
        or record.get("overview_url")
        or record.get("entry_url")
        or record.get("page_url")
    )

    # Program name has historically existed under multiple keys in the pipeline
    program = (
        record.get("program_name")
        or record.get("program")
        or record.get("program_name_clean")
        or record.get("llm-generated-program")
        or record.get("llm_generated_program")
    )

    return {
        "program": clean_text(program),
        "comments": clean_text(record.get("comments")),
        "date_added": clean_date(record.get("date_added")),
        "url": clean_text(url),

        # Applicant status is stored as normalized text (Accepted/Rejected/etc.)
        "status": clean_text(record.get("applicant_status")),

        # Term fields differ by pipeline version; we normalize into `term`
        "term": clean_text(record.get("semester_year_start") or record.get("start_term")),

        # Citizenship field can vary; normalize into `us_or_international`
        "us_or_international": clean_text(
            record.get("international_or_american")
            or record.get("citizenship")
            or record.get("us_or_international")
        ),

        # Numeric metrics (optional)
        "gpa": clean_float(record.get("gpa")),
        "gre": clean_float(record.get("gre_score") or record.get("gre_general")),
        "gre_v": clean_float(record.get("gre_v_score") or record.get("gre_verbal")),
        "gre_aw": clean_float(record.get("gre_aw")),

        # Degree level can vary; normalize into `degree`
        "degree": clean_text(record.get("masters_or_phd") or record.get("degree_level")),

        # LLM-generated / cleaned fields used for analysis queries
        "llm_generated_program": clean_text(
            record.get("program_name_clean")
            or record.get("llm-generated-program")
            or record.get("llm_generated_program")
        ),
        "llm_generated_university": clean_text(
            record.get("university_clean")
            or record.get("llm-generated-university")
            or record.get("llm_generated_university")
        ),
    }


# ============================================================================
# JSON Loading
# ----------------------------------------------------------------------------
# Loads cleaned JSON exported from your ETL pipeline.
# Ensures data is a list of dict records before inserting.
# ============================================================================

def load_json(path: str) -> List[Dict[str, Any]]:
    """
    Load a JSON file and return a list of dictionary records.

    Args:
        path: File path to the JSON file.

    Returns:
        list[dict]: Valid JSON objects only.

    Raises:
        ValueError: If JSON root is not a list (assignment expects list of records).
    """
    logger.info("Loading JSON file: %s", path)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects (records).")

    # Defensive: filter out non-dict items to avoid insert failures.
    records = [x for x in data if isinstance(x, dict)]
    logger.info("Loaded %s dict records from JSON.", len(records))

    return records


# ============================================================================
# Primary loader function
# ----------------------------------------------------------------------------
# Handles:
# - DB connection
# - schema creation
# - batch inserts with ON CONFLICT DO NOTHING
# - summary logging
# ============================================================================

# pylint: disable=too-many-locals
def load_data(json_file: str, batch_size: int = 1000) -> None:
    """
    Load JSON records into PostgreSQL.

    Args:
        json_file: Path to the cleaned JSON file.
        batch_size: Number of records inserted per batch commit.

    Why batching:
        - Faster inserts than per-row commits
        - More deterministic performance for CI
        - Lower transaction overhead
    """
    start_time = time.time()
    logger.info("Starting data load from file: %s (batch_size=%s)", json_file, batch_size)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable not set. Example:\n"
            'export DATABASE_URL="postgresql://user:pass@localhost:5432/gradcafe"'
        )

    data = load_json(json_file)

    # Parameterized insert statement.
    # ON CONFLICT(url) DO NOTHING enforces idempotency for duplicate pulls.
    insert_sql = """
        INSERT INTO applicants (
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw, degree,
            llm_generated_program, llm_generated_university
        )
        VALUES (
            %(program)s, %(comments)s, %(date_added)s, %(url)s,
            %(status)s, %(term)s, %(us_or_international)s,
            %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s, %(degree)s,
            %(llm_generated_program)s, %(llm_generated_university)s
        )
        ON CONFLICT (url) DO NOTHING;
    """

    skipped_missing_url = 0
    committed_batches = 0

    # psycopg connection context manager:
    # - opens connection
    # - ensures close even on exceptions
    with psycopg.connect(database_url) as conn:
        logger.info("Connected to PostgreSQL.")
        create_table(conn)

        batch: List[Dict[str, Any]] = []

        for idx, record in enumerate(data, start=1):
            row = map_record(record)

            # URL is required to dedupe reliably; skip rows missing URL.
            if row["url"] is None:
                skipped_missing_url += 1

                # Avoid noisy logs: warn only for first few and periodic milestones
                if skipped_missing_url <= 5 or skipped_missing_url % 1000 == 0:
                    logger.warning(
                        "Skipping record #%s due to missing URL (skipped=%s).",
                        idx,
                        skipped_missing_url,
                    )
                continue

            batch.append(row)

            # When batch is full, insert and commit.
            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch)
                conn.commit()

                committed_batches += 1
                logger.info(
                    "Committed batch #%s (processed up to record #%s).",
                    committed_batches,
                    idx,
                )
                batch = []

        # Insert remaining rows after loop ends.
        if batch:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            conn.commit()
            committed_batches += 1
            logger.info("Committed final batch #%s (end of file).",committed_batches)

        # Summary statistics for debugging and assignment evidence.
        total = conn.execute("SELECT COUNT(*) FROM applicants LIMIT 1;").fetchone()[0]
        elapsed = round(time.time() - start_time, 2)

        logger.info("Data loading complete. Total records in database: %s", total)
        logger.info("Skipped missing-url records: %s", skipped_missing_url)
        logger.info("Total batches committed: %s", committed_batches)
        logger.info("Total execution time: %s seconds", elapsed)

        # Friendly final console output for humans
        print(f"\n✅ Data loading complete. Total records in database: {total}")


# ============================================================================
# CLI entrypoint
# ----------------------------------------------------------------------------
# Makes this module usable as a script:
#   python3 load_data.py --file applicant_data.json
# ============================================================================

def main() -> None:
    """
    Parse CLI args and run the loader.

    This exists so the loader can be executed:
    - locally from terminal
    - in CI scripts
    - in Makefile targets (if added later)
    """
    parser = argparse.ArgumentParser(
        description="Load applicant JSON data into PostgreSQL."
    )
    parser.add_argument("--file", required=True, help="Path to JSON file.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of inserts per batch commit (default: 1000).",
    )
    args = parser.parse_args()
    load_data(args.file, batch_size=args.batch_size)


# Standard Python
