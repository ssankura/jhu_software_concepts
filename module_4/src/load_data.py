"""
load_data.py

Loads cleaned GradCafe applicant data from a JSON file into a PostgreSQL database
using psycopg.

Module 3 requirements satisfied:
- Uses Python + psycopg to load data into PostgreSQL (not manual psql import)
- Creates the `applicants` table if it does not exist
- Inserts records in batches for performance
- Prevents duplicate entries using a UNIQUE constraint on `url`

Run:
  export DATABASE_URL="postgresql://<user>:<pass>@localhost:5432/gradcafe"
  python3 load_data.py --file applicant_data.json

Notes:
- Writes timestamped logs to both console + load_data.log
"""

import argparse
import json
import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg

# ---------------------------------------------------
# Logging Configuration (console + file with timestamps)
# ---------------------------------------------------
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


def clean_text(value: Any) -> Optional[str]:
    """Return a stripped string or None for empty/blank values."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text == "" else text


def clean_float(value: Any) -> Optional[float]:
    """Convert a value to float. Return None if conversion fails."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_date(value: Any) -> Optional[datetime.date]:
    """Parse a date string into a date object.

    Supports common GradCafe-style formats. Returns None if parsing fails.
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


def create_table(conn: psycopg.Connection) -> None:
    """Create the applicants table and a UNIQUE index on url (if missing)."""
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

    # Prevent duplicates across reloads and later "Pull Data" updates.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS applicants_url_uniq
        ON applicants(url);
        """
    )
    logger.info("Table and UNIQUE index verified/created.")


def map_record(record: Dict[str, Any]) -> Dict[str, Any]:
    # Accept multiple possible key names (robust across your pipeline versions)
    url = (
        record.get("url")
        or record.get("overview_url")
        or record.get("entry_url")
        or record.get("page_url")
    )

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

        "status": clean_text(record.get("applicant_status")),

        # your JSON calls this semester_year_start (not start_term)
        "term": clean_text(record.get("semester_year_start") or record.get("start_term")),

        "us_or_international": clean_text(
            record.get("international_or_american")
            or record.get("citizenship")
            or record.get("us_or_international")
        ),

        "gpa": clean_float(record.get("gpa")),
        "gre": clean_float(record.get("gre_score") or record.get("gre_general")),
        "gre_v": clean_float(record.get("gre_v_score") or record.get("gre_verbal")),
        "gre_aw": clean_float(record.get("gre_aw")),

        "degree": clean_text(record.get("masters_or_phd") or record.get("degree_level")),

        # your JSON uses program_name_clean/university_clean
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



def load_json(path: str) -> List[Dict[str, Any]]:
    """Load JSON file and return a list of dict records."""
    logger.info(f"Loading JSON file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects (records).")

    # Defensive: keep only dict-like items.
    records = [x for x in data if isinstance(x, dict)]
    logger.info(f"Loaded {len(records)} dict records from JSON.")
    return records


def load_data(json_file: str, batch_size: int = 1000) -> None:
    """Load JSON records into PostgreSQL.

    Args:
        json_file: Path to the cleaned JSON file.
        batch_size: Number of records inserted per batch commit.
    """
    start_time = time.time()
    logger.info(f"Starting data load from file: {json_file} (batch_size={batch_size})")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable not set. Example:\n"
            'export DATABASE_URL="postgresql://user:pass@localhost:5432/gradcafe"'
        )

    data = load_json(json_file)

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

    with psycopg.connect(database_url) as conn:
        logger.info("Connected to PostgreSQL.")
        create_table(conn)

        batch: List[Dict[str, Any]] = []
        for idx, record in enumerate(data, start=1):
            row = map_record(record)

            # URL is required to dedupe reliably; skip rows missing url.
            if row["url"] is None:
                skipped_missing_url += 1
                # Keep warnings light (log first few + every 1000th skip)
                if skipped_missing_url <= 5 or skipped_missing_url % 1000 == 0:
                    logger.warning(f"Skipping record #{idx} due to missing URL (skipped={skipped_missing_url}).")
                continue

            batch.append(row)

            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch)
                conn.commit()
                committed_batches += 1
                logger.info(f"Committed batch #{committed_batches} (processed up to record #{idx}).")
                batch = []

        if batch:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            conn.commit()
            committed_batches += 1
            logger.info(f"Committed final batch #{committed_batches} (end of file).")

        total = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
        elapsed = round(time.time() - start_time, 2)

        logger.info(f"Data loading complete. Total records in database: {total}")
        logger.info(f"Skipped missing-url records: {skipped_missing_url}")
        logger.info(f"Total batches committed: {committed_batches}")
        logger.info(f"Total execution time: {elapsed} seconds")

        # Keep a friendly final console line too
        print(f"\n✅ Data loading complete. Total records in database: {total}")


def main() -> None:
    """Parse CLI args and run the loader."""
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


if __name__ == "__main__":  # pragma: no cover
    try:
        logger.info("Starting PostgreSQL loader script...")
        main()
        logger.info("Loader script completed successfully.")
    except Exception as e:
        logger.error(f"Loader script failed: {str(e)}", exc_info=True)
        raise
