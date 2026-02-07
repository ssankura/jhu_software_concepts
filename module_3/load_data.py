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
  applicant_data.json is the instructor-provided dataset used for Module 3 database loading.

"""

import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg


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


def map_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map a single JSON record to DB-ready fields.

    This mapping assumes the instructor-provided JSON keys:
      - overview_url, applicant_status, start_term, citizenship, gre_general,
        gre_verbal, degree_level, llm-generated-program, llm-generated-university
    """
    return {
        "program": clean_text(record.get("program")),
        "comments": clean_text(record.get("comments")),
        "date_added": clean_date(record.get("date_added")),
        "url": clean_text(record.get("overview_url")),
        "status": clean_text(record.get("applicant_status")),
        "term": clean_text(record.get("start_term")),
        "us_or_international": clean_text(record.get("citizenship")),
        "gpa": clean_float(record.get("gpa")),
        "gre": clean_float(record.get("gre_general")),
        "gre_v": clean_float(record.get("gre_verbal")),
        "gre_aw": clean_float(record.get("gre_aw")),
        "degree": clean_text(record.get("degree_level")),
        "llm_generated_program": clean_text(record.get("llm-generated-program")),
        "llm_generated_university": clean_text(record.get("llm-generated-university")),
    }


def load_json(path: str) -> List[Dict[str, Any]]:
    """Load JSON file and return a list of dict records."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of objects (records).")

    # Defensive: keep only dict-like items.
    return [x for x in data if isinstance(x, dict)]


def load_data(json_file: str, batch_size: int = 1000) -> None:
    """Load JSON records into PostgreSQL.

    Args:
        json_file: Path to the cleaned JSON file.
        batch_size: Number of records inserted per batch commit.
    """
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

    with psycopg.connect(database_url) as conn:
        create_table(conn)

        batch: List[Dict[str, Any]] = []
        for idx, record in enumerate(data, start=1):
            row = map_record(record)

            # URL is required to dedupe reliably; skip rows missing url.
            if row["url"] is None:
                continue

            batch.append(row)

            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, batch)
                conn.commit()
                print(f"Processed {idx} records...")
                batch = []

        if batch:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
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


if __name__ == "__main__":
    main()
