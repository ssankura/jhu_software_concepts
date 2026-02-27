"""
query_data.py

Module 5 - Querying PostgreSQL (GradCafe dataset)

This module supports two use cases:

1) CLI reporting:
   - Connects to PostgreSQL via environment variables
   - Runs SQL analytics queries
   - Prints results in a readable format

2) Web/Test support:
   - Exposes query_applicants_as_dicts()
   - Returns rows as dictionaries with stable keys

How to run:
Set DATABASE_URL and execute:
python3 query_data.py

Design notes:
- Uses psycopg for PostgreSQL connectivity
- Formats Decimal outputs safely
- Keeps query printing logic in run_query()
"""


import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

import psycopg
from psycopg import sql

from app.db import fetch_all as default_fetch_all_fn


# --- Path setup: allow running as script without installing the package ---
ROOT = Path(__file__).resolve().parents[1]  # points to src/
sys.path.insert(0, str(ROOT))

def _clamp_limit(value: int, min_v: int = 1, max_v: int = 100) -> int:
    """
    Ensure LIMIT value is an integer within allowed bounds.
    Defaults to 10 if invalid.
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = 10  # default if input is invalid

    # Restrict limit between min_v and max_v
    return max(min_v, min(n, max_v))


# ============================================================================
# Connection Management
# ----------------------------------------------------------------------------
# Centralizes DATABASE_URL handling so both CLI runs and tests fail early with a
# clear message if configuration is missing.
# ============================================================================

def get_connection():
    """
    Create and return a psycopg connection using DATABASE_URL.

    Returns:
        psycopg.Connection: An open database connection.

    Raises:
        ValueError: If DATABASE_URL environment variable is not set.

    Why:
        Using DATABASE_URL keeps configuration portable across:
        - local dev
        - GitHub Actions CI
        - Read the Docs / documentation examples
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set.\n"
            "Example:\n"
            'export DATABASE_URL="postgresql://graduser:grad123@localhost:5432/gradcafe"'
        )
    return psycopg.connect(db_url)


# ============================================================================
# Output normalization helpers
# ----------------------------------------------------------------------------
# PostgreSQL numeric columns often arrive as Decimal objects. Converting to float
# makes console output (and sometimes serialization) cleaner and consistent.
# ============================================================================

def _clean_value(value):
    """
    Convert Decimal values to float for cleaner output.

    Args:
        value: Any value returned from PostgreSQL.

    Returns:
        float if value is Decimal; otherwise returns original value.
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


# ============================================================================
# Query runner (CLI formatting)
# ----------------------------------------------------------------------------
# Prints results in a screenshot-style format similar to assignment examples.
# Supports:
# - single scalar outputs (COUNT, AVG, etc.)
# - multi-column single-row outputs (e.g., 4 averages)
# - multi-row outputs (e.g., top programs table)
# ============================================================================
# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches
def run_query(
            cursor,
            title,
            sql_query,
            params=None,
            multi=False,
            label=None,
            multi_labels=None,
    ):
    """
    Execute SQL query and print results in a screenshot-style output.

    Args:
        cursor: psycopg cursor object.
        title: Human-readable title for the query (kept for readability).
        sql_query: SQL query string.
        multi: True if the query returns multiple rows.
        label: Label for single-value outputs (e.g., "Applicant count").
        multi_labels: Labels for multi-column single-row outputs (e.g., averages).

    Notes:
        - This function prints to console (for Module 3 screenshots).
        - Web UI rendering is handled separately by Flask routes/templates.
    """
    _ = title
    if params is None:
        cursor.execute(sql_query)
    else:
        cursor.execute(sql_query, params)

    # -------------------- Multi-row results --------------------
    # Used for queries like "Top 5 programs" where multiple rows are expected.
    if multi:
        rows = cursor.fetchall()
        print("")  # spacing line for readability
        for row in rows:
            cleaned = []
            for v in row:
                cleaned.append(_clean_value(v))
            print(*cleaned)
        return

    # -------------------- Single-row results --------------------
    # Most analytics queries return exactly one row.
    row = cursor.fetchone()
    if row is None:
        print("No results")
        return

    cleaned = []
    for v in row:
        cleaned.append(_clean_value(v))

    # -------------------- Scalar output --------------------
    # Example: COUNT(*), AVG(gpa), percent calculations, etc.
    if len(cleaned) == 1:
        value = cleaned[0]
        if label:
            print(f"{label}: {value}")
        else:
            print(value)
        return

    # -------------------- Multi-value output --------------------
    # Example: averages for GPA, GRE, GRE V, GRE AW in one row.
    if multi_labels and len(multi_labels) == len(cleaned):
        parts = []
        for lab, val in zip(multi_labels, cleaned):
            parts.append(f"{lab}: {val}")
        print(", ".join(parts))
    else:
        print(*cleaned)


# ============================================================================
# Testability helper (Module 4)
# ----------------------------------------------------------------------------
# Returns DB rows as dictionaries with stable keys.
# This is used by tests to verify schema/data correctness without parsing HTML.
# ============================================================================

def query_applicants_as_dicts(
    limit: int = 10,
    fetch_all_fn: Optional[Callable[[str], list]] = None
) -> list[dict[str, Any]]:
    """
    Return applicants as a list of dicts using the required schema keys.

    This function exists primarily for Module 4 testing, where tests need to:
    - verify inserted rows exist
    - validate required columns are present
    - validate uniqueness/idempotency behavior

    Args:
        limit: Maximum number of rows to return (default: 10).
        fetch_all_fn: Optional injected dependency for fetching rows.
                      If None, defaults to app.db.fetch_all.

    Returns:
        list[dict[str, Any]]: List of dict rows with stable keys.
    """
    if fetch_all_fn is None:
        # Local import avoids circular dependencies when app imports query layer.
        fetch_all_fn = default_fetch_all_fn

    # Ensure limit is safe and bounded (1–100)
    limit_n = _clamp_limit(limit)

    stmt = sql.SQL("""
                   SELECT url,
                          term,
                          status,
                          us_or_international,
                          gpa,
                          gre,
                          gre_v,
                          gre_aw,
                          degree,
                          program,
                          llm_generated_program,
                          llm_generated_university
                   FROM applicants
                   ORDER BY url ASC LIMIT {lim};
                   """).format(lim=sql.Placeholder())

    rows = fetch_all_fn(stmt, (limit_n,))

    # Keys match the SELECT order and represent required fields for the app/tests.
    keys = [
        "url", "term", "status", "us_or_international", "gpa", "gre", "gre_v", "gre_aw",
        "degree", "program", "llm_generated_program", "llm_generated_university"
    ]

    out = []
    for r in rows:
        out.append(dict(zip(keys, r)))
    return out


# ============================================================================
# CLI Entry Point (Module 3 reporting)
# ----------------------------------------------------------------------------
# Runs all required queries and prints answers.
# Used for screenshots or sanity-checking DB state.
# ============================================================================

def main():
    """
    Run the required Module 3 SQL analytics queries and print results.

    Notes:
        - Output is console-based (for assignment screenshots).
        - The Flask web UI performs similar queries but renders via templates.
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:

            # ----------------------------------------------------------
            # Q1: Count Fall 2026 applicants
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q1: Number of Fall 2026 applicants",
                sql_query="""
                    SELECT COUNT(*)
                    FROM applicants
                    WHERE term = 'Fall 2026';
                    """,
                label="Applicant count"
            )

            # ----------------------------------------------------------
            # Q2: Percent international applicants
            # NULLIF prevents divide-by-zero when DB is empty.
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q2: Percentage of International Applicants",
                sql_query="""
                SELECT
                  ROUND(
                    100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                  )
                FROM applicants
                WHERE us_or_international IS NOT NULL;
                """,
                label="Percent International"
            )

            # ----------------------------------------------------------
            # Q3: Average GPA/GRE metrics
            # Printed as labeled values for screenshot clarity.
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q3: Average GPA, GRE, GRE V, GRE AW",
                sql_query="""
                SELECT
                  ROUND(AVG(gpa)::numeric, 3),
                  ROUND(AVG(gre)::numeric, 3),
                  ROUND(AVG(gre_v)::numeric, 3),
                  ROUND(AVG(gre_aw)::numeric, 3)
                FROM applicants;
                """,
                multi_labels=[
                    "Average GPA",
                    "Average GRE",
                    "Average GRE V",
                    "Average GRE AW"
                ]
            )

            # ----------------------------------------------------------
            # Q4: Avg GPA of American students (Fall 2026)
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q4: Avg GPA of American Students (Fall 2026)",
                sql_query="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND us_or_international = 'American'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA American"
            )

            # ----------------------------------------------------------
            # Q5: Acceptance % for Fall 2025
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q5: Acceptance % Fall 2025",
                sql_query="""
                SELECT
                  ROUND(
                    100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                  )
                FROM applicants
                WHERE term = 'Fall 2025';
                """,
                label="Acceptance percent"
            )

            # ----------------------------------------------------------
            # Q6: Avg GPA of accepted applicants (Fall 2026)
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q6: Avg GPA of Accepted Applicants (Fall 2026)",
                sql_query="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND status = 'Accepted'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA Acceptance"
            )

            # ----------------------------------------------------------
            # Q7: JHU Masters CS applicants (LLM standardized fields)
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q7: JHU Masters Computer Science Applicants",
                sql_query="""
                SELECT COUNT(*)
                FROM applicants
                WHERE degree ILIKE 'Master%'
                  AND llm_generated_program ILIKE '%computer science%'
                  AND (
                        llm_generated_university ILIKE '%Johns Hopkins%'
                     OR llm_generated_university ILIKE '%JHU%'
                     OR llm_generated_university ILIKE '%John%Hopkins%'
                  );
                """,
                label="JHU Masters Computer Science count"
            )

            # ----------------------------------------------------------
            # Q8: Accepted PhD CS applicants in 2026 at top universities
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q8: 2026 Accepted PhD CS Applicants at Top Universities",
                sql_query="""
                SELECT COUNT(*)
                FROM applicants
                WHERE term ILIKE '%2026%'
                  AND status = 'Accepted'
                  AND degree ILIKE 'PhD%'
                  AND llm_generated_program ILIKE '%computer science%'
                  AND (
                        llm_generated_university ILIKE '%Georgetown%'
                     OR llm_generated_university ILIKE '%MIT%'
                     OR llm_generated_university ILIKE '%Massachusetts Institute of Technology%'
                     OR llm_generated_university ILIKE '%Stanford%'
                     OR llm_generated_university ILIKE '%Carnegie Mellon%'
                     OR llm_generated_university ILIKE '%CMU%'
                  );
                """,
                label="Accepted PhD CS count"
            )

            # ----------------------------------------------------------
            # Q9: Same query using raw downloaded fields (not LLM fields)
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q9: Same as Q8 Using Raw Downloaded Fields",
                sql_query="""
                SELECT COUNT(*)
                FROM applicants
                WHERE term ILIKE '%2026%'
                  AND status = 'Accepted'
                  AND degree ILIKE 'PhD%'
                  AND program ILIKE '%computer science%'
                  AND (
                        program ILIKE '%Georgetown%'
                     OR program ILIKE '%MIT%'
                     OR program ILIKE '%Massachusetts Institute of Technology%'
                     OR program ILIKE '%Stanford%'
                     OR program ILIKE '%Carnegie Mellon%'
                     OR program ILIKE '%CMU%'
                  );
                """,
                label="Raw field accepted count"
            )

            # ----------------------------------------------------------
            # Custom Q10A: Top 5 programs (Fall 2026)
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Custom Q10A: Top 5 Programs (Fall 2026)",
                sql_query="""
                SELECT program, COUNT(*)
                FROM applicants
                WHERE term = 'Fall 2026'
                GROUP BY program
                ORDER BY COUNT(*) DESC
                LIMIT 5;
                """,
                multi=True
            )

            # ----------------------------------------------------------
            # Custom Q10B: Avg GPA of international applicants
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Custom Q10B: Avg GPA of International Applicants",
                sql_query="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE us_or_international = 'International'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA International"
            )


# Standard s
