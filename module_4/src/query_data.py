"""
query_data.py

Module 3 - Querying PostgreSQL (GradCafe dataset)

This script connects to a PostgreSQL database (using DATABASE_URL),
runs the required SQL analysis queries, prints results to the console,
and includes two custom analysis questions.

How to run:
-----------
export DATABASE_URL="postgresql://graduser:grad123@localhost:5432/gradcafe"
python3 query_data.py
"""

import os
from decimal import Decimal
from typing import Any, Callable, Optional

import psycopg


def get_connection():
    """
    Create and return a psycopg connection using DATABASE_URL.

    Returns:
        psycopg.Connection: An open database connection.

    Raises:
        ValueError: If DATABASE_URL environment variable is not set.
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError(
            "DATABASE_URL environment variable is not set.\n"
            "Example:\n"
            'export DATABASE_URL="postgresql://graduser:grad123@localhost:5432/gradcafe"'
        )
    return psycopg.connect(db_url)


def _clean_value(value):
    """
    Convert Decimal values to float for cleaner console output.

    Args:
        value: Any value returned from PostgreSQL

    Returns:
        float | original value
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


from decimal import Decimal

def run_query(cursor, title, sql, multi=False, label=None, multi_labels=None):
    """
    Executes SQL and prints in a screenshot-style output.

    Args:
        cursor: psycopg cursor
        title: question title (can be printed or ignored)
        sql: SQL query string
        multi: True if query returns multiple rows
        label: label for single-value outputs (e.g., "Applicant count")
        multi_labels: labels for multi-column single-row outputs (e.g., averages)
    """
    cursor.execute(sql)

    # -------------------- Multi-row results --------------------
    if multi:
        rows = cursor.fetchall()
        print("")  # spacing line (like screenshot)
        for row in rows:
            cleaned = []
            for v in row:
                if isinstance(v, Decimal):
                    v = float(v)
                cleaned.append(v)
            print(*cleaned)
        return

    # -------------------- Single-row results --------------------
    row = cursor.fetchone()
    if row is None:
        print("No results")
        return

    cleaned = []
    for v in row:
        if isinstance(v, Decimal):
            v = float(v)
        cleaned.append(v)

    # Single value
    if len(cleaned) == 1:
        value = cleaned[0]
        if label:
            print(f"{label}: {value}")
        else:
            print(value)
        return

    # Multi-value (like averages)
    if multi_labels and len(multi_labels) == len(cleaned):
        parts = []
        for lab, val in zip(multi_labels, cleaned):
            parts.append(f"{lab}: {val}")
        print(", ".join(parts))
    else:
        print(*cleaned)

def query_applicants_as_dicts(
    limit: int = 10,
    fetch_all_fn: Optional[Callable[[str], list]] = None
) -> list[dict[str, Any]]:
    """
    Return applicants as a list of dicts with required keys.
    Used for Module 4 testability.
    """
    if fetch_all_fn is None:
        from app.db import fetch_all as fetch_all_fn  # local import to avoid circulars

    sql = f"""
    SELECT
        url, term, status, us_or_international, gpa, gre, gre_v, gre_aw,
        degree, program, llm_generated_program, llm_generated_university
    FROM applicants
    ORDER BY url ASC
    LIMIT {int(limit)};
    """
    rows = fetch_all_fn(sql)

    keys = [
        "url", "term", "status", "us_or_international", "gpa", "gre", "gre_v", "gre_aw",
        "degree", "program", "llm_generated_program", "llm_generated_university"
    ]

    out = []
    for r in rows:
        out.append(dict(zip(keys, r)))
    return out


def main():
    """
    Main execution function.

    Runs all required Module 3 SQL analytical queries and prints
    answers in formatted console output.
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:

            # ----------------------------------------------------------
            # Q1
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q1: Number of Fall 2026 applicants",
                sql="""
                    SELECT COUNT(*)
                    FROM applicants
                    WHERE term = 'Fall 2026';
                    """,
                label="Applicant count"
            )

            # ----------------------------------------------------------
            # Q2
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q2: Percentage of International Applicants",
                sql="""
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
            # Q3
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q3: Average GPA, GRE, GRE V, GRE AW",
                sql="""
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
            # Q4
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q4: Avg GPA of American Students (Fall 2026)",
                sql="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND us_or_international = 'American'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA American"
            )

            # ----------------------------------------------------------
            # Q5
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q5: Acceptance % Fall 2025",
                sql="""
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
            # Q6
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q6: Avg GPA of Accepted Applicants (Fall 2026)",
                sql="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND status = 'Accepted'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA Acceptance"
            )

            # ----------------------------------------------------------
            # Q7
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q7: JHU Masters Computer Science Applicants",
                sql="""
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
            # Q8
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q8: 2026 Accepted PhD CS Applicants at Top Universities",
                sql="""
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
            # Q9
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Q9: Same as Q8 Using Raw Downloaded Fields",
                sql="""
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
            # Custom Q10A
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Custom Q10A: Top 5 Programs (Fall 2026)",
                sql="""
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
            # Custom Q10B
            # ----------------------------------------------------------
            run_query(
                cursor,
                title="Custom Q10B: Avg GPA of International Applicants",
                sql="""
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE us_or_international = 'International'
                  AND gpa IS NOT NULL;
                """,
                label="Average GPA International"
            )


if __name__ == "__main__":
    main()
