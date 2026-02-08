"""
query_data.py

Purpose:
---------
Executes SQL analytics queries on the GradCafe PostgreSQL database
to answer all Module 3 assignment questions.

This script:
- Connects to PostgreSQL using DATABASE_URL
- Executes required analytical SQL queries
- Prints results in readable console format

Requirements:
-------------
- PostgreSQL database populated with applicant data
- DATABASE_URL environment variable must be configured

Run:
----
python query_data.py
"""

import os
import psycopg


def get_connection():
    """
    Establish database connection using DATABASE_URL environment variable.

    Returns:
        psycopg.Connection object
    """

    db_url = os.environ.get("DATABASE_URL")

    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set.")

    return psycopg.connect(db_url)


def run_query(cursor, question, sql):
    """
    Executes a SQL query and prints results.

    Args:
        cursor: PostgreSQL cursor object
        question: Description of query being executed
        sql: SQL query string
    """

    print("\n" + "=" * 70)
    print(question)
    print("=" * 70)

    cursor.execute(sql)
    results = cursor.fetchall()

    for row in results:
        print(row)


def main():
    """
    Main execution function.

    Runs all required Module 3 SQL analytical queries.
    """

    with get_connection() as conn:
        with conn.cursor() as cursor:

            # ----------------------------------------------------------
            # Q1: Count number of applicants applying for Fall 2026
            # ----------------------------------------------------------
            # This query counts all applicant entries where the
            # program start term is Fall 2026.
            run_query(
                cursor,
                "Q1: Number of Fall 2026 applicants",
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE term = 'Fall 2026';
                """
            )

            # ----------------------------------------------------------
            # Q2: Percentage of International Applicants
            # ----------------------------------------------------------
            # This query calculates the percentage of applicants who
            # identified as International students among all entries
            # with known citizenship information.
            run_query(
                cursor,
                "Q2: Percentage of International Applicants",
                """
                SELECT
                  ROUND(
                    100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                  )
                FROM applicants
                WHERE us_or_international IS NOT NULL;
                """
            )

            # ----------------------------------------------------------
            # Q3: Average GPA, GRE Quantitative, GRE Verbal, GRE AW
            # ----------------------------------------------------------
            # Calculates average standardized metrics for applicants
            # who provided GPA and GRE score information.
            run_query(
                cursor,
                "Q3: Average GPA, GRE, GRE V, GRE AW",
                """
                SELECT
                  ROUND(AVG(gpa)::numeric, 3),
                  ROUND(AVG(gre)::numeric, 3),
                  ROUND(AVG(gre_v)::numeric, 3),
                  ROUND(AVG(gre_aw)::numeric, 3)
                FROM applicants;
                """
            )

            # ----------------------------------------------------------
            # Q4: Average GPA of American Applicants in Fall 2026
            # ----------------------------------------------------------
            # Filters applicants who identified as American and
            # applied for Fall 2026 programs.
            run_query(
                cursor,
                "Q4: Avg GPA of American Students (Fall 2026)",
                """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND us_or_international = 'American'
                  AND gpa IS NOT NULL;
                """
            )

            # ----------------------------------------------------------
            # Q5: Acceptance Percentage for Fall 2025 Applicants
            # ----------------------------------------------------------
            # Calculates acceptance rate for Fall 2025 admission cycle.
            run_query(
                cursor,
                "Q5: Acceptance % Fall 2025",
                """
                SELECT
                  ROUND(
                    100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                  )
                FROM applicants
                WHERE term = 'Fall 2025';
                """
            )

            # ----------------------------------------------------------
            # Q6: Average GPA of Accepted Applicants in Fall 2026
            # ----------------------------------------------------------
            # Calculates GPA average for applicants who were accepted
            # into Fall 2026 programs.
            run_query(
                cursor,
                "Q6: Avg GPA of Accepted Applicants (Fall 2026)",
                """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND status = 'Accepted'
                  AND gpa IS NOT NULL;
                """
            )

            # ----------------------------------------------------------
            # Q7: Count JHU Masters Computer Science Applicants
            # ----------------------------------------------------------
            # Uses LLM-generated university and program fields to
            # accurately identify applicants to Johns Hopkins University
            # Masters Computer Science programs.
            run_query(
                cursor,
                "Q7: JHU Masters Computer Science Applicants",
                """
                SELECT COUNT(*)
                FROM applicants
                WHERE degree ILIKE 'Master%'
                  AND llm_generated_program ILIKE '%computer science%'
                  AND (
                        llm_generated_university ILIKE '%Johns Hopkins%'
                     OR llm_generated_university ILIKE '%JHU%'
                     OR llm_generated_university ILIKE '%John%Hopkins%' );
                      );
                """
            )

            # ----------------------------------------------------------
            # Q8: Accepted PhD CS Applicants at Top Universities (2026)
            # ----------------------------------------------------------
            # Identifies accepted PhD Computer Science applicants in 2026
            # at Georgetown, MIT, Stanford, and Carnegie Mellon.
            run_query(
                cursor,
                "Q8: 2026 Accepted PhD CS Applicants at Top Universities",
                """
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
                """
            )

            # ----------------------------------------------------------
            # Q9: Compare Q8 Using Raw Scraped Program Fields
            # ----------------------------------------------------------
            # Demonstrates difference between raw scraped text data
            # and standardized LLM-generated fields.
            run_query(
                cursor,
                "Q9: Same as Q8 Using Raw Downloaded Fields",
                """
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
                """
            )

            # ----------------------------------------------------------
            # Custom Q10A: Top 5 Most Applied Programs (Fall 2026)
            # ----------------------------------------------------------
            run_query(
                cursor,
                "Custom Q10A: Top 5 Programs (Fall 2026)",
                """
                SELECT program, COUNT(*)
                FROM applicants
                WHERE term = 'Fall 2026'
                GROUP BY program
                ORDER BY COUNT(*) DESC
                LIMIT 5;
                """
            )

            # ----------------------------------------------------------
            # Custom Q10B: Average GPA of International Applicants
            # ----------------------------------------------------------
            run_query(
                cursor,
                "Custom Q10B: Avg GPA of International Applicants",
                """
                SELECT ROUND(AVG(gpa)::numeric, 3)
                FROM applicants
                WHERE us_or_international = 'International'
                  AND gpa IS NOT NULL;
                """
            )


if __name__ == "__main__":
    main()
