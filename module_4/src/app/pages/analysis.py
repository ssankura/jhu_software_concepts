"""
app/pages/analysis.py

This module defines the webpage routes responsible for displaying
GradCafe analysis results.

Responsibilities:
----------------
• Execute SQL queries
• Retrieve analytics data from PostgreSQL
• Pass results to HTML templates
"""
import subprocess
import sys
from pathlib import Path
from flask import redirect, url_for, flash
from flask import request, jsonify, redirect, url_for, flash, current_app

from app.pages.pull_state import is_running, start, stop

from decimal import Decimal
from flask import render_template

# Import Blueprint instance
from app.pages import pages_bp

# Import database helper functions
from app.db import fetch_one, fetch_all

from flask import current_app, jsonify
from app.pages.pull_state import is_running, start, stop

from app.db import fetch_all, fetch_one

def _fmt_pct(value) -> str:
    """
    Ensure percentage is always rendered with two decimals + a percent sign.
    Accepts None/Decimal/int/float/str.
    """
    if value is None:
        return "0.00%"
    value = _convert_decimal(value)
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        # if it's already a string like "12.34", still append %
        s = str(value)
        return s if s.endswith("%") else f"{s}%"


def _convert_decimal(value):
    """
    Helper function to convert Decimal objects into float values
    so they display cleanly in HTML templates.

    Args:
        value: Decimal or numeric value

    Returns:
        float or original value
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


@pages_bp.route("/")
def home():
    """
    Homepage route.

    Redirects users to the analysis page.
    """
    return analysis()


@pages_bp.route("/analysis")
def analysis():
    """
    Main analysis webpage route.

    Executes SQL queries required by Module 3 assignment
    and sends results to the HTML template.
    """
    deps = current_app.extensions.get("deps", {})
    fetch_one = deps["fetch_one_fn"]
    fetch_all = deps["fetch_all_fn"]

    # -------------------------------------------------
    # Question 1: Number of Fall 2026 Applicants
    # -------------------------------------------------
    q1_fall_2026_count = fetch_one("""
        SELECT COUNT(*)
        FROM applicants
        WHERE term = 'Fall 2026';
    """)

    # -------------------------------------------------
    # Question 2: Percentage of International Applicants
    # -------------------------------------------------
    q2_pct_international = fetch_one("""
        SELECT
          ROUND(
            100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
          )
        FROM applicants
        WHERE us_or_international IS NOT NULL;
    """)

    # -------------------------------------------------
    # Question 3: Average GPA, GRE, GRE V, GRE AW
    # -------------------------------------------------
    q3_avgs = fetch_all("""
        SELECT
          ROUND(AVG(gpa)::numeric, 3),
          ROUND(AVG(gre)::numeric, 3),
          ROUND(AVG(gre_v)::numeric, 3),
          ROUND(AVG(gre_aw)::numeric, 3)
        FROM applicants;
    """)[0]

    # -------------------------------------------------
    # Question 4: Avg GPA of American Students (Fall 2026)
    # -------------------------------------------------
    q4_avg_gpa_american_fall_2026 = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE term = 'Fall 2026'
          AND us_or_international = 'American'
          AND gpa IS NOT NULL;
    """)

    # -------------------------------------------------
    # Question 5: Acceptance % for Fall 2025
    # -------------------------------------------------
    q5_pct_accepted_fall_2025 = fetch_one("""
        SELECT
          ROUND(
            100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
          )
        FROM applicants
        WHERE term = 'Fall 2025';
    """)

    # -------------------------------------------------
    # Question 6: Avg GPA of Accepted Applicants (Fall 2026)
    # -------------------------------------------------
    q6_avg_gpa_accepted_fall_2026 = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE term = 'Fall 2026'
          AND status = 'Accepted'
          AND gpa IS NOT NULL;
    """)

    # -------------------------------------------------
    # Question 7: JHU Masters Computer Science Applicants
    # -------------------------------------------------
    q7_jhu_ms_cs = fetch_one("""
        SELECT COUNT(*)
        FROM applicants
        WHERE degree ILIKE 'Master%'
          AND llm_generated_program ILIKE '%computer science%'
          AND (
                llm_generated_university ILIKE '%Johns Hopkins%'
             OR llm_generated_university ILIKE '%JHU%'
              );
    """)

    # -------------------------------------------------
    # Question 8: 2026 Accepted PhD CS Applicants at Top Universities
    # (Using LLM generated fields)
    # -------------------------------------------------
    q8_top_univ_phd_cs_accepted_2026 = fetch_one("""
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
    """)

    # -------------------------------------------------
    # Question 9: Same Query Using Raw Program Fields
    # -------------------------------------------------
    q9_top_univ_phd_cs_accepted_2026_raw = fetch_one("""
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
    """)

    # -------------------------------------------------
    # Custom Question 1: Top 5 Programs for Fall 2026
    # -------------------------------------------------
    custom_top5_programs_fall_2026 = fetch_all("""
        SELECT program, COUNT(*) AS application_count
        FROM applicants
        WHERE term = 'Fall 2026'
        GROUP BY program
        ORDER BY application_count DESC
        LIMIT 5;
    """)

    # -------------------------------------------------
    # Custom Question 2: Avg GPA of International Applicants
    # -------------------------------------------------
    custom_avg_gpa_international = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE us_or_international = 'International'
          AND gpa IS NOT NULL;
    """)

    # -------------------------------------------------
    # Package results into dictionary for HTML template
    # -------------------------------------------------
    results = {
        "q1_fall_2026_count": q1_fall_2026_count,
        "q2_pct_international": _fmt_pct(q2_pct_international),
        "q3_avg_gpa": _convert_decimal(q3_avgs[0]),
        "q3_avg_gre": _convert_decimal(q3_avgs[1]),
        "q3_avg_gre_v": _convert_decimal(q3_avgs[2]),
        "q3_avg_gre_aw": _convert_decimal(q3_avgs[3]),
        "q4_avg_gpa_american_fall_2026": _convert_decimal(q4_avg_gpa_american_fall_2026),
        "q5_pct_accepted_fall_2025": _fmt_pct(q5_pct_accepted_fall_2025),
        "q6_avg_gpa_accepted_fall_2026": _convert_decimal(q6_avg_gpa_accepted_fall_2026),
        "q7_jhu_ms_cs": q7_jhu_ms_cs,
        "q8_top_univ_phd_cs_accepted_2026": q8_top_univ_phd_cs_accepted_2026,
        "q9_top_univ_phd_cs_accepted_2026_raw": q9_top_univ_phd_cs_accepted_2026_raw,
        "q10a_top_programs": custom_top5_programs_fall_2026,
        "q10b_avg_gpa_international": _convert_decimal(custom_avg_gpa_international),
    }

    # Render HTML template with results
    return render_template(
        "analysis.html",
        results=results,
        pull_running=is_running()
    )


@pages_bp.post("/pull-data")
def pull_data():
    deps = current_app.extensions.get("deps", {})
    pull_data_fn = deps["pull_data_fn"]

    result = pull_data_fn()

    # If request expects HTML (browser form submit), redirect
    wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json

    if wants_html:
        if result.get("ok"):
            flash(f"Pull Data completed. Inserted: {result.get('inserted', 0)}", "success")
        else:
            flash("Pull Data failed. Check logs.", "danger")

        return redirect(url_for("pages.analysis"))

    # Otherwise (tests / API), return JSON
    return jsonify(result), 200


@pages_bp.post("/update-analysis")
def update_analysis():
    if is_running():
        return jsonify({"busy": True}), 409

    deps = current_app.extensions.get("deps", {})
    update_analysis_fn = deps.get("update_analysis_fn", lambda: None)
    update_analysis_fn()

    return jsonify({"ok": True}), 200

