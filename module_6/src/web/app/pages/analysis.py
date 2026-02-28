"""
Flask routes for the GradCafe analytics web UI.

This module renders the analysis page and handles pull-data and update-analysis requests.
It uses dependency injection through current_app.extensions["deps"] so tests can provide fakes.
A shared busy-state in pull_state prevents pull/update operations from running at the same time.
Helper functions keep formatting consistent (for example, percentages with two decimals).
"""
import os

from decimal import Decimal
from flask import current_app, flash, jsonify, redirect, render_template, request, url_for
from app.pages import pages_bp
from app.pages.pull_state import is_running
from publisher import publish_task



# ---------------------------------------------------------------------------
# Formatting helpers (kept here so the template stays simple and consistent)
# ---------------------------------------------------------------------------

def _convert_decimal(value):
    """
    Convert Decimal values (from SQL numeric columns) into Python floats.

    Why:
        Jinja templates render Decimal fine, but converting to float produces
        cleaner output and avoids surprises when formatting/serializing.

    Args:
        value: Any Python value; commonly Decimal, int, float, or None.

    Returns:
        float if value is Decimal; otherwise returns the original value.
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


def _fmt_pct(value) -> str:
    """
    Format a numeric value as a percentage string with exactly two decimals.

    Required by assignment:
        - All percentages shown with two decimals (e.g., "39.28%")

    Accepts:
        None, Decimal, int, float, or str.

    Returns:
        str: formatted percentage with "%" suffix.
    """
    if value is None:
        return "0.00%"

    value = _convert_decimal(value)

    # Primary path: numeric values
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        # Fallback: if it is already a string, ensure it ends with "%"
        s = str(value)
        return s if s.endswith("%") else f"{s}%"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@pages_bp.route("/")
def home():
    """
    Home route.

    Redirects to the analysis page so the application has a single “main” UI.
    Keeping this as a function (rather than an HTTP redirect) makes it easy
    to test and keeps routing simple.
    """
    return analysis()


@pages_bp.route("/analysis")
def analysis():
    """
    Render the Analysis page.

    Responsibilities:
    - Execute required SQL queries for the Module 3 analytics
    - Package results into a dictionary used by the Jinja template
    - Provide busy-state flag to disable/gate button actions in UI

    Dependency injection:
        `current_app.extensions["deps"]` must provide:
        - fetch_one_fn(sql: str) -> scalar result
        - fetch_all_fn(sql: str) -> list of rows

    Returns:
        Flask response rendering templates/analysis.html
    """
    # Pull injected dependencies (tests can replace these with fakes)
    deps = current_app.extensions.get("deps", {})
    fetch_one = deps["fetch_one_fn"]
    fetch_all = deps["fetch_all_fn"]

    # -----------------------------------------------------------------------
    # Q1: Number of Fall 2026 Applicants
    # -----------------------------------------------------------------------
    q1_fall_2026_count = fetch_one("""
        SELECT COUNT(*)
        FROM applicants
        WHERE term = 'Fall 2026'
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q2: Percentage of International Applicants
    # Note: NULLIF avoids division by zero when table is empty.
    # -----------------------------------------------------------------------
    q2_pct_international = fetch_one("""
        SELECT
          ROUND(
            100.0 * SUM(CASE WHEN us_or_international = 'International' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
          )
        FROM applicants
        WHERE us_or_international IS NOT NULL
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q3: Average GPA, GRE, GRE V, GRE AW
    # fetch_all returns a list of rows; we take row[0] since this query returns 1 row.
    # -----------------------------------------------------------------------
    q3_avgs = fetch_all("""
        SELECT
          ROUND(AVG(gpa)::numeric, 3),
          ROUND(AVG(gre)::numeric, 3),
          ROUND(AVG(gre_v)::numeric, 3),
          ROUND(AVG(gre_aw)::numeric, 3)
        FROM applicants
        LIMIT 1;
    """)[0]

    # -----------------------------------------------------------------------
    # Q4: Avg GPA of American Students (Fall 2026)
    # -----------------------------------------------------------------------
    q4_avg_gpa_american_fall_2026 = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE term = 'Fall 2026'
          AND us_or_international = 'American'
          AND gpa IS NOT NULL
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q5: Acceptance % for Fall 2025
    # -----------------------------------------------------------------------
    q5_pct_accepted_fall_2025 = fetch_one("""
        SELECT
          ROUND(
            100.0 * SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
          )
        FROM applicants
        WHERE term = 'Fall 2025'
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q6: Avg GPA of Accepted Applicants (Fall 2026)
    # -----------------------------------------------------------------------
    q6_avg_gpa_accepted_fall_2026 = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE term = 'Fall 2026'
          AND status = 'Accepted'
          AND gpa IS NOT NULL
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q7: JHU Masters Computer Science Applicants (LLM normalized fields)
    # -----------------------------------------------------------------------
    q7_jhu_ms_cs = fetch_one("""
        SELECT COUNT(*)
        FROM applicants
        WHERE degree ILIKE 'Master%'
          AND llm_generated_program ILIKE '%computer science%'
          AND (
                llm_generated_university ILIKE '%Johns Hopkins%'
             OR llm_generated_university ILIKE '%JHU%'
              )
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q8: Accepted PhD CS Applicants (2026) at "Top" Universities (LLM fields)
    # -----------------------------------------------------------------------
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
              )
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Q9: Same query using raw program fields (non-LLM fields)
    # -----------------------------------------------------------------------
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
              )
        LIMIT 1;
    """)

    # -----------------------------------------------------------------------
    # Custom Q10a: Top 5 Programs for Fall 2026
    # -----------------------------------------------------------------------
    custom_top5_programs_fall_2026 = fetch_all("""
        SELECT program, COUNT(*) AS application_count
        FROM applicants
        WHERE term = 'Fall 2026'
        GROUP BY program
        ORDER BY application_count DESC
        LIMIT 5;
    """)

    # -----------------------------------------------------------------------
    # Custom Q10b: Avg GPA of International Applicants
    # -----------------------------------------------------------------------
    custom_avg_gpa_international = fetch_one("""
        SELECT ROUND(AVG(gpa)::numeric, 3)
        FROM applicants
        WHERE us_or_international = 'International'
          AND gpa IS NOT NULL
        LIMIT 1;
    """)

    # Package results for the template. Keeping keys stable is important for tests.
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

    # Render template and include busy state so UI can disable/guard actions.
    return render_template(
        "analysis.html",
        results=results,
        pull_running=is_running(),
    )


@pages_bp.post("/pull-data", endpoint="pull_data_route")
def pull_data():
    if is_running():
        return jsonify({"busy": True}), 409

    # Use queue path only when broker is configured (Docker runtime).
    if os.getenv("RABBITMQ_URL"):
        try:
            publish_task("scrape_new_data", payload={})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json
            if wants_html:
                flash("Pull Data queue unavailable. Please retry.", "warning")
                return redirect(url_for("pages.analysis"))
            return jsonify({"ok": False, "error": str(exc)}), 503

        wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json
        if wants_html:
            flash("Pull Data request queued.", "success")
            return redirect(url_for("pages.analysis"))
        return jsonify({"ok": True, "queued": True, "kind": "scrape_new_data"}), 202

    # Backward-compatible test/local path.
    deps = current_app.extensions.get("deps", {})
    pull_data_fn = deps["pull_data_fn"]
    try:
        result = pull_data_fn()
        if not isinstance(result, dict):
            result = {"ok": True, "inserted": int(result) if result is not None else 0}
    except Exception as exc:  # pylint: disable=broad-exception-caught
        result = {"ok": False, "error": str(exc)}

    wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json
    if wants_html:
        if result.get("ok"):
            flash(f"Pull Data completed. Inserted: {result.get('inserted', 0)}", "success")
        else:
            flash("Pull Data failed. Check logs.", "danger")
        return redirect(url_for("pages.analysis"))

    status = 200 if result.get("ok") else 500
    return jsonify(result), status


@pages_bp.post("/update-analysis")
def update_analysis():
    if is_running():
        return jsonify({"busy": True}), 409

    # Use queue path only when broker is configured (Docker runtime).
    if os.getenv("RABBITMQ_URL"):
        try:
            publish_task("recompute_analytics", payload={})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json
            if wants_html:
                flash("Update Analysis queue unavailable. Please retry.", "warning")
                return redirect(url_for("pages.analysis"))
            return jsonify({"ok": False, "error": str(exc)}), 503

        wants_html = request.accept_mimetypes.accept_html and not request.accept_mimetypes.accept_json
        if wants_html:
            flash("Update Analysis request queued.", "success")
            return redirect(url_for("pages.analysis"))
        return jsonify({"ok": True, "queued": True, "kind": "recompute_analytics"}), 202

    # Backward-compatible test/local path.
    deps = current_app.extensions.get("deps", {})
    update_analysis_fn = deps.get("update_analysis_fn", lambda: None)
    update_analysis_fn()
    return jsonify({"ok": True}), 200
