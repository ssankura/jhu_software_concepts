"""
clean.py

Parse raw GradCafe survey HTML into structured applicant records.

Goals (Module 2 requirements):
- No remnant HTML in values
- Consistent missing values ("")
- Extracts:
  program, university, comments, date_added, overview_url,
  applicant_status, status_date, start_term,
  citizenship, gre_general, gre_verbal, gre_aw,
  degree_level, gpa
"""

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


BASE_SITE = "https://www.thegradcafe.com"


# ----------------------------
# Helpers
# ----------------------------

def _clean_text(text: str) -> str:
    """Normalize whitespace and strip."""
    return " ".join(text.split()).strip() if text else ""


def _first_match(pattern: str, text: str, flags=0) -> str:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else ""


def _extract_term(text: str) -> str:
    # e.g., "Fall 2026", "Spring 2025"
    return _first_match(r"\b(Fall|Spring|Summer|Winter)\s+(20\d{2})\b", text, re.IGNORECASE).title()


def _extract_citizenship(text: str) -> str:
    t = text.lower()
    if "international" in t:
        return "International"
    if "american" in t or "u.s." in t or "us citizen" in t or "domestic" in t:
        return "American"
    if "other" in t:
        return "Other"
    return ""


def _extract_gpa(text: str) -> str:
    # common formats: "GPA: 3.85" / "GPA 3.7"
    gpa = _first_match(r"\bGPA[:\s]*([0-4]\.\d{1,2})\b", text, re.IGNORECASE)
    return gpa


def _extract_degree(text: str) -> str:
    t = text.lower()

    # prefer explicit words first
    if re.search(r"\b(phd|ph\.d|doctorate|doctoral)\b", t):
        return "PhD"
    if re.search(r"\b(master|masters|m\.s|ms\b|m\.a|ma\b|m\.sc|msc\b|meng|mengg)\b", t):
        return "Masters"

    # sometimes degree is in parentheses like "(MS)" "(PhD)"
    paren = _first_match(r"\(([^)]{1,10})\)", text)
    if paren:
        p = paren.lower()
        if "phd" in p:
            return "PhD"
        if p in {"ms", "m.s", "ma", "m.a", "msc", "m.sc", "meng"}:
            return "Masters"

    return ""


def _extract_gre(text: str) -> tuple[str, str, str]:
    """
    Return (gre_general, gre_verbal, gre_aw) as strings.
    - gre_general is typically total 260-340 if present
    - gre_verbal is 130-170 if labeled V/Verbal
    - gre_aw is 0.0-6.0 if labeled AW/Writing
    """
    t = text

    # Verbal (often like "V: 160" or "160V")
    gre_v = ""
    m = re.search(r"\b(?:V|Verbal)[:\s]*([13]\d{2}|170)\b", t, re.IGNORECASE)
    if m:
        gre_v = m.group(1)
    else:
        m2 = re.search(r"\b([13]\d{2}|170)\s*V\b", t, re.IGNORECASE)
        if m2:
            gre_v = m2.group(1)

    # AW (often "AW: 4.5" or "4.0AW")
    gre_aw = ""
    m = re.search(r"\b(?:AW|Writing)[:\s]*([0-6](?:\.\d)?)\b", t, re.IGNORECASE)
    if m:
        gre_aw = m.group(1)
    else:
        m2 = re.search(r"\b([0-6](?:\.\d)?)\s*AW\b", t, re.IGNORECASE)
        if m2:
            gre_aw = m2.group(1)

    # GRE total (often "GRE: 325" or just "325")
    gre_total = ""
    m = re.search(r"\bGRE[:\s]*([2-3]\d{2})\b", t, re.IGNORECASE)
    if m:
        gre_total = m.group(1)
    else:
        # sometimes the only 3-digit number is the total
        # Keep it conservative: pick first 260-340
        m2 = re.search(r"\b(2[6-9]\d|3[0-3]\d|340)\b", t)
        if m2:
            gre_total = m2.group(1)

    return gre_total, gre_v, gre_aw


def _extract_status_and_date(text: str) -> tuple[str, str]:
    """
    status examples:
    - "Accepted 23 Jan"
    - "Rejected 10 Feb"
    - "Wait listed 5 Mar"
    """
    status = ""
    status_date = ""

    # Status word (best effort)
    if re.search(r"\baccepted\b", text, re.IGNORECASE):
        status = "Accepted"
    elif re.search(r"\brejected\b", text, re.IGNORECASE):
        status = "Rejected"
    elif re.search(r"\bwait\s*list(?:ed)?\b|\bwaitlisted\b", text, re.IGNORECASE):
        status = "Wait listed"
    elif re.search(r"\binterview\b", text, re.IGNORECASE):
        status = "Interview"
    else:
        status = _clean_text(text)

    # Date pattern like "23 Jan" or "5 Feb"
    m = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3})\b", text)
    if m:
        status_date = m.group(1)

    return status, status_date


# ----------------------------
# Public API
# ----------------------------

def clean_data(raw_html: str) -> list[dict]:
    """
    Convert a survey page HTML into list of structured records.
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    rows = soup.find_all("tr")

    results: list[dict] = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        # Extract a result link from anywhere in the row (more robust than fixed index)
        link_tag = row.find("a", href=re.compile(r"^/result/"))
        overview_url = urljoin(BASE_SITE, link_tag["href"]) if link_tag and link_tag.get("href") else ""

        cell_texts = [_clean_text(td.get_text(" ", strip=True)) for td in cells]
        combined_text = " ".join(cell_texts)

        # Heuristics:
        # Typically the first two cells are University and Program, but we keep it safe:
        university = cell_texts[0] if len(cell_texts) >= 1 else ""
        program_raw = cell_texts[1] if len(cell_texts) >= 2 else ""

        # Find a "date added" cell: often includes month name or looks like "January 24, 2026"
        date_added = ""
        for t in cell_texts:
            if re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b", t):
                date_added = t
                break

        # Find status cell: contains Accepted/Rejected/Wait listed/Interview
        status_cell = ""
        for t in cell_texts:
            if re.search(r"\b(accepted|rejected|wait\s*list|waitlisted|interview)\b", t, re.IGNORECASE):
                status_cell = t
                break

        applicant_status, status_date = _extract_status_and_date(status_cell)

        # Comments: pick the "largest" leftover text that is not program/university/date/status
        comments = ""
        leftovers = []
        for t in cell_texts:
            if t and t not in {university, program_raw, date_added, status_cell}:
                leftovers.append(t)
        if leftovers:
            comments = max(leftovers, key=len)

        # Extract fields from program_raw + comments + status cell (best chance)
        parse_blob = " ".join([program_raw, comments, status_cell])

        start_term = _extract_term(parse_blob)
        citizenship = _extract_citizenship(parse_blob)
        gpa = _extract_gpa(parse_blob)
        degree_level = _extract_degree(program_raw)

        gre_general, gre_verbal, gre_aw = _extract_gre(parse_blob)

        # Always output consistent keys; missing => ""
        results.append({
            "program": program_raw,
            "university": university,
            "comments": comments,
            "date_added": date_added,
            "overview_url": overview_url,
            "applicant_status": applicant_status,
            "status_date": status_date,
            "start_term": start_term,
            "citizenship": citizenship,
            "gre_general": gre_general,
            "gre_verbal": gre_verbal,
            "gre_aw": gre_aw,
            "degree_level": degree_level,
            "gpa": gpa,
        })

    return results
