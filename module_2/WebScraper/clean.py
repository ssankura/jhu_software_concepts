"""
Module for parsing raw HTML into structured JSON data.
This module specializes in extracting and reformatting applicant data from
The GradCafe's specific table structure. It handles text normalization
and field separation (e.g., extracting Degree from Program strings).
"""

from bs4 import BeautifulSoup


def _clean_text(text: str) -> str:
    """Private method to strip whitespace and normalize strings.
    Args:
        text: The raw string extracted from an HTML element.
    Returns:
        A cleaned version of the string with all extra whitespace removed.
    """
    return " ".join(text.split()).strip() if text else ""


def clean_data(raw_html: str) -> list:
    """Converts raw HTML table rows into a structured list of dictionaries.

    This function parses the table, separates program names from degrees
    using parentheses as delimiters, and builds a standardized record for
    each applicant.

    Args:
        raw_html: The HTML source code of the survey results page.

    Returns:
        A list of dictionaries containing keys such as 'program', 'Degree',
        'status', and 'url'. Returns an empty list if no valid rows are found.
    """
    soup = BeautifulSoup(raw_html, 'html.parser')
    rows = soup.find_all('tr')
    structured_results = []

    for row in rows:
        cells = row.find_all('td')
        # Skip header rows or malformed rows that don't have enough data columns
        if len(cells) < 5:
            continue

        uni = _clean_text(cells[0].get_text())
        prog_raw = _clean_text(cells[1].get_text())

        # Logic to split "Computer Science (MS)" into "Computer Science" and "MS"
        program_name = prog_raw.split('(')[0].strip()
        degree = prog_raw.split('(')[1].replace(')', '').strip() if '(' in prog_raw else ""

        structured_results.append({
            "program": f"{program_name}, {uni} ",
            "comments": "",
            "date_added": f"Added on {_clean_text(cells[2].get_text())}",
            # Extract the href attribute from the anchor tag if it exists
            "url": "https://www.thegradcafe.com" + cells[4].find('a')['href'] if cells[4].find('a') else "",
            "status": _clean_text(cells[3].get_text()),
            "term": "Fall 2024",
            "US/International": "",
            "GPA": "",
            "Degree": degree
        })
    return structured_results