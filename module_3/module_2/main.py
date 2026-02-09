"""
main.py

Module 2 orchestrator script:
1) Scrape GradCafe survey pages (scrape.py)
2) Parse/clean the HTML into structured records (clean.py)
3) Save results into applicant_data.json (JSON)

Key improvements:
- Uses incremental saving (crash-safe)
- Deduplicates by overview_url (prevents repeated entries)
- Prints progress clearly
- Keeps missing data consistent (""), handled in clean.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from scrape import scrape_data
from clean import clean_data

OUTPUT_FILE = Path(__file__).resolve().parent / "applicant_data.json"


def load_data(file_path: Path) -> list[dict[str, Any]]:
    """
    Load previously saved applicant records.

    Args:
        file_path: Path to JSON file.

    Returns:
        List of dict records. Returns empty list if file doesn't exist.
    """
    if not file_path.exists():
        return []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        # If file exists but is corrupted, do not crash; start fresh.
        print(f"[main.py] WARNING: {file_path} is not valid JSON. Starting with empty dataset.")
        return []


def save_data(records: list[dict[str, Any]], file_path: Path) -> None:
    """
    Save applicant records to JSON.

    Args:
        records: List of dict records.
        file_path: Output JSON path.
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def _record_key(rec: dict[str, Any]) -> str:
    """
    Create a stable unique key for a record.

    Primary: overview_url (best unique identifier)
    Fallback: combination of university+program+date_added+status

    Returns:
        A string key.
    """
    url = (rec.get("overview_url") or "").strip()
    if url:
        return url

    # Fallback (less reliable, but prevents lots of duplicates)
    university = (rec.get("university") or "").strip()
    program = (rec.get("program") or "").strip()
    date_added = (rec.get("date_added") or "").strip()
    status = (rec.get("applicant_status") or "").strip()

    return f"{university}||{program}||{date_added}||{status}"


def run_scraper(
    start_page: int = 1,
    end_page: int = 1200,
    sleep_seconds: float = 1.0,
    save_every_pages: int = 10,
    max_empty_pages_in_a_row: int = 25,
) -> list[dict[str, Any]]:
    """
    Scrape pages and build the full dataset.

    Args:
        start_page: First page to scrape (1-based).
        end_page: Last page to scrape (inclusive).
        sleep_seconds: Delay between requests.
        save_every_pages: Save JSON after this many pages (crash-safe).
        max_empty_pages_in_a_row: If many pages return zero records, stop early.

    Returns:
        A list of unique applicant records.
    """
    # Load existing data so re-runs can continue without losing progress
    repeat_streak = 0
    last_first = None
    last_last = None
    records = load_data(OUTPUT_FILE)
    seen = { _record_key(r) for r in records }

    print(f"[main.py] Loaded {len(records)} existing records from {OUTPUT_FILE.name}")

    empty_streak = 0

    print(f"[main.py] start_page={start_page}, end_page={end_page}")

    for page in range(start_page, end_page + 1):
        html = scrape_data(page)
        if html is None:
            print(f"[main.py] Page {page}: fetch failed (skipping).")
            time.sleep(sleep_seconds)
            continue

        page_records = clean_data(html)

        if page_records:
            first_url = page_records[0].get("overview_url", "")
            last_url = page_records[-1].get("overview_url", "")

            if first_url == last_first and last_url == last_last and first_url and last_url:
                repeat_streak += 1
            else:
                repeat_streak = 0

            last_first, last_last = first_url, last_url

            if repeat_streak >= 5:
                print("[main.py] Stopping: same first/last URLs repeated 5 pages in a row (pagination not advancing).")
                break

        # If parsing returned nothing, track streak
        if not page_records:
            empty_streak += 1
            print(f"[main.py] Page {page}: 0 records (empty streak={empty_streak}).")
        else:
            empty_streak = 0

        # Add only new (deduped) records
        added = 0
        for rec in page_records:
            key = _record_key(rec)
            if key in seen:
                continue
            seen.add(key)
            records.append(rec)
            added += 1

        print(f"[main.py] Page {page}: parsed={len(page_records)}, added={added}, total={len(records)}")

        # Save periodically
        if page % save_every_pages == 0:
            save_data(records, OUTPUT_FILE)
            print(f"[main.py] ✅ Saved checkpoint: {len(records)} records -> {OUTPUT_FILE.name}")

        # Optional early stop if we keep seeing empty pages
        if empty_streak >= max_empty_pages_in_a_row:
            print(f"[main.py] Stopping early after {empty_streak} empty pages in a row.")
            break

        import random
        time.sleep(0.9 + random.random() * 0.4)

    # Final save
    save_data(records, OUTPUT_FILE)
    print(f"[main.py] ✅ Done. Final save: {len(records)} records -> {OUTPUT_FILE.name}")

    return records


def main() -> None:
    """
    Entry point.

    Default: scrape pages 1..1200 (target: 30,000+ entries).
    """
    run_scraper(
        start_page=1,
        end_page=1200,
        #end_page=40,
        sleep_seconds=1.0,
        save_every_pages=10,
        max_empty_pages_in_a_row=25,
    )


if __name__ == "__main__":
    main()
