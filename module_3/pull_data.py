"""
pull_data.py

Runs module_2 scraper to fetch new GradCafe data, then loads the resulting JSON
into PostgreSQL using module_3/load_data.py.

This script is intended to be launched by Flask via subprocess.
"""

import os
import subprocess
import sys
from pathlib import Path

from app.pages.pull_state import is_running, start, stop

def main() -> int:
    # If another run is truly active, bail.
    if is_running():
        print("ERROR: Pull Data appears to be already running (.pull_data.lock exists).")
        return 10

    start()
    try:
        module_3_dir = Path(__file__).resolve().parent
        module_2_dir = module_3_dir / "module_2"

        scrape_py = module_2_dir / "scrape.py"
        load_py = module_3_dir / "load_data.py"
        scraped_json = module_2_dir / "applicant_data.json"

        if "DATABASE_URL" not in os.environ:
            print("ERROR: DATABASE_URL is not set.")
            return 2

        print("Running module_2 scraper...")
        r1 = subprocess.run([sys.executable, str(scrape_py)], cwd=str(module_2_dir))
        if r1.returncode != 0:
            print("ERROR: module_2 scrape failed.")
            return r1.returncode

        if not scraped_json.exists():
            print(f"ERROR: Scraper did not produce: {scraped_json}")
            return 5

        print("Loading scraped data into PostgreSQL...")
        r2 = subprocess.run(
            [sys.executable, str(load_py), "--file", str(scraped_json)],
            cwd=str(module_3_dir),
        )
        if r2.returncode != 0:
            print("ERROR: load_data.py failed.")
            return r2.returncode

        print("SUCCESS: Pull Data complete.")
        return 0

    finally:
        stop()

if __name__ == "__main__":
    raise SystemExit(main())

