"""
pull_data.py

Runs module_2 scraper to fetch new GradCafe data, then loads the resulting JSON
into PostgreSQL using module_3/load_data.py.

This script is intended to be launched by Flask via subprocess.
"""

import os
import subprocess
from pathlib import Path
import logging

from app.pages.pull_state import stop

logging.basicConfig(
    filename="pull_data.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

def _run(cmd, cwd: Path, step_name: str) -> int:
    """Run a subprocess and log stdout/stderr to pull_data.log."""
    logger.info("Running %s: %s (cwd=%s)", step_name, cmd, cwd)
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if proc.stdout:
        logger.info("%s stdout:\n%s", step_name, proc.stdout)
    if proc.stderr:
        logger.error("%s stderr:\n%s", step_name, proc.stderr)

    return proc.returncode


def main() -> int:
    try:
        module_3_dir = Path(__file__).resolve().parent
        module_2_dir = module_3_dir / "module_2"

        # ✅ Always force venv python so dependencies match terminal
        venv_python = module_3_dir / ".venv" / "bin" / "python"
        if not venv_python.exists():
            logger.error("Venv python not found at %s", venv_python)
            print(f"ERROR: venv python not found: {venv_python}")
            return 99

        scrape_py = module_2_dir / "scrape.py"
        clean_py = module_2_dir / "clean.py"
        standardize_merge_py = module_2_dir / "standardize_merge.py"
        load_py = module_3_dir / "load_data.py"

        scraped_json = module_2_dir / "applicant_data.json"
        scraped_llm_json = module_2_dir / "applicant_data_final.json"

        if "DATABASE_URL" not in os.environ:
            logger.error("DATABASE_URL is not set in environment")
            print("ERROR: DATABASE_URL is not set.")
            return 2

        # 1) Scrape
        rc = _run([str(venv_python), str(scrape_py)], module_2_dir, "scrape.py")
        if rc != 0:
            print("ERROR: module_2 scrape failed.")
            return rc

        if not scraped_json.exists():
            logger.error("Scraper did not produce %s", scraped_json)
            print(f"ERROR: Scraper did not produce: {scraped_json}")
            return 5

        # 2) Clean
        rc = _run([str(venv_python), str(clean_py)], module_2_dir, "clean.py")
        if rc != 0:
            print("ERROR: module_2 clean failed.")
            return rc

        # 3) Standardize + Merge (LLM)
        rc = _run([str(venv_python), str(standardize_merge_py)], module_2_dir, "standardize_merge.py")
        if rc != 0:
            print("ERROR: module_2 standardize_merge failed.")
            return rc

        if not scraped_llm_json.exists():
            logger.error("standardize_merge did not produce %s", scraped_llm_json)
            print(f"ERROR: standardize_merge did not produce: {scraped_llm_json}")
            return 6

        # 4) Load into DB
        rc = _run([str(venv_python), str(load_py), "--file", str(scraped_llm_json)], module_3_dir, "load_data.py")
        if rc != 0:
            print("ERROR: load_data.py failed.")
            return rc

        print("SUCCESS: Pull Data complete.")
        logger.info("SUCCESS: Pull Data complete.")
        return 0

    finally:
        # release lock (if you are using lock elsewhere)
        stop()


if __name__ == "__main__":
    raise SystemExit(main())
