"""
pull_data.py

Orchestrates the full GradCafe ETL pipeline:

    1. Scrape new data  (module_2/scrape.py)
    2. Clean raw data   (module_2/clean.py)
    3. Standardize/LLM  (module_2/standardize_merge.py)
    4. Load into DB     (module_3/load_data.py)

This script is intended to be launched by Flask via subprocess.

Design Goals:
-------------
- Keep the web layer (Flask routes) thin and orchestration-free.
- Execute ETL stages sequentially with clear logging.
- Fail fast if any stage fails.
- Ensure idempotent DB loads (handled by load_data.py UNIQUE constraint).
- Always release busy-state lock even if errors occur.

Important:
----------
This script assumes DATABASE_URL is set in the environment.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

from web.app.pages.pull_state import stop


# --- Path setup: allow autodoc to import modules from /src ---
ROOT = Path(__file__).resolve().parents[2]   # module_4/
SRC = ROOT / "src"                          # module_4/src
sys.path.insert(0, str(SRC))

# ============================================================================
# Logging Configuration
# ----------------------------------------------------------------------------
# Writes logs to pull_data.log for post-run debugging.
# We log stdout and stderr from each subprocess stage.
# ============================================================================

logging.basicConfig(
    filename="pull_data.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ============================================================================
# Subprocess Helper
# ----------------------------------------------------------------------------
# Runs a Python script as a subprocess and captures output.
# Returns exit code so caller can decide whether to stop pipeline.
# ============================================================================

def _run(cmd, cwd: Path, step_name: str) -> int:
    """
    Execute a subprocess command and log its output.

    Args:
        cmd: Command list (e.g., ["python", "script.py"])
        cwd: Working directory for execution
        step_name: Logical name of the step (for logging)

    Returns:
        int: Subprocess return code (0 = success)

    Why capture_output=True?
        Ensures stdout and stderr are logged instead of printed silently.
    """
    logger.info("Running %s: %s (cwd=%s)", step_name, cmd, cwd)

    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,  # explicitly state we are handling return code manually
    )

    if proc.stdout:
        logger.info("%s stdout:\n%s", step_name, proc.stdout)

    if proc.stderr:
        logger.error("%s stderr:\n%s", step_name, proc.stderr)

    return proc.returncode


# ============================================================================
# Main ETL Orchestration
# ----------------------------------------------------------------------------
# Executes each stage sequentially.
# If any stage fails → immediately abort and return error code.
# Always releases busy lock in finally block.
# ============================================================================

# pylint: disable=too-many-return-statements
def main() -> int:
    """
    Execute the full Pull Data pipeline.

    Returns:
        int: Exit code (0 = success, non-zero = failure)

    Flow:
        scrape → clean → standardize_merge → load_data

    Error Handling:
        - If any step returns non-zero, abort immediately.
        - Print user-friendly message for Flask logs.
        - Lock is released in finally block.
    """
    try:
        # Resolve project directories
        module_3_dir = Path(__file__).resolve().parent
        module_2_dir = module_3_dir / "module_2"

        # --------------------------------------------------------------------
        # Force use of virtual environment Python interpreter.
        #
        # Why:
        # Ensures dependencies (psycopg, Flask, etc.) match the environment
        # used during development and CI.
        # Prevents accidental use of system Python.
        # --------------------------------------------------------------------
        venv_python = module_3_dir / ".venv" / "bin" / "python"
        if not venv_python.exists():
            logger.error("Venv python not found at %s", venv_python)
            print(f"ERROR: venv python not found: {venv_python}")
            return 99

        # Define script paths
        scrape_py = module_2_dir / "scrape.py"
        clean_py = module_2_dir / "clean.py"
        standardize_merge_py = module_2_dir / "standardize_merge.py"
        load_py = module_3_dir / "load_data.py"

        # Expected JSON outputs
        scraped_json = module_2_dir / "applicant_data.json"
        scraped_llm_json = module_2_dir / "applicant_data_final.json"

        # Validate environment variable before proceeding
        if "DATABASE_URL" not in os.environ:
            logger.error("DATABASE_URL is not set in environment")
            print("ERROR: DATABASE_URL is not set.")
            return 2

        # --------------------------------------------------------------------
        # 1) Scrape
        # --------------------------------------------------------------------
        rc = _run([str(venv_python), str(scrape_py)], module_2_dir, "scrape.py")
        if rc != 0:
            print("ERROR: module_2 scrape failed.")
            return rc

        if not scraped_json.exists():
            logger.error("Scraper did not produce %s", scraped_json)
            print(f"ERROR: Scraper did not produce: {scraped_json}")
            return 5

        # --------------------------------------------------------------------
        # 2) Clean
        # --------------------------------------------------------------------
        rc = _run([str(venv_python), str(clean_py)], module_2_dir, "clean.py")
        if rc != 0:
            print("ERROR: module_2 clean failed.")
            return rc

        # --------------------------------------------------------------------
        # 3) Standardize + Merge (LLM step)
        #
        # Produces enriched JSON with normalized fields.
        # Required for advanced analysis queries.
        # --------------------------------------------------------------------
        rc = _run(
            [str(venv_python), str(standardize_merge_py)],
            module_2_dir,
            "standardize_merge.py",
        )
        if rc != 0:
            print("ERROR: module_2 standardize_merge failed.")
            return rc

        if not scraped_llm_json.exists():
            logger.error("standardize_merge did not produce %s", scraped_llm_json)
            print(f"ERROR: standardize_merge did not produce: {scraped_llm_json}")
            return 6

        # --------------------------------------------------------------------
        # 4) Load into PostgreSQL
        #
        # Uses load_data.py which:
        # - Creates table if missing
        # - Enforces UNIQUE(url)
        # - Performs batched inserts
        # --------------------------------------------------------------------
        rc = _run(
            [str(venv_python), str(load_py), "--file", str(scraped_llm_json)],
            module_3_dir,
            "load_data.py",
        )
        if rc != 0:
            print("ERROR: load_data.py failed.")
            return rc

        print("SUCCESS: Pull Data complete.")
        logger.info("SUCCESS: Pull Data complete.")
        return 0

    finally:
        # --------------------------------------------------------------------
        # Always release busy-state lock.
        #
        # Even if scraping or loading fails, we must prevent the system from
        # being stuck in a permanent "busy" state.
        # --------------------------------------------------------------------
        stop()


# Standard CLI execution guard.
# Raises SystemExit so exit codes propagate correctly.
if __name__ == "__main__":
    raise SystemExit(main())
