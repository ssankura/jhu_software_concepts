"""
run.py

Local development entry point for the GradCafe Flask web application.

Purpose:
--------
This script initializes and runs the Flask app using the
application factory defined in app/__init__.py.

How to run:
-----------
1) Ensure DATABASE_URL is set:
   export DATABASE_URL="postgresql://user:pass@localhost:5432/gradcafe"

2) Start the server:
   python3 run.py

3) Open in browser:
   http://127.0.0.1:5000/analysis

Notes:
------
- This file is for local development only.
- In production, a WSGI server (e.g., gunicorn) would import `app`
  instead of running Flask’s built-in server.
"""

from web.app import create_app


# ============================================================================
# Application Initialization
# ----------------------------------------------------------------------------
# The application factory pattern:
# - Allows multiple independent app instances (important for testing)
# - Enables dependency injection (used heavily in Module 4)
# - Keeps configuration separate from runtime execution
# ============================================================================

app = create_app()


# ============================================================================
# Local Development Server
# ----------------------------------------------------------------------------
# This block runs only when this file is executed directly:
#     python3 run.py
#
# It does NOT execute when:
# - Imported by pytest
# - Imported by gunicorn or another WSGI server
# ============================================================================

if __name__ == "__main__":

    # debug=True:
    # - Enables automatic reload on file changes
    # - Provides interactive debugger on errors
    # - Should be set to False in production environments
    app.run(host="127.0.0.1", port=8080, debug=False)
