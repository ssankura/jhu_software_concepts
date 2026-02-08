"""
run.py

Entry point to run the Module 3 Flask web application locally.

How to run:
-----------
1) Ensure DATABASE_URL is set
2) python3 run.py
3) Open http://127.0.0.1:5000/analysis
"""

from app import create_app

# Create the Flask app using the application factory pattern
app = create_app()

if __name__ == "__main__":
    # debug=True is useful during development; you can set it to False for final submission.
    app.run(host="127.0.0.1", port=5000, debug=True)
