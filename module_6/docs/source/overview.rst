Overview & Setup
================

Project Overview
----------------

GradCafe Analytics is a full-stack web application that:

- Scrapes GradCafe-style applicant data
- Loads it into PostgreSQL
- Executes analytics queries
- Displays results via a Flask web interface
- Enforces concurrency safety during data pulls
- Achieves 100% automated test coverage

The system is structured into:

- Web Layer (Flask)
- ETL Layer (Pull / Load / Query)
- Database Layer (PostgreSQL helpers)

------------------------------------

Requirements
------------

- Python 3.10+
- PostgreSQL (local instance)
- Virtual environment (recommended)

------------------------------------

Environment Variables
---------------------

The application requires:

DATABASE_URL

Example:

.. code-block:: bash

   export DATABASE_URL=postgresql://graduser:grad123@localhost:5432/gradcafe

This is required for:

- Database helper functions
- Integration tests
- ETL data insertion

------------------------------------

Installation
------------

Clone repository:

.. code-block:: bash

   git clone <repo-url>
   cd module_4

Create virtual environment:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate

Install dependencies:

.. code-block:: bash

   pip install -r src/requirements.txt

------------------------------------

Running the Web Application
---------------------------

From module_4 directory:

.. code-block:: bash

   export FLASK_APP=src/run.py
   flask run

Open browser:

http://127.0.0.1:5000/analysis

------------------------------------

Running Tests
-------------

Run all tests:

.. code-block:: bash

   pytest

Run specific groups:

Integration tests:

.. code-block:: bash

   pytest -m integration

Web tests:

.. code-block:: bash

   pytest -m web

------------------------------------

Coverage Enforcement
--------------------

The project enforces strict coverage rules.

Run:

.. code-block:: bash

   pytest --cov=src --cov-report=term-missing --cov-fail-under=100

If any line is untested, pytest fails.

------------------------------------

Known Limitations
-----------------

1. Pull Data JSON Behavior

Depending on browser Accept headers,
clicking "Pull Data" may display JSON
instead of redirecting cleanly.

Using the browser back button may temporarily
leave the button disabled.

This is a UI-level limitation.

2. Requires Local PostgreSQL

The application assumes a running PostgreSQL instance.
No in-memory fallback database is provided.

------------------------------------

Documentation
-------------

Build Sphinx documentation:

.. code-block:: bash

   cd docs
   make clean
   make html

HTML output is located at:

docs/build/html/index.html
