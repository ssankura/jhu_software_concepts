Testing Guide
=============

Overview
--------

This project uses **pytest** for unit, web, and integration testing.

The test suite achieves:

- 100% test coverage
- Full web route coverage
- Database integration coverage
- Busy-state concurrency validation

------------------------------------

Running Tests
-------------

Run all tests:

.. code-block:: bash

   pytest

Run with coverage enforcement:

.. code-block:: bash

   pytest --cov=src --cov-report=term-missing --cov-fail-under=100

------------------------------------

Test Markers
------------

The test suite uses pytest markers to organize test types.

Integration tests:

.. code-block:: bash

   pytest -m integration

Web layer tests:

.. code-block:: bash

   pytest -m web

Button behavior tests:

.. code-block:: bash

   pytest -m buttons

------------------------------------

Fixtures
--------

The project uses reusable fixtures defined in:

tests/conftest.py

Key fixtures:

client
  Provides a Flask test client.

app
  Flask application with injected fake dependencies.

app_db
  Flask application wired to a real PostgreSQL database
  for integration testing.

database_url
  Provides DATABASE_URL environment variable.

db_clean
  Clears test database before integration tests.

------------------------------------

Dependency Injection
--------------------

The Flask factory (create_app) supports dependency injection.

Injected components:

- scraper_fn
- loader_fn
- update_analysis_fn
- fetch_one_fn
- fetch_all_fn
- pull_data_fn

This enables:

- Unit testing without real DB
- Integration testing with real DB
- Isolation of business logic

------------------------------------

Web Testing
-----------

Web tests verify:

- Required HTML text
- Required buttons
- Stable selectors

Required selectors include:

- data-testid="pull-data-btn"
- data-testid="update-analysis-btn"

Tests confirm:

- Proper rendering
- Proper percentage formatting (two decimals)
- Busy-state blocking returns HTTP 409

------------------------------------

Busy-State Concurrency
----------------------

The system prevents concurrent pulls using a file lock:

- app.pages.pull_state
- .pull_data.lock file

Tests simulate concurrency by:

- Monkeypatching lock file path
- Forcing busy state
- Verifying HTTP 409 responses

------------------------------------

Database Testing
----------------

Integration tests verify:

- Inserts into PostgreSQL
- Idempotent behavior (ON CONFLICT DO NOTHING)
- Correct query results
- End-to-end flow:
  pull → update → render

------------------------------------

Coverage Enforcement
--------------------

The project enforces 100% coverage via pytest.ini:

--cov=src
--cov-report=term-missing
--cov-fail-under=100

If any line is untested,
pytest will fail the run.

------------------------------------

Testing Philosophy
------------------

The architecture is designed to maximize testability through:

- Layer separation
- Dependency injection
- Explicit SQL isolation
- Deterministic fixtures
- Strict coverage enforcement

This ensures reliability and maintainability.
