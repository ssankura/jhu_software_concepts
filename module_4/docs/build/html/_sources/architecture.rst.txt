Architecture
============

This project follows a layered architecture separating the Web layer,
ETL layer, and Database layer. This design improves testability,
maintainability, and modularity.

System Overview
---------------

The application consists of three main layers:

1. Web Layer (Flask)
2. ETL Layer (Scrape → Transform → Load)
3. Database Layer (PostgreSQL)

These layers communicate through clearly defined interfaces.

------------------------------------

Web Layer
---------

Location:
    src/app/

Responsibilities:

- Exposes Flask routes
- Renders HTML templates
- Handles user interaction (Pull Data, Update Analysis)
- Enforces busy-state locking
- Formats analytics results for display

Key Components:

- app.__init__.py → create_app() factory (dependency injection)
- app.pages.analysis → Web routes
- app.pages.pull_state → File-based lock mechanism
- templates/analysis.html → UI rendering

The Web layer does NOT directly access the database.
Instead, it uses injected fetch functions for testability.

------------------------------------

ETL Layer
---------

Location:
    src/pull_data.py
    src/load_data.py
    src/query_data.py

Responsibilities:

- Scrape GradCafe data
- Clean and standardize fields
- Insert rows into PostgreSQL
- Execute analytics queries

The ETL layer is fully testable independent of Flask.

------------------------------------

Database Layer
--------------

Location:
    src/app/db.py

Responsibilities:

- Execute SQL queries
- Provide fetch_one() and fetch_all() helpers
- Return scalar or row-based results
- Isolate SQL from business logic

The database schema enforces:

- Unique URL constraint
- Idempotent inserts (ON CONFLICT DO NOTHING)

------------------------------------

Concurrency & Locking
---------------------

The system prevents concurrent data pulls using a file-based lock:

- app.pages.pull_state
- Lock file: .pull_data.lock

If a pull is running:
- /pull-data returns HTTP 409
- /update-analysis returns HTTP 409
- Buttons are disabled in UI

------------------------------------

Testing Strategy
----------------

The architecture enables 100% test coverage through:

- Dependency injection in create_app()
- Fake scraper/loader injection in tests
- Database fixtures for integration tests
- Busy-state monkeypatching

This separation of concerns ensures the web layer,
ETL layer, and database layer can be tested independently.
