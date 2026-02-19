API Reference
=============

This section is generated automatically from Python docstrings using
Sphinx autodoc.

It documents the core ETL modules, database helpers, and Flask
application routes used in the GradCafe Analytics project.

Core ETL Modules
----------------

pull_data.py
~~~~~~~~~~~~

Responsible for orchestrating the data pipeline:
- Scrapes GradCafe data
- Cleans and transforms records
- Loads them into the database

.. automodule:: pull_data
   :members:
   :undoc-members:
   :show-inheritance:


load_data.py
~~~~~~~~~~~~

Handles inserting cleaned data into PostgreSQL.

Responsibilities:
- Insert applicants into DB
- Enforce idempotency via ON CONFLICT
- Return inserted row counts

.. automodule:: load_data
   :members:
   :undoc-members:
   :show-inheritance:


query_data.py
~~~~~~~~~~~~~

Provides reusable SQL query helpers for analytics.

Responsibilities:
- Execute analytics queries
- Return aggregate values
- Support reporting layer

.. automodule:: query_data
   :members:
   :undoc-members:
   :show-inheritance:


Flask Application & Web Layer
------------------------------

app package (create_app factory)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Defines the Flask application factory using dependency injection.

Responsibilities:
- Register blueprints
- Inject scraper/loader/update dependencies
- Configure testing hooks

.. automodule:: app.__init__
   :members:
   :undoc-members:
   :show-inheritance:


app.pages.analysis
~~~~~~~~~~~~~~~~~~

Defines the main web routes for:

- `/analysis`
- `/pull-data`
- `/update-analysis`

Responsibilities:
- Execute SQL queries
- Format analytics results
- Enforce busy-state locking
- Return JSON or HTML responses

.. automodule:: app.pages.analysis
   :members:
   :undoc-members:
   :show-inheritance:


app.db
~~~~~~

Database helper functions.

Responsibilities:
- fetch_one()
- fetch_all()
- Execute SQL safely
- Return scalar or row results

.. automodule:: app.db
   :members:
   :undoc-members:
   :show-inheritance:


app.pages.pull_state
~~~~~~~~~~~~~~~~~~~~

Implements file-based locking to prevent concurrent data pulls.

Responsibilities:
- is_running()
- start()
- stop()

.. automodule:: app.pages.pull_state
   :members:
   :undoc-members:
   :show-inheritance:
