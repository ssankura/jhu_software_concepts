# Module 6: GradCafe Microservices (Flask + Worker + Postgres + RabbitMQ)

## Overview
- This project refactors the prior SQL + Flask app into a microservice architecture.
- Services:
- `web` (Flask UI + RabbitMQ publisher)
- `worker` (RabbitMQ consumer + DB task handlers)
- `db` (PostgreSQL)
- `rabbitmq` (message broker)
- Long-running/data-modifying work is offloaded from web tier to worker tier.

## Project Structure
- `module_6/docker-compose.yml`
- `module_6/setup.py`
- `module_6/README.md`
- `module_6/docs/`
- `module_6/tests/`
- `module_6/src/data/applicant_data.json`
- `module_6/src/db/init.sql`
- `module_6/src/db/load_data.py`
- `module_6/src/web/Dockerfile`
- `module_6/src/web/requirements.txt`
- `module_6/src/web/run.py`
- `module_6/src/web/publisher.py`
- `module_6/src/web/app/`
- `module_6/src/worker/Dockerfile`
- `module_6/src/worker/requirements.txt`
- `module_6/src/worker/consumer.py`
- `module_6/src/worker/etl/incremental_scraper.py`
- `module_6/src/worker/etl/query_data.py`

## Services and Ports
- Web UI: `http://localhost:8080`
- RabbitMQ UI: `http://localhost:15672` (`guest / guest`)
- PostgreSQL: `localhost:5432` (used for local pytest)

## Docker Compose
- Start all services:
- `docker compose up --build`
- Stop services:
- `docker compose down`
- Show running services:
- `docker compose ps`

## Messaging Architecture
- Publisher code: `src/web/publisher.py`
- Consumer code: `src/worker/consumer.py`
- Durable exchange: `tasks` (direct)
- Durable queue: `tasks_q`
- Routing key: `tasks`
- Persistent messages: `delivery_mode=2`

## Flask Integration
- `POST /pull-data` publishes task kind `scrape_new_data`
- `POST /update-analysis` publishes task kind `recompute_analytics`
- JSON/API behavior:
- return `202` when queued
- return `503` when publish fails
- Browser form behavior:
- redirect + flash messages for queued/failure states

## Worker Behavior
- Connects to RabbitMQ using `RABBITMQ_URL`
- Declares durable AMQP entities (idempotent declarations)
- Sets backpressure via `basic_qos(prefetch_count=1)`
- Routes task JSON by `kind`:
- `scrape_new_data` -> `handle_scrape_new_data(conn, payload)`
- `recompute_analytics` -> `handle_recompute_analytics(conn, payload)`
- Per-message DB transaction:
- open DB using `DATABASE_URL`
- commit on success
- rollback on failure
- Ack semantics:
- `basic_ack` after successful commit
- `basic_nack(requeue=False)` on handler error

## Watermark and Idempotence
- Watermark table in `src/db/init.sql`:
- `ingestion_watermarks(source, last_seen, updated_at)`
- Scrape task logic:
- reads `payload["since"]` or stored `last_seen`
- fetches newer rows
- normalizes to `applicants` schema
- batch inserts with parameterized SQL
- idempotence via `ON CONFLICT (url) DO NOTHING`
- updates watermark to max seen key after success

## Local Setup
- Create and activate virtual environment:
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- Install dependencies:
- `python -m pip install --upgrade pip`
- `python -m pip install -r src/web/requirements.txt -r src/worker/requirements.txt`

## Local Quality Checks
- Export DB URL:
- `export DATABASE_URL="postgresql://app:app@localhost:5432/appdb"`
- Run tests with coverage:
- `PYTHONPATH=src/web:src/worker:src/db python -m pytest --cov=src/web --cov=src/worker --cov=src/db/load_data.py --cov-report=term-missing --cov-fail-under=100`
- Run pylint:
- `PYTHONPATH=src/web:src/worker:src/db python -m pylint src/web src/worker src/db --fail-under=10`

## CI Workflow
- File: `.github/workflows/module6-ci.yml`
- CI jobs:
- pylint
- pytest + 100% coverage gate
- dependency graph generation
- snyk test (skips if `SNYK_TOKEN` not set)

## Docker Images
- Docker Hub repo: `<DOCKERHUB_USER>/module_6`
- Web image: `<DOCKERHUB_USER>/module_6:web-v1`
- Worker image: `<DOCKERHUB_USER>/module_6:worker-v1`
- Build and push:
- `docker login`
- `docker build -t <DOCKERHUB_USER>/module_6:web-v1 src/web`
- `docker build -t <DOCKERHUB_USER>/module_6:worker-v1 src/worker`
- `docker push <DOCKERHUB_USER>/module_6:web-v1`
- `docker push <DOCKERHUB_USER>/module_6:worker-v1`
- Pull verify:
- `docker pull <DOCKERHUB_USER>/module_6:web-v1`
- `docker pull <DOCKERHUB_USER>/module_6:worker-v1`

## Known Limitation: Button Response UX
- `Pull Data` and `Update Analysis` are asynchronous queue endpoints.
- Current behavior returns JSON confirmation payloads (for example: `{"ok": true, "queued": true, "kind": "scrape_new_data"}`) instead of always redirecting to the home/analysis page.

## Impact
- Backend functionality is correct.
- Tasks are published to RabbitMQ.
- Worker consumes and processes tasks.
- Database updates occur as expected.
- Assignment intent is satisfied for asynchronous processing (`202` queued behavior).

## Future Improvement
- Force HTML-form redirect behavior after enqueue for browser button clicks.
- Keep JSON response behavior for API-style requests.

## Docker Hub Images

- Docker Hub repository:
- `https://hub.docker.com/r/ssankura/module_6`

- Published image tag:
- `ssankura/module_6:v1`

- Image digest:
- `sha256:94dfe53d50a364db99684ca56ea5e4095d1d451cab96b9f949a91a3edce79f17`

## Build, Push, and Pull Commands

- Build:
- `docker build -t ssankura/module_6:v1 -f src/web/Dockerfile src/web`

- Login:
- `docker login`

- Push:
- `docker push ssankura/module_6:v1`

- Pull verification:
- `docker pull ssankura/module_6:v1`

## Verification

- Push completed successfully to Docker Hub.
- Pull completed successfully from Docker Hub.
- Tag `v1` is visible on repository page.


## Submission Checklist
- Docker/Compose install verified
- Multi-service scaffold complete
- RabbitMQ publisher/consumer flow implemented
- Watermark/idempotent ingestion implemented
- Compose build/run verified
- Tests passing with 100% coverage
- Pylint 10/10
- Module 6 CI passing
- Docker Hub links added
- Screenshots/PDF included (web + RabbitMQ)
- Final zip + GitHub link + Docker Hub link submitted
