---
name: local-setup
description: Start, check, stop, or wipe the local stack for this project (Kafka, Schema Registry, Kafka UI, Postgres in Docker Compose). Use when the user wants the platform running, when a service is unreachable, or before any step that needs Kafka or Postgres.
---

# Local setup

The stack is defined in `docker-compose.yml` and driven by the `Makefile`. Run everything from the repository root.

## Before starting

- Docker Desktop must be running, with a memory limit of 8 GB. `docker info --format '{{.MemTotal}}'` prints roughly 8 billion.
- The shell must not have another project's virtual environment active (the prompt would show a prefix such as `(server)`). If it does, run `deactivate`.
- `uv sync` creates `.venv/` on Python 3.12 if it does not exist.

Kafka does not start with Docker Desktop. It has to be started with `make up`.

## Commands

| Command | Effect |
|---|---|
| `make up` | Start all four containers in the background. Safe to repeat. |
| `make ps` | List containers and their status. |
| `make down` | Delete the containers. Data on the volumes is kept. |
| `docker compose down -v` | Delete the containers and the volumes. This wipes every topic and the Postgres database; confirm with the user first. |
| `make lint` | `uv run ruff check .` |
| `make test` | `uv run pytest` |

## Services and addresses

| Service | From the Mac | From another container |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:29092` |
| Schema Registry | `http://localhost:8081` | `http://schema-registry:8081` |
| Kafka UI | `http://localhost:8080` | — |
| Postgres | `localhost:5432` | `postgres:5432` |

Postgres user, password and database are all `streamml`.

Kafka has two client listeners because a client uses whatever address the broker advertises: `HOST` advertises `localhost:9092`, `DOCKER` advertises `kafka:29092`. A client given the wrong one connects, then fails with "Connection to node 1 (localhost/127.0.0.1:9092) could not be established".

## Health checks

Run after `make up`. Kafka needs about five seconds, Kafka UI and Schema Registry about twenty.

```bash
docker compose ps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
curl -s http://localhost:8081/subjects
docker compose exec -T postgres psql -U streamml -c "select 1;"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/clusters
```

Expected: four containers "Up"; a topic list that includes `_schemas`; a JSON list from Schema Registry (`[]` when no schemas are registered); one row from Postgres; `200` from Kafka UI.

## Known issues

- **Kafka UI, Brokers → broker 1 → Metrics goes blank.** No metrics collection is configured, and the page does not handle the empty reply. Ignore it. Do not enable JMX on the broker to fix it without testing the CLI tools afterwards.
- **A config edit seems to have no effect.** Check that the file was saved: compare the file on disk with what the user describes. `docker compose config --quiet` validates the Compose file and `make -n <target>` dry-runs a Makefile target, both without changing anything.
- **Changing a service's settings recreates its container.** Kafka and Postgres data survive because they are on the `kafka-data` and `postgres-data` volumes.
- **Containers and volumes that are not in this project's Compose file** belong to other projects. Leave them alone.

## How this project is worked on

This is a guided learning project. Give the user the commands to run, and do not run state-changing commands (`make up`, `make down`, anything that writes) unless the user asks for that on the step in hand. The read-only checks above are fine to run.
