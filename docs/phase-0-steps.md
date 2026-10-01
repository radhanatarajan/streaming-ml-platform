# Phase 0 — Scaffold: step-by-step log

A record of each setup step: what was run, why, and how it was checked. Steps are added once they have been run and verified. The overall plan is in [roadmap.md](roadmap.md).

| Step | Status |
|---|---|
| 1. Create folder, `git init`, first commit | Done (2026-10-01) |
| 2. Create GitHub repo, push, create `develop` | Done (2026-10-01) |
| 3. Docker Desktop with 8 GB, `hello-world` | Done (2026-10-01) |
| 4. `uv` project pinned to Python 3.12 | Done (2026-10-01) |
| 5. Kafka broker in Docker Compose | Done (2026-10-01) |
| 6. Create a topic, send and read one message | Done (2026-10-01) |
| 7. Kafka UI and advertised listeners | Done (2026-10-01) |
| 8a. Permanent storage for Kafka | Done (2026-10-01) |
| 8b. Schema Registry and Postgres | Done (2026-10-01) |
| 8c. Makefile | Done (2026-10-01) |
| 9a. `ruff`, `pytest` and a first test | Done (2026-10-01) |
| 9b. CI on GitHub | Not started |
| 9c. `local_setup` skill, learning note, Phase 0 PR | Not started |

---

## Step 1 — Create the folder and the git repository

Created `~/streaming-ml-platform`, ran `git init`, and made the first commit.

## Step 2 — GitHub repository and `develop` branch

Repository: https://github.com/radhanatarajan/streaming-ml-platform

`main` holds finished phases. All work happens on `develop`, and each roadmap phase is merged into `main` through one pull request.

## Step 3 — Docker Desktop with 8 GB of memory

**Concept:** on a Mac, Docker runs every container inside one hidden Linux virtual machine. The memory limit in Docker Desktop is the size of that machine, shared by all containers.

**What to do**

1. Docker Desktop → Settings → Resources → set **Memory limit** to 8 GB → **Apply & restart**.
2. Run:

```bash
docker run hello-world
docker info --format '{{.MemTotal}}'
```

**Why:** the full stack (Kafka, Schema Registry, Kafka UI, Postgres, MLflow, our services) runs in that one virtual machine. With too little memory, containers are killed without a clear error. 8 GB leaves room on a 32 GB Mac for Ollama and the Qwen model, which run outside Docker.

**Check**

- `hello-world` prints "Hello from Docker!".
- `docker info` prints roughly 8 billion bytes.

**Observed:** "Hello from Docker!" with the `arm64v8` (Apple Silicon) image; memory `8321429504` bytes.

## Step 4 — `uv` project pinned to Python 3.12

**Concept:** the project has its own Python version and its own libraries, separate from the Mac's Python (3.14). `uv` manages both.

**What to do**

If the shell prompt shows another project's environment, for example `(server)`, run `deactivate` first.

```bash
uv init --package --name streamml --python 3.12
uv sync
uv run python --version
uv run streamml
git status
```

| Command | What it does |
|---|---|
| `uv init` | Writes `pyproject.toml` (project description and dependency list) and `.python-version` (the pin to 3.12). `--package` creates `src/streamml/`, where the project code lives. |
| `uv sync` | Downloads Python 3.12 if needed, creates `.venv/`, and writes `uv.lock`, which records the exact version of every library. |
| `uv run ...` | Runs a command inside the project's environment. Nothing needs to be activated by hand. |

**Why:** LightGBM, MLX and the Kafka client are best supported on Python 3.12. The lock file makes the project install the same way on any machine, including CI.

**Check**

- `uv run python --version` prints Python 3.12.x.
- `uv run streamml` prints "Hello from streamml!".
- `git status` lists `.python-version`, `pyproject.toml`, `src/` and `uv.lock`, and does not list `.venv/`.

**Observed:** Python 3.12.13; all checks passed.

**Commit**

```bash
git add .python-version pyproject.toml src uv.lock
git commit -m "add uv project pinned to python 3.12"
```

## Step 5 — Kafka broker in Docker Compose

**Concept:** a **broker** is the Kafka server. It receives events, stores them on disk, and hands them to programs that ask for them. A cluster also needs a **controller**, which tracks the cluster's own bookkeeping (which topics exist, which broker holds what). Modern Kafka does this itself in **KRaft mode**, with no ZooKeeper. Our single container plays both roles.

**What to do**

Create `docker-compose.yml` in the project root:

```yaml
services:
  kafka:
    image: apache/kafka:4.3.1
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Then start it and look at it:

```bash
docker compose up -d
docker compose ps
docker compose logs kafka | grep "Kafka Server started"
docker compose logs kafka | tail -n 15
```

| Setting | Meaning |
|---|---|
| `KAFKA_PROCESS_ROLES: broker,controller` | This one container is both the broker and the controller. |
| `KAFKA_NODE_ID: 1` | Its ID in the cluster. `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093` says the only controller is node 1. |
| `KAFKA_LISTENERS` | The ports Kafka opens: 9092 for clients (our programs), 9093 for controller traffic. |
| `ports: "9092:9092"` | Makes port 9092 inside the container reachable from the Mac at `localhost:9092`. |
| The three settings ending in `_FACTOR: 1` / `MIN_ISR: 1` | Kafka normally keeps three copies of its internal data across three brokers. With one broker, one copy is enough. |

`KAFKA_ADVERTISED_LISTENERS` is explained in step 7.

**Check**

- `docker compose up -d` ends with "Container kafka Started".
- `docker compose ps` shows `kafka` with status "Up" and `0.0.0.0:9092->9092/tcp`.
- The `grep` command prints one line containing "Kafka Server started".
- The last 15 log lines contain no `ERROR` lines.

**Observed:** all checks passed. Kafka 4.3.1 started in under a second after the image was pulled. Log lines worth recognising:

| Log line | Meaning |
|---|---|
| `The broker has been unfenced. Transitioning from RECOVERY to RUNNING` | The controller has accepted the broker into the cluster. A "fenced" broker is one that is not yet allowed to serve clients. |
| `Awaiting socket connections on 0.0.0.0:9092` | The client port is open. |
| `Kafka Server started` | Startup is complete. |

Log timestamps are in UTC, not local time.

Committed together with step 7.

## Step 6 — Create a topic, send and read one message

**Concept:** a **topic** is a named stream of messages. A **producer** writes to it and a **consumer** reads from it; both are clients that connect to the broker and never talk to each other directly. A topic is an append-only log on disk. Reading a message does not remove it, so any consumer can read the topic again from the beginning.

**What to do**

The Kafka image ships with command-line tools in `/opt/kafka/bin/`. `docker compose exec kafka ...` runs a command inside the running container.

Create a topic and look at it:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic hello --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello
```

Send messages. At the `>` prompt type a line and press Enter; press Ctrl+C to exit.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello
```

Read them back. The command keeps waiting for new messages; press Ctrl+C to stop. Running it a second time prints the same messages again.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello --from-beginning
```

`--bootstrap-server` is the address of the broker a client connects to first. Every Kafka client needs one.

**Check**

- Create prints "Created topic hello."
- Describe shows `PartitionCount: 1` and `Partition: 0  Leader: 1`.
- The consumer prints the messages, then "Processed a total of 2 messages" after Ctrl+C.

**Observed:** all checks passed with two messages. Reading the describe output:

| Field | Meaning |
|---|---|
| `PartitionCount: 1` | The topic has one partition, numbered 0. |
| `ReplicationFactor: 1` | One copy of the data. |
| `Leader: 1` | Broker 1 handles reads and writes for this partition. |
| `Replicas: 1` | The brokers that hold a copy: only broker 1. |
| `Isr: 1` | "In-sync replicas": the copies that are fully up to date. |

Messages that are not errors:

- "The consumer rebalance protocol (KIP-848) is production-ready!" is an advertisement for a newer way consumer groups coordinate. Nothing to do.
- "Debug this Compose error with Gordon" is printed by Docker Desktop because Ctrl+C ends a command with a non-zero exit code. Nothing failed.

Where the data lives: inside the container at `/tmp/kafka-logs/hello-0`, one folder per topic partition. It is not on a Docker volume, so it is lost when the container is deleted or recreated.

## Step 7 — Kafka UI and advertised listeners

**Concept:** a Kafka client connects in two stages. It first connects to the **bootstrap address** and asks where the brokers are. The broker replies with its **advertised address**, and the client uses that address for everything after. The advertised address must therefore be one the client can reach from where it runs.

### 7a — Add Kafka UI and watch it fail

Added a second service to `docker-compose.yml`:

```yaml
  kafka-ui:
    image: kafbat/kafka-ui:v1.5.0
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      - kafka
```

```bash
docker compose up -d
docker compose logs kafka-ui | grep "could not be established" | tail -n 3
```

On the Compose network each service is reachable by its service name, so Kafka UI reaches the broker at `kafka`, not `localhost`. `depends_on` starts `kafka` first.

**Observed:** http://localhost:8080 loaded, but the `local` cluster showed Broker Count 0 and "No Active Controller". The log showed:

```
Connection to node 1 (localhost/127.0.0.1:9092) could not be established.
```

Kafka UI reached the broker at `kafka:9092`, was told the broker's advertised address is `localhost:9092`, and then tried that. Inside the Kafka UI container, `localhost` is the Kafka UI container itself.

A lesson from this step: `docker compose up -d` reads the file on disk. Save `docker-compose.yml` before running it. If the output says `up 1/1` when two services are expected, the file was not saved.

### 7b — Two listeners

Programs on the Mac need the address `localhost:9092`; other containers need `kafka:<port>`. One listener advertises one address, so the broker gets two client listeners.

Changed in the `kafka` service:

```yaml
      KAFKA_LISTENERS: DOCKER://:29092,HOST://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: DOCKER://kafka:29092,HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,DOCKER:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
```

Changed in the `kafka-ui` service:

```yaml
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
```

| Line | Meaning |
|---|---|
| `KAFKA_LISTENERS` | Kafka opens three ports. `DOCKER` and `HOST` are names we chose. |
| `KAFKA_ADVERTISED_LISTENERS` | A client connecting on the `DOCKER` port is told to use `kafka:29092`; a client connecting on the `HOST` port is told to use `localhost:9092`. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Every listener name needs a security setting. All three are unencrypted. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | The listener brokers use to talk to each other. Required once listeners have custom names. |

| Client runs | Bootstrap address to use |
|---|---|
| On the Mac (our Python programs) | `localhost:9092` |
| In another container | `kafka:29092` |
| Inside the Kafka container (the CLI tools) | `localhost:9092` |

Port 29092 is not published in `ports:` because only containers on the same Docker network use it.

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Changing a container's settings makes Compose delete and recreate it, so the `hello` topic was lost and had to be created again (step 6 commands).

**Check**

- The dot next to `local` in Kafka UI is green; Brokers shows Broker Count 1 and an active controller.
- Topics → hello → Messages shows the messages.

**Observed:** Broker Count 1, Active Controller 1, Controller Type KRaft, broker listed as host `kafka` port `29092`. The `hello` topic was recreated with 3 messages. "Version: Unknown" is a display gap in Kafka UI, not a fault.

Committed together with step 8a.

## Step 8a — Permanent storage for Kafka

**Concept:** a container's files are deleted with the container. A **volume** is storage that Docker manages outside any container. Attaching one to Kafka's data folder makes topics and messages outlive the container.

**What to do**

Three additions to `docker-compose.yml`. In the `kafka` service, one more environment line and a `volumes:` block:

```yaml
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
```

At the end of the file, at the left edge:

```yaml
volumes:
  kafka-data:
```

| Line | Meaning |
|---|---|
| `KAFKA_LOG_DIRS` | Where Kafka stores topic data. Kafka calls its stored messages "logs"; this is the data folder, not a folder of log files. |
| `kafka-data:/var/lib/kafka/data` | Attaches the volume `kafka-data` to that folder in the container. Format: `volume:folder`. |
| Top-level `volumes:` | Declares the volume so Compose creates it. |

`volumes:` appears at two levels on purpose: inside a service it attaches a volume, at the left edge it declares one. In YAML the indentation is the structure. A top-level block indented by two spaces would be read as a service named "volumes".

Check the file before starting anything:

```bash
docker compose config --quiet && echo "file is valid"
```

"Valid" means well-formed, not correct: a file with the volume declared but not attached is also valid.

Apply, create a topic, then delete and recreate the containers:

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic hello --partitions 1 --replication-factor 1
docker compose down
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker volume ls
```

| Command | Containers | Volumes |
|---|---|---|
| `docker compose stop` | Stopped, kept | Kept |
| `docker compose down` | Deleted | Kept |
| `docker compose down -v` | Deleted | Deleted (a deliberate full wipe) |

**Check**

- After `down` and `up -d`, the list still prints `hello`.
- `docker volume ls` includes `streaming-ml-platform_kafka-data`.

**Observed:** both checks passed. The volumes with long random names in `docker volume ls` are anonymous volumes left by earlier containers; `infra_db_data` belongs to another project.

**Commit**

```bash
git add docker-compose.yml docs/phase-0-steps.md
git commit -m "add kafka broker, kafka ui and kafka data volume to docker compose"
```

## Step 8b — Schema Registry and Postgres

**Concept:** two supporting services. **Schema Registry** is a small web service that stores the agreed shape of each kind of message (its fields and their types), so a producer cannot silently change a message and break its consumers. It keeps its data in a Kafka topic named `_schemas`. **Postgres** is the database that will hold features, forecasts and enrichment results for the API and the dashboard.

**What to do**

Added to `docker-compose.yml`:

```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:8.3.2
    container_name: schema-registry
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - kafka
```

```yaml
  postgres:
    image: postgres:17.11
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: streamml
      POSTGRES_PASSWORD: streamml
      POSTGRES_DB: streamml
    volumes:
      - postgres-data:/var/lib/postgresql/data
```

In `kafka-ui`: one more environment line, `KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081`, and `schema-registry` added to `depends_on`. In the top-level `volumes:` block: `postgres-data:`.

| Line | Meaning |
|---|---|
| `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092` | Schema Registry is a container, so it uses the `DOCKER` listener. |
| `SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1` | One copy of the `_schemas` topic, because there is one broker. |
| `KAFKA_CLUSTERS_0_SCHEMAREGISTRY` | Tells Kafka UI where Schema Registry is. |
| `POSTGRES_USER` / `PASSWORD` / `DB` | Created on first start. The password is in the file because this database only runs on a laptop. |

```bash
docker compose config --quiet && echo "file is valid"
docker compose up -d
docker compose ps
curl -s http://localhost:8081/subjects
docker compose exec postgres psql -U streamml -c "select version();"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Check**

- Four containers, all "Up".
- `curl` prints `[]` (running, no schemas yet).
- `psql` prints "PostgreSQL 17.11 ...".
- The topic list includes `_schemas`.
- Kafka UI's menu includes "Schema Registry".

**Observed:** all checks passed. Kafka UI shows 52 partitions: 50 for `__consumer_offsets` (Kafka's own topic for remembering how far each consumer has read), 1 for `_schemas`, 1 for `hello`. Memory in use: Kafka about 350 MB, Kafka UI about 460 MB, Schema Registry about 290 MB, Postgres about 25 MB.

**Commit**

```bash
git add docker-compose.yml docs/phase-0-steps.md
git commit -m "add kafka, kafka ui, schema registry and postgres to docker compose"
```

**Known issue in Kafka UI:** Brokers → broker 1 → **Metrics** tab turns the page blank. Kafka UI's server returns an empty reply for per-broker metrics because we have not set up metrics collection from the broker, and the page does not handle an empty reply. Reload the page to recover. The broker and the other pages are unaffected.

## Step 8c — Makefile

**Concept:** a Makefile gives short names to commands. `make up` is the one command that starts the whole platform.

**What to do**

Create `Makefile` in the project root. The indented lines must start with a tab character, not spaces.

```make
.PHONY: up down ps

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps
```

| Line | Meaning |
|---|---|
| `up:` | The name typed after `make`. |
| The indented line under it | The command that runs. |
| `.PHONY: up down ps` | Tells `make` these are command names, not files to build. |

```bash
make -n down     # dry run: prints the command without running it
make down
make up
make ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Two error messages and what they mean:

| Message | Cause |
|---|---|
| `No rule to make target 'down'` | The Makefile on disk has no such rule. Here the file was empty because it had not been saved. |
| `missing separator` | An indented line starts with spaces instead of a tab. |

**Check**

- `make down` removes four containers; `make up` starts four; `make ps` lists four as "Up".
- The topic list still shows `hello` and `_schemas`.

**Observed:** all checks passed. This is also the roadmap's check for Phase 0: `make up`, then Kafka UI at http://localhost:8080 shows the broker.

**Commit**

```bash
git add Makefile docs/phase-0-steps.md
git commit -m "add makefile with up, down and ps"
```

## Step 9a — `ruff`, `pytest` and a first test

**Concept:** two automatic checks. **`ruff`** is a linter: it reads the code without running it and reports mistakes such as unused imports and undefined names. **`pytest`** runs tests: small functions that call the code and state what the result must be.

**What to do**

```bash
uv add --dev ruff pytest
```

Create `tests/test_smoke.py`:

```python
from streamml import main


def test_main_prints_greeting(capsys):
    main()
    assert capsys.readouterr().out == "Hello from streamml!\n"
```

```bash
uv run ruff check .
uv run pytest
```

| Part | Meaning |
|---|---|
| `uv add --dev` | Adds the tools as development dependencies: needed to work on the project, not to run it. |
| `test_` prefix on the file and function | How `pytest` finds tests. |
| `capsys` | A `pytest` helper that captures what the code prints. |
| `assert` | The statement that must be true for the test to pass. |

The test is a smoke test: it is trivial on purpose and proves the test setup works before real tests arrive in Phase 1.

**Check**

- `ruff check` prints "All checks passed!".
- `pytest` prints "1 passed".

**Observed:** both passed, with pytest 9.1.1 on Python 3.12.13.

**Commit**

```bash
git add pyproject.toml uv.lock tests docs/phase-0-steps.md
git commit -m "add ruff, pytest and a smoke test"
```
