# Phase 0 — learning notes

What Phase 0 taught, as five questions and answers. The commands behind each answer are in [phase-0-steps.md](phase-0-steps.md).

## 1. What does the broker do, and what does the controller do?

The broker is the Kafka server. It accepts messages from producers, writes them to disk, and serves them to consumers. Producers and consumers are separate programs; they talk to the broker and never to each other.

The controller manages the cluster instead of the messages. It keeps the record of which topics exist, which broker holds which data, and which brokers are alive. A broker cannot serve clients until the controller has admitted it ("unfenced" it).

In production these are separate servers. Here one container does both jobs (`KAFKA_PROCESS_ROLES: broker,controller`), in KRaft mode, with no ZooKeeper.

## 2. Why does our Kafka have two listeners?

A Kafka client connects in two stages. It first connects to a bootstrap address and asks where the brokers are. The broker answers with its advertised address, and the client uses that address from then on.

The advertised address has to be reachable from where the client runs, and our clients run in two places:

| Client runs | Address it uses |
|---|---|
| On the Mac (the Python programs) | `localhost:9092` |
| In another container (Kafka UI, Schema Registry) | `kafka:29092` |

One listener advertises one address, so the broker has two: `HOST` and `DOCKER`. With only one, Kafka UI reached the broker, was told to use `localhost:9092`, and failed, because inside its own container `localhost` is itself.

## 3. What happens when you read the same topic twice?

You get the same messages again. A topic is an append-only log on disk; reading does not remove anything.

This matters for the project in two ways. Several consumers (sink, features, enricher) can each read all of `events.raw` independently. And a consumer that crashes can restart and continue from where it stopped, or go back and read again.

## 4. What survives `make down`, and why?

The topics, the messages, and the Postgres data survive. The containers and the network do not.

A container's files are deleted with the container. Kafka's data folder and Postgres's data folder are attached to Docker volumes (`kafka-data`, `postgres-data`), which live outside the containers. `make down` deletes containers and keeps volumes. `docker compose down -v` deletes the volumes too, which is the deliberate way to wipe everything.

Before the volume was added, the `hello` topic was lost twice: every change to Kafka's settings made Compose recreate the container.

## 5. What went wrong because of an unsaved file?

Twice a command read the file on disk while the edit was still only in the editor.

| What happened | How it showed |
|---|---|
| `docker compose up -d` after adding Kafka UI | The output said `up 1/1` when two services were expected, and http://localhost:8080 did not connect. |
| `make down` after creating the Makefile | `No rule to make target 'down'`; the file on disk was 0 bytes. |

The habit: save before running, and when a tool behaves as if an edit is not there, check the file on disk. `docker compose config --quiet` and `make -n <target>` both check a file without changing anything.

## Other things worth remembering

- Kafka does not start when Docker Desktop starts. Run `make up` from the project folder.
- In YAML, indentation is the structure. `volumes:` indented two spaces was read as a service named "volumes".
- A file can be valid and still wrong: the volume was declared but not attached, and Compose accepted it.
- Kafka's "log" means stored messages. `KAFKA_LOG_DIRS` is the data folder.
- The broker's Metrics tab in Kafka UI goes blank because no metrics collection is set up. It is safe to ignore.
