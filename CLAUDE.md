# CLAUDE.md — Streaming ML Platform

Project-specific instructions for Claude Code. These override default behavior.

---

## What this is

A real-time ML platform on a laptop: e-commerce events are replayed into Kafka, processed by Python stream consumers, and used to train and serve ML models. The first use case is demand forecasting; the pipeline is built so that others (for example anomaly detection) can be added on the same stream.

Roadmap, architecture, and design decisions: `docs/roadmap.md`. Read it before starting work.

**Status:** Phase 0 (scaffold) in progress. Update this line and the roadmap as phases complete.

---

## Behavioral Guidelines

- **Think before coding.** State assumptions. If something is unclear or has more than one reading, ask.
- **Simplicity first.** Minimum code that solves the problem. No speculative features or abstractions.
- **Surgical changes.** Touch only what the task needs. Match existing style.
- **Verify.** Every phase in the roadmap has a check; it must pass before the next phase starts.
- **Report results as measured.** Benchmarks and model metrics go in the README as observed, including when the model does not beat the baseline.

---

## Git Workflow

- Active development branch: `develop`
- PRs target `main` — never commit directly to `main`
- One PR per roadmap phase

---

## Stack

| Layer | Technology |
|---|---|
| Broker | Apache Kafka (KRaft, single broker) in Docker |
| Schemas | Confluent Schema Registry + Avro |
| Kafka client | `confluent-kafka` (Python) |
| Raw storage | Parquet, queried with DuckDB |
| Serving storage | Postgres |
| ML | LightGBM, MLflow (tracking + registry) |
| API | FastAPI |
| UI | Streamlit |
| LLM | Qwen via Ollama (`qwen2.5:14b`); LoRA fine-tune with `mlx-lm` |
| Python | 3.12, managed with `uv` |

---

## Key Conventions

- **Ollama and MLX run on the host, not in Docker.** Docker on macOS cannot use the Apple GPU. Containers reach Ollama at `host.docker.internal:11434`.
- **No dataset files in git.** `data/` is git-ignored. Tests and CI use small generated fixtures.
- **Consumers commit offsets manually**, only after the write they depend on has succeeded.
- **Events that fail validation go to `events.dlq`** with the error reason; they are never dropped silently.
- **Windows use event time**, not processing time.
- **Synthetic content is labelled as synthetic** in the README (Qwen-written product text, injected faults).
- Lint with `ruff`; test with `pytest`. Both must pass before pushing.
