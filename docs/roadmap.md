# Roadmap

## Goal

A real-time ML platform that runs on one laptop. E-commerce events are replayed into Kafka, processed by stream consumers, and used to train and serve models. The first use case is **demand forecasting** (units sold per product over the next 24 hours). The stream and feature pipeline are shared, so further use cases such as anomaly detection can be added later.

## Architecture

```
HOST (needs Apple GPU)                    DOCKER COMPOSE
----------------------                    --------------
Ollama qwen2.5:14b  <--- host.docker.internal:11434 ---+
mlx-lm (LoRA train + serve)                            |
                                                       |
real dataset --> replayer --+                          |
fault injector -------------+--> Kafka (KRaft) + Schema Registry + Kafka UI
                                  |
        +-------------------------+--------------------------+
   sink consumer          feature consumer             enricher consumer
   (Parquet, by hour)     (hourly windows -> Postgres) (calls Qwen)
        |                         |
        +--> trainer (LightGBM) --+--> MLflow (tracking + registry)
                                         |
                    FastAPI /predict + scorer consumer --> Postgres
                                         |
                                     Streamlit
```

Ollama and the MLX fine-tune run on the host, not in Docker, because Docker on macOS cannot use the Apple GPU. Containers reach Ollama at `host.docker.internal:11434`.

Kafka needs no installation: it runs as a container from the official `apache/kafka` image. Redpanda (Kafka-compatible) is the fallback if the broker proves too heavy; that is a Compose-file change with no code changes.

## Design choices

| Area | Choice | Why |
|---|---|---|
| Broker | `apache/kafka` in KRaft mode, 1 broker | Real Kafka, no ZooKeeper, light enough for a laptop |
| Schemas | Confluent Schema Registry + Avro | Schema evolution is handled explicitly |
| Client | `confluent-kafka` (Python) | Supports idempotent producer and Schema Registry |
| Stream processing | Plain Python consumers, hand-written tumbling windows | Makes offsets, commits, and event time explicit. Flink is a stretch goal |
| Raw storage | Parquet partitioned by date/hour, queried with DuckDB | Handles tens of millions of rows with no server |
| Serving storage | Postgres | Features, predictions, enrichment results |
| Model | LightGBM vs a seasonal-naive baseline | Standard for tabular forecasting; the baseline keeps the result honest |
| MLOps | MLflow tracking + registry, `champion` alias | Serving loads whatever is tagged champion |
| Python | 3.12 pinned via `uv` | Widest wheel support for LightGBM, MLX and confluent-kafka |

## Topics

| Topic | Key | Notes |
|---|---|---|
| `catalog.products` | product_id | Log-compacted |
| `events.raw` | product_id | 6 partitions; view, cart, remove_from_cart, purchase |
| `events.dlq` | — | Events that fail validation, with the error reason |
| `catalog.enriched` | product_id | Qwen output |
| `predictions` | product_id | Scorer output |

## Data

**Real stream (main source).** The REES46 "eCommerce behavior data from multi category store" dataset on Kaggle. Expected columns: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`. Phase 2 starts by confirming the actual columns, sizes, and license from the downloaded files.

- Start with one monthly file; add a second month if more training history is needed.
- The replayer reads the CSV in timestamp order and produces to `events.raw`. Backfill mode runs at full speed; live mode shifts timestamps to "now" and plays at real or accelerated speed.
- Raw data is git-ignored and never committed. `make download` fetches it with the Kaggle CLI.
- The catalog (`catalog.products`) is derived from the distinct products in the dataset.

**Forecast scope.** Purchase data is sparse for most products. The model forecasts daily units for the top products by purchase count; the rest are reported at category level. The cut-off is set after profiling the data.

**Synthetic injector.** Runs alongside the replay and adds:
- malformed, duplicate, and late events at a low configurable rate, to exercise the DLQ, dedupe, and late-event handling;
- new products on `catalog.products`, to trigger the Qwen enricher.

**Qwen-generated text.** Qwen writes a title and short description for each forecast-scope product from its category, brand and price. These texts are synthetic and labelled as such.

**Tests and CI** use small generated fixtures, not the Kaggle data.

## Repo layout (target)

```
streaming-ml-platform/
├── docker-compose.yml        # kafka, schema-registry, kafka-ui, postgres, mlflow, services
├── Makefile                  # up, down, download, backfill, live, bench, train, retrain, test
├── pyproject.toml            # uv project
├── schemas/                  # Avro .avsc files
├── data/                     # git-ignored: raw CSVs, Parquet
├── src/streamml/
│   ├── ingest/               # download.py, profile.py, replayer.py, injector.py
│   ├── catalog/              # build.py (from dataset), describe.py (Qwen text)
│   ├── consumers/            # sink.py, features.py, enricher.py, scorer.py
│   ├── ml/                   # dataset.py, train.py, evaluate.py, drift.py
│   ├── api/                  # FastAPI: /predict, /explain, /health
│   └── llm/                  # ollama client, lora/ (prepare, train, eval)
├── app/                      # Streamlit pages
├── tests/
├── docs/
└── .github/workflows/ci.yml  # ruff + pytest
```

## Phases

Each phase is one PR and has a check that must pass before the next starts.

**Phase 0 — Scaffold.** Repo, `uv` project, Compose with Kafka + Schema Registry + Kafka UI + Postgres, Makefile, CI.
*Check:* `make up`; Kafka UI at localhost:8080 shows the broker.

**Phase 1 — Kafka fundamentals.** Topic creation script, Avro schemas, a minimal producer and consumer. Covers partitions, keys, consumer groups, offsets, rebalancing.
*Check:* two consumers in one group split the partitions between them; killing one triggers a rebalance.

**Phase 2 — Data ingestion.**
- `make download`, then a profiling script reporting columns, row counts, date range, nulls, and purchases per product.
- Catalog build from distinct products.
- Replayer with backfill and live modes; idempotent producer (`enable.idempotence`, `acks=all`).
- Fault and new-product injector.

*Check:* `make backfill` loads one full month; produced count matches the CSV row count. `make bench` records measured producer and consumer events/sec (1 vs 6 consumer instances).

**Phase 3 — Stream processing.**
- Sink consumer: validated events to Parquet, manual offset commit only after the file is written, bad events to the DLQ.
- Feature consumer: hourly tumbling windows on event time, with a watermark for late events and an idempotent upsert into Postgres.
- Lag metrics exposed for the UI.

*Check:* DuckDB row count over Parquet equals produced count minus DLQ count; killing and restarting a consumer mid-run produces no duplicates or gaps.

**Phase 4 — Training.** Features (lags, rolling means, views, cart adds, price, hour, weekday, category, brand), time-based split, LightGBM vs seasonal-naive, logged to MLflow, best run tagged `champion`.
*Check:* held-out WAPE for both models is reported in MLflow. If LightGBM does not beat the baseline, that is investigated and reported.

**Phase 5 — Serving and monitoring.** FastAPI `/predict` loads the champion. A scorer consumer writes forecasts to `predictions` and Postgres. A drift job compares forecasts with actuals and computes feature PSI. `make retrain` trains a challenger and promotes it only if it beats the champion.
*Check:* `curl /predict` returns a forecast; the forecast-vs-actual table fills as live mode runs.

**Phase 6 — Qwen in the stream.** Qwen writes titles and descriptions for forecast-scope products. The enricher consumer sends newly injected products to Qwen and writes to `catalog.enriched`. `/explain` gives Qwen the top feature contributions (SHAP) for a forecast and returns a plain-English explanation.
*Check:* inject a product; an enriched record appears. The explanation cites the actual top features.

**Phase 7 — Streamlit.** Four pages: Pipeline (throughput, consumer lag, DLQ), Forecasts (per-product forecast vs actual, Explain button), Model (metrics, drift, champion history), Enrichment.
*Check:* all pages load against a running stack.

**Phase 8 — LoRA fine-tune.**
- Task: classify a messy merchant-style product title into its category.
- Labels: the dataset's real `category_code`. Inputs: Qwen-written titles, noised into abbreviated forms (synthetic inputs, real labels).
- Fine-tune `Qwen2.5-0.5B-Instruct` (or 1.5B) with `mlx-lm` on the host.
- Compare the base small model, the LoRA model, and the 14B model on held-out accuracy and products/sec.
- Swap the tuned model into the enricher via `mlx_lm.server`.

*Check:* evaluation table reports accuracy and throughput for all three models.

**Phase 9 — Polish.** README with architecture diagram, quick start, results tables, and data attribution; integration test using Testcontainers Kafka; green CI badge.

## Later use cases

- Real-time anomaly detection on the same `events.raw` stream (price glitches, order spikes).

## End-to-end verification

```bash
make up          # all containers healthy
make download    # one monthly file from Kaggle
make backfill    # full month into Kafka, Parquet and Postgres
make train       # champion registered in MLflow
make live        # live replay; scorer writes forecasts
open http://localhost:8501   # Streamlit: lag near zero, forecasts tracking actuals
make test        # pytest green, same as CI
```
