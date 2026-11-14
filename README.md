# freshcart-pipelines
# freshcart-pipelines

[![CI](https://github.com/<your-username>/freshcart-pipelines/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-username>/freshcart-pipelines/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Snowflake](https://img.shields.io/badge/warehouse-Snowflake-29B5E8)
![Mage](https://img.shields.io/badge/orchestration-Mage-6B4FBB)
![RabbitMQ](https://img.shields.io/badge/messaging-RabbitMQ-FF6600)

An end-to-end data engineering project with four parts:

| Part | What it is | Stack |
|---|---|---|
| **ELT lakehouse** | Order data from web and mobile APIs, CRM, FX and pincode sources goes into a bronze → silver → gold model on Snowflake | Python, Snowflake SQL |
| **ML pipeline** | An Iris classifier as a Mage DAG, where a failing test stops bad results from being saved | Mage, scikit-learn |
| **Event streaming** | Routing on a topic exchange, ACK/NACK handling, and a streaming consumer that feeds the lakehouse | RabbitMQ, pika |
| **CI/CD** | Every push runs the test suite on GitHub Actions | GitHub Actions, pytest |

---

## Contents

- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [1. FreshCart ELT on Snowflake](#1-freshcart-elt-on-snowflake)
- [2. ML pipeline with Mage](#2-ml-pipeline-with-mage)
- [3. Event streaming with RabbitMQ](#3-event-streaming-with-rabbitmq)
- [4. CI/CD](#4-cicd)
- [Getting started](#getting-started)
- [Design decisions](#design-decisions)
- [Known limitations and next steps](#known-limitations-and-next-steps)

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        WEB[Web orders API]
        MOB[Mobile orders API]
        CRM[CRM tier events]
        FX[Frankfurter FX API]
        PIN[India Post pincode API]
        MQ[(RabbitMQ<br/>freshcart-orders)]
    end

    subgraph Snowflake["Snowflake lakehouse"]
        BR[Bronze<br/>raw VARIANT]
        SV[Silver<br/>typed · validated · deduplicated]
        Q[Quarantine<br/>rejected rows + reasons]
        DIM[Dim / Ref<br/>SCD2 customers · daily FX · pincode geo]
        GD[Gold<br/>fct_orders]
    end

    WEB & MOB & CRM & FX & PIN -->|extract → landing files| BR
    MQ -->|order_consumer.py<br/>1-minute micro-batches| BR
    BR --> SV
    SV -->|fails rules| Q
    SV --> GD
    DIM --> GD
    GD --> QC{Quality checks}
```

The Mage ML pipeline and the CI workflow run next to this flow: CI tests the pipeline code on every push, and the Mage DAG uses test gates to stop a poor model from being published.

---

## Repository layout

```
freshcart-pipelines/
├── README.md
├── .gitignore                    # .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt
├── elt/
│   └── freshcart_elt_colab.ipynb # FreshCart ELT pipeline, with all outputs
├── iris_project/                 # Mage project
│   ├── data_loaders/load_iris.py
│   ├── transformers/
│   │   ├── prepare_data.py
│   │   ├── train_model.py        # decision tree (with an accuracy test)
│   │   ├── train_rf.py           # random forest (parallel branch)
│   │   ├── train_knn.py          # k-nearest neighbours (parallel branch)
│   │   └── evaluate_model.py     # confusion matrices and a metrics table
│   ├── data_exporters/save_results.py
│   └── pipelines/iris_classifier/metadata.yaml
├── stream/
│   ├── dispatcher.py             # publishes ride/food requests to a topic exchange
│   ├── driver.py                 # binds a queue to a pattern, ACKs or NACKs each message
│   ├── order_producer.py         # publishes FreshCart orders
│   └── order_consumer.py         # RabbitMQ → bronze → gold, in 1-minute batches
├── tests/
│   └── test_prepare_data.py
└── .github/workflows/ci.yml
```

---

## 1. FreshCart ELT on Snowflake

FreshCart is a grocery retailer that takes orders through two channels. Each channel has its own API, with different field names, date formats and amount formats. The pipeline combines both channels into one trusted fact table for revenue and loyalty reporting.

### Pipeline stages

| Stage | What happens | Code |
|---|---|---|
| **Extract** | Pulls each API incrementally (`updated_since`, with paging) and writes one gzipped NDJSON landing file per source per run. Each record is wrapped with `source`, `run_id` and `extracted_at`. | `extract.py` |
| **Load** | `PUT` the file to an internal stage, then `COPY INTO` a bronze table with a `VARIANT` column. The file name and row number are kept for lineage. | `snowflake_io.load_file()` |
| **Silver: clean** | Maps both API shapes onto one schema and parses types: emails trimmed and lower-cased, `"2,100.00"` → `2100.00`, dates in several formats. Each row gets a list of validation reasons. | `10_silver_orders_clean.sql` |
| **Quarantine** | Rows that fail a rule are kept, with their reasons, in `quarantine.orders` instead of being dropped. | `10_silver_orders_clean.sql` |
| **Silver: dedup** | Keeps one row per `order_id`, choosing the latest `updated_at` and breaking ties with `ingest_seq`. | `20_silver_orders_dedup.sql` |
| **Dimensions** | Builds the SCD Type 2 customer tier history, a daily FX calendar with weekends filled in, and pincode → city, state and region. | `30_…`, `31_…`, `32_….sql` |
| **Gold** | A point-in-time join to the tier valid on the order date, conversion to INR and loyalty points, then a `MERGE` into `gold.fct_orders` that compares a row hash. | `40_gold_fct_orders.sql` |
| **Checks** | Assertions run after each load. A check marked "fail" stops the pipeline; a "warn" check only logs, unless `--strict` is passed. | `run_pipeline.quality_checks()` |

### Validation rules (silver)

`bad_email` · `bad_pincode` · `unparseable_date` · `unparseable_updated_at` · `non_positive_amount` · `unsupported_currency`

A missing amount does not reject the row. It sets the `amount_missing` flag, so a later update to the same order can fill it in.

### Quality checks (after gold)

| Check | Severity |
|---|---|
| Enrichment joins add no extra rows (rows out = rows in) | fail |
| One gold row per `order_id` | fail |
| Every order matches a customer | warn |
| Every order with an amount has an FX rate | warn |
| Every order resolves to a city | warn |
| Quarantine rate ≤ 5% (configurable) | warn |
| No gold row has a valid amount but a missing `amount_inr` | warn |

### Business logic

```
amount_inr     = ROUND(amount × rate_to_inr(order_date), 2)
loyalty_points = FLOOR(amount_inr / 100) × tier_multiplier(tier on order_date)
is_high_value  = amount_inr ≥ 2000
```

Tier multipliers: Silver ×1, Gold ×2, Platinum ×3. The tier comes from the SCD2 row that was valid **on the order date**, so a customer's upgrade never re-scores their past orders.

### Result on the sample data

```
GOLD: 4 orders, revenue ₹7785.00, 161 loyalty points
```

A second run with the same data leaves gold unchanged. Bronze receives new copies of the rows, but deduplication and the `MERGE` absorb them.

---

## 2. ML pipeline with Mage

```mermaid
flowchart LR
    L[load_iris<br/><i>data_loader</i>] --> P[prepare_data<br/><i>transformer</i>]
    P --> DT[train_model<br/>decision tree]
    P --> RF[train_rf<br/>random forest]
    P --> KNN[train_knn<br/>KNN]
    DT & RF & KNN --> S[save_results<br/><i>data_exporter</i>]
    DT & RF & KNN --> E[evaluate_model<br/>metrics + confusion matrices]
```

Every block has a `@test` function. The key one is on `train_model`:

```python
@test
def test_accuracy(output, *args):
    accuracy = (output['predicted'] == output['target']).mean()
    assert accuracy >= 0.8, 'Model accuracy is below 80%'
```

If the test fails, Mage marks the block as failed and does **not** run the blocks after it. A poor model therefore never produces `iris_predictions.csv`, so nothing downstream can use bad predictions.

### Test gate in practice

| `max_depth` | Test accuracy | Gate passed | `iris_predictions.csv` written |
|---|---|---|---|
| 1 | below 0.8 | ✗ | ✗ |
| 2 | 0.967 | ✓ | ✓ |
| 3 | 1.000 | ✓ | ✓ |
| 4 | 1.000 | ✓ | ✓ |
| 5 | 1.000 | ✓ | ✓ |

```bash
mage run iris_project iris_classifier --test
mage run iris_project iris_classifier --test --runtime-vars '{"max_depth": 1}'   # gate fails
```

The `evaluate_model` block prints a confusion matrix and accuracy, precision, recall and F1 for all three models side by side.

---

## 3. Event streaming with RabbitMQ

### Dispatch routing (`dispatcher.py`, `driver.py`)

`dispatcher.py` publishes ride and food requests to the `rapido-dispatch` topic exchange, using routing keys of the form `city.zone.type`, for example `bangalore.koramangala.food`. Each driver binds a private queue with its own pattern:

| Pattern | Receives |
|---|---|
| `bangalore.koramangala.*` | everything in one zone |
| `bangalore.*.ride` | rides anywhere in Bangalore |
| `*.*.food` | food orders in every city |
| `bangalore.#` | everything in Bangalore (control queue) |

`*` matches exactly one word and `#` matches zero or more. That is why a two-word key such as `bangalore.koramangala` reaches only `bangalore.#`.

```bash
python stream/dispatcher.py
python stream/driver.py Ravi "bangalore.koramangala.*"
```

**Reliability:** drivers use manual acknowledgements. A message missing its `zone` field is rejected with `basic_nack(requeue=False)`. Sending it back to the queue would make a permanently bad message loop forever.

### Streaming into the lakehouse (`order_producer.py`, `order_consumer.py`)

```
order_producer.py ──► exchange 'freshcart-orders' ──order.#──► durable queue 'freshcart.lakehouse'
                                                                         │
order_consumer.py:  collect 60 s ─► write_landing("stream") ─► load_file() ─► bronze.orders_raw
                    ─► SQL 10 → 20 → 30 → 31 → 32 → 40 ─► gold.fct_orders ─► ACK the batch
```

- **Reuses the batch pipeline's code.** The consumer imports `write_landing`, `load_file`, the SQL files and `quality_checks`, so streamed and batch orders go through the same logic.
- **At-least-once delivery.** Messages are acknowledged only after gold has been updated. If any step fails, the whole batch is NACKed with `requeue=True`, so no order is lost.
- **Idempotent.** Sending the same batch twice doubles the rows in bronze but not in gold, because dedup and the `MERGE` absorb the duplicates. Reloading the same landing file does nothing, because Snowflake's `COPY` remembers which files it has already loaded.

```bash
python stream/order_producer.py --count 20 --seed 42 --date 2026-09-30
python stream/order_consumer.py --once --window 60
```

---

## 4. CI/CD

`.github/workflows/ci.yml` runs on every push:

1. Check out the code
2. Set up Python 3.12
3. `pip install -r requirements.txt`
4. `pytest`

`tests/test_prepare_data.py` imports the Mage `prepare_data` block directly. Each block imports Mage's decorators only when they are missing, so it can run outside Mage. The test asserts that the train/test split is exactly 120/30. Changing `test_size` makes the build fail, which was checked on purpose with a red run followed by a fix commit.

---

## Getting started

### Prerequisites

- Python 3.12
- A Snowflake account (the free trial is enough)
- RabbitMQ: a local install, or a hosted broker such as the CloudAMQP free plan
- Google Colab, if you would rather not install anything locally

### Setup

```bash
git clone https://github.com/<your-username>/freshcart-pipelines.git
cd freshcart-pipelines
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file. It is git-ignored and must never be committed.

```dotenv
SNOWFLAKE_ACCOUNT=xxxxxxx-xxxxxxx
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
AMQP_URL=amqp://guest:guest@localhost:5672/%2F
# Only needed for real (non-mock) API runs:
# WEB_ORDERS_URL=...  WEB_ORDERS_TOKEN=...
# MOBILE_ORDERS_URL=...  MOBILE_ORDERS_TOKEN=...
# CRM_TIER_EVENTS_URL=...  CRM_TOKEN=...
```

In Colab, store these in **Colab Secrets** instead.

### Run

| Task | Command |
|---|---|
| ELT, first run (creates the Snowflake objects) | Open `elt/freshcart_elt_colab.ipynb` → **Run all**. Under the hood this runs `python run_pipeline.py --setup --mock --run-date 2026-09-30`. |
| ELT, later runs | `python run_pipeline.py --mock --run-date 2026-09-30` |
| ELT without Snowflake (extract only) | `python run_pipeline.py --mock --dry-run` |
| ML pipeline | `mage run iris_project iris_classifier --test` |
| Tests | `pytest` |
| Streaming into the lakehouse | See the [commands above](#streaming-into-the-lakehouse-order_producerpy-order_consumerpy) |

---

## Design decisions

| Decision | Why |
|---|---|
| **ELT rather than ETL** | Raw data is loaded unchanged and transformed inside Snowflake. Bronze is a full, replayable record of what each source sent, so a change to the business logic only needs a SQL re-run, not a new extract. |
| **Medallion layers** | Each layer has one job: bronze keeps the raw data, silver makes it correct, gold makes it useful. Problems can be traced to the layer that caused them. |
| **Idempotency at every layer** | `COPY` load history, `QUALIFY ROW_NUMBER()` dedup, and a gold `MERGE` that compares a row hash. Re-running a job is always safe, which matters for retries and backfills. |
| **Quarantine instead of drop** | Bad rows are kept with machine-readable reasons, so data quality problems are visible and measurable instead of silently disappearing. |
| **SCD Type 2 + point-in-time join** | Loyalty points use the tier the customer had on the order date. History stays correct after a tier change. |
| **Test gates inside the DAG** | Checks run before anything is published, not afterwards. A bad model or bad data never reaches downstream consumers. |
| **ACK after the commit** | The streaming consumer acknowledges a message only after its effect is in gold, which gives at-least-once delivery. The idempotent design keeps any duplicates harmless. |

---

## Known limitations and next steps

- **Mock sources.** The order and CRM APIs are mocked. The FX and pincode APIs are real public endpoints.
- **Orchestration.** The ELT runs from a notebook or CLI. The next step is a schedule in Mage, Airflow or Snowflake Tasks, with alerting on failed checks.
- **Timezones.** `ref.fx_daily` ends at Snowflake's `CURRENT_DATE`, which uses the account's timezone, US Pacific by default. Orders dated "today" in UTC can briefly have no FX rate. Setting the session to UTC would fix this.
- **Streaming scale.** Micro-batches rebuild the silver tables from all of bronze. At larger volume this should become incremental (streams and tasks, or dbt incremental models).
- **Tests.** CI covers the Mage transform. The next step is SQL tests for the silver rules and gold logic, run against a disposable Snowflake schema.

---

## Author

**<Your Name>** · [LinkedIn](https://www.linkedin.com/in/<your-handle>) · [GitHub](https://github.com/<your-username>)
