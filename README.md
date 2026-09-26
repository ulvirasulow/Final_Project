# batch-pipeline

An end-to-end **batch ETL pipeline for fake banking data**, built with
PostgreSQL, Apache Spark, MinIO, and Apache Airflow, using a Medallion
(Bronze/Silver/Gold) architecture. Runs entirely locally in Docker
Compose and can be started from an empty database with a single
Airflow DAG trigger.

## What it does

1. **Generates** realistic, internally-consistent fake banking data
   (branches, customers, accounts, transactions, loans, etc.) with
   5 deliberately injected data-quality defects.
2. **Loads** it into PostgreSQL as the OLTP source of truth
   (`banking_source`).
3. **Ingests** it into MinIO (S3-compatible) as raw **Bronze** Parquet
   via Spark — no transformation, just a faithful copy.
4. **Cleans and validates** it into **Silver** Parquet, quarantining
   any row that fails a data-quality rule instead of dropping it.
5. **Builds a Kimball-style Gold star schema** (4 dimensions + 1 fact
   table), including a full **SCD Type 2** customer dimension.
6. **Orchestrates the whole flow** — empty database to validated Gold
   layer — with a single Airflow DAG.

Every layer is idempotent: re-running any part (or the whole DAG)
never creates duplicate or accumulating data.

## Architecture

```
Fake Data Generator (Python)
        ↓
   PostgreSQL (banking_source, 9 OLTP tables)
        ↓
  Spark Bronze (raw copy)
        ↓
  MinIO Bronze
        ↓
  Spark Silver (cleaning, dedup, quarantine)
        ↓
MinIO Silver + Quarantine
        ↓
   Spark Gold (dimensions + SCD Type 2 + fact)
        ↓
Gold Star Schema
        ↓
Airflow Orchestration (banking_batch_pipeline DAG, single trigger)
```

## Tech stack

| Technology | Role |
|---|---|
| Python (pandas, Faker, psycopg2) | Fake data generation, PostgreSQL loading |
| PostgreSQL 16 | OLTP source (`banking_source`) + Airflow metadata |
| Apache Spark | All Bronze/Silver/Gold transformations |
| MinIO | S3-compatible data lake |
| Parquet | Storage format for all lake layers |
| Apache Airflow 2.9 | End-to-end orchestration (`LocalExecutor`) |
| Docker Compose | Runs the whole local environment |

## Data model

9 datasets, generated together and loaded as matching PostgreSQL
tables: `branches`, `dim_date`, `customers`, `customer_history`,
`accounts`, `account_customer_bridge`, `transactions`,
`account_balance_snapshot`, `loan_lifecycle`. Defaults: 10 branches,
300 customers, 700 accounts, 5,000 transactions, 150 loans, over
2023–2025.

`transactions` carries the 5 intentional defects (duplicate ID, NULL
amount, orphan account, negative amount, invalid currency) — detected
and quarantined in Silver.

## Gold layer (star schema)

| Object | Grain |
|---|---|
| `dim_customer` | one row per customer per historical validity period (**SCD Type 2**) |
| `dim_account` | one row per account |
| `dim_branch` | one row per branch |
| `dim_date` | one row per calendar date |
| `fact_transaction` | one row per valid transaction |

`fact_transaction` always resolves the customer version valid **on
the transaction's date**, not just the current one.

## Airflow DAG

```
generate_fake_data >> load_to_postgres >> check_source_ready
  >> run_bronze >> run_silver
  >> run_gold_dimensions >> run_gold_fact_transaction
  >> validate_gold
```

Manually triggered, `catchup=False`. A single trigger on an empty
database bootstraps everything end to end.

## How to run

```bash
cp .env.example .env
# on Linux, also set AIRFLOW_UID and DOCKER_GID (see .env.example)

docker compose up -d --build
docker compose ps
```

Then trigger `banking_batch_pipeline` in the Airflow UI at
`http://localhost:8080` — one manual trigger runs the whole pipeline
from an empty database.

## Resetting

Every layer overwrites/truncates before writing, so re-triggering the
DAG is a full reset — no manual cleanup needed. To reset volumes too:

```bash
docker compose down -v
docker compose up -d --build
```

## Project status

**Implementation:** complete — generation, loading, Bronze, Silver +
quarantine, Gold (with SCD Type 2), the full Airflow DAG, and the
verification harness all exist and pass static/syntax checks.
