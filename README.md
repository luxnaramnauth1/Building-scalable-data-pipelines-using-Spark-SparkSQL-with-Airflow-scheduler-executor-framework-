# Sales data pipeline: Spark SQL + Airflow

A complete, runnable batch pipeline. It generates raw files, reads them with Spark,
transforms them with SparkSQL files (medallion layers), checks data quality and exports reports.
Airflow schedules it; the same jobs also run from a plain Python runner, `spark-submit` or cron.

```
generate_data -> ingest -> transform -> quality_check -> export_report
 (raw CSV)       (bronze)   (silver+gold)   (gate)          (CSV reports)
```

## Quick start

```bash
make install                 # creates .venv, installs pyspark + pytest (needs Java 17+)
make run DS=2026-10-01       # one day
make backfill START=2026-10-01 END=2026-10-07
make test                    # 16 end-to-end tests (~80s)
make read F=data/gold/gold_daily_sales
```

## Layout

| Path | Purpose |
|---|---|
| `dags/sales_pipeline_dag.py` | Airflow DAG (BashOperator per job, retries, `{{ ds }}`) |
| `run_pipeline.py` | Local runner: same tasks, single date or backfill range |
| `jobs/` | One file per task: `generate_data`, `ingest`, `transform`, `quality_check`, `export_report`, `read_file` |
| `sql/` | One SparkSQL statement per file, numbered in execution order |
| `pipeline/` | Config/paths, Spark session factory, SQL templating, CLI helper |
| `tests/` | pytest end-to-end tests on a temporary data lake |

## Data layout (`data/`, override with `PIPELINE_DATA_DIR`)

```
raw/<entity>/ds=YYYY-MM-DD/<entity>.csv       landing files
bronze/<entity>/ds=.../*.parquet              all-string raw data + lineage columns
silver/<table>/ds=.../*.parquet               typed, deduplicated, validated
gold/<table>/ds=.../*.parquet                 business aggregates
quality/ds=.../results.json                   check results
reports/ds=.../*.csv, summary.json            exported reports
```

## What the SQL does

| File | Output |
|---|---|
| `10`, `11` | typed `silver_customers`, `silver_products` |
| `12` | staging view: parse each order, assign one `reject_reason` (or none), dedupe by `order_id` |
| `13` | `silver_orders_clean`: valid orders joined with product and customer data, with revenue |
| `14` | `silver_orders_rejected`: quarantine table with the reason |
| `20` | `gold_daily_sales` by date, category and country |
| `21` | `gold_customer_ltv`: cumulative over all days up to `ds`, with tiers and rank |
| `22` | `gold_top_products`: top 3 per category |

## Reading files

```bash
python jobs/read_file.py data/raw/orders/ds=2026-10-01/orders.csv --limit 5
python jobs/read_file.py data/silver/silver_orders_clean --schema
python jobs/read_file.py data/gold/gold_customer_ltv --sql "SELECT ltv_tier, count(*) FROM t GROUP BY 1"
```

Format is auto-detected (csv, parquet, json, text). Partition folders such as `ds=` become columns.

## Design notes

* **Idempotent.** Writes use dynamic partition overwrite, so retries and re-runs replace only that `ds`.
* **Backfill-safe.** `max_active_runs=1` in the DAG, and the LTV step reads every partition up to `ds`.
* **No data silently lost.** Bronze keeps every row. A quality check enforces `bronze = clean + rejected`.
* **Quality gate.** `quality_check` exits non-zero on failure, so Airflow stops before `export_report`.
  Tune with `MAX_REJECT_RATE` (default `0.10`).
* **Safe SQL templating.** `ds` is validated as `YYYY-MM-DD` before it is substituted into SQL or paths.

## Running with Airflow

Install Airflow where the DAG is parsed and point its dags folder at `dags/`, or use the included
Docker files (`docker compose up --build`, UI on port 8080). Worker environment variables:

| Variable | Meaning |
|---|---|
| `PIPELINE_PROJECT_DIR` | project root (default: folder above `dags/`) |
| `PIPELINE_DATA_DIR` | data lake root |
| `USE_SPARK_SUBMIT=1` | run jobs through `spark-submit` |
| `SPARK_MASTER` | `local[2]` (default), `spark://host:7077`, `yarn`, ... |
| `SPARK_SUBMIT_ARGS` | extra flags, e.g. `--executor-memory 4g` |

For a real cluster, put the data lake on shared storage (S3/HDFS/ADLS): change `Paths` in
`pipeline/config.py` and add the matching Hadoop connector jars.

## Without Airflow

```bash
python run_pipeline.py --ds 2026-10-01
# cron, daily 02:00, yesterday's partition:
0 2 * * * cd /path/to/project && .venv/bin/python run_pipeline.py --ds $(date -d yesterday +\%F)
```

The task functions are plain `run(ds, paths)` callables, so wrapping them for Prefect or Dagster is straightforward.

## Verification status

Tested here: the full pipeline run, the 16 pytest tests, and the file reader (PySpark 4.0.4, Java 21).
**Not tested here:** the DAG inside a running Airflow (the file compiles, but Airflow was not installed),
and the Docker files (neither was built or started).
