# Spark Test Environment for E-commerce KPI Demo

This project demonstrates a local end-to-end KPI pipeline using FastAPI, PostgreSQL, and PySpark.

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local script execution)

## Local Runbook

Follow these exact commands to boot the environment, seed data, run the batch aggregation, and verify the results.

### 1. Boot the Environment
Start the PostgreSQL, API, and Spark services.
```bash
docker compose up -d --build
```

### 2. Run Database Migrations
Apply the schema to the PostgreSQL database.
```bash
python3 -m alembic upgrade head
```

### 3. Seed Deterministic Raw Data
Insert sample e-commerce events into the raw tables.
```bash
python3 scripts/seed_data.py
```

### 4. Run Spark Batch Aggregation
Compute daily KPIs for the inclusive date range `2026-03-01` to `2026-03-03`.
```bash
python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-03
```

### 5. Verify SQL Summary Tables
Check the aggregated results directly in the database.
```bash
docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
docker compose exec postgres psql -U postgres -c "SELECT summary_date, view_users, cart_users, order_users, payment_users FROM daily_conversion_funnel ORDER BY summary_date ASC;"
```

### 6. Verify API Endpoints
Check the KPI results via the FastAPI read endpoints.
```bash
# Daily Traffic
curl -s "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01" | jq .

# Ranged Funnel
curl -s "http://localhost:8000/kpi/funnel/range?start_date=2026-03-01&end_date=2026-03-03" | jq .
```

### 7. Rerun Batch (Idempotency Check)
Re-running the batch for the same range should replace existing rows with identical values.
```bash
python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-03
```

### 8. Teardown
Stop all services and remove volumes.
```bash
docker compose down -v
```

## Automated Verification

Run the full automated verification suite:
```bash
pytest -q
```

`jq` is optional; remove `| jq .` in API examples if it is not installed.
