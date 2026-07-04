# Superset Setup Guide

BI layer for the supply chain stack. Superset connects directly to Druid SQL
via the broker — no export layer needed; charts reflect streaming data within
seconds. Runs native arm64 (no emulation).

## Prerequisites

- The stack is up (`docker compose up -d`) and the `superset` service is
  defined in `docker-compose.yaml` (custom image layering `pydruid` +
  `psycopg2-binary` onto `apache/superset:4.1.1`).

## One-Time Setup

Run each of these exactly once (state persists in the `postgres-data` and
`superset-data` volumes — plain `docker compose down` or a reboot does NOT
require redoing any of this; only `down -v` does):

```bash
# 1. Create Superset's metadata database on the existing Postgres
docker exec druid-metadata psql -U druid -c "CREATE DATABASE superset;"

# 2. Build and start Superset (first build takes ~a minute)
docker compose up -d superset

# 3. Initialize Superset's schema, admin user, and roles
docker exec superset superset db upgrade
docker exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@localhost --password admin
docker exec superset superset init
```

Ignore the Flask-Limiter and "No PIL installation found" warnings — both are
cosmetic for local use.

## Connect Superset to Druid

1. Open **http://localhost:8088** (admin / admin).
2. **Settings → Database Connections → + Database**.
3. If "Apache Druid" is not listed, type "druid" in the Supported Databases
   dropdown, or choose **Other** — it accepts any SQLAlchemy URI and is
   functionally identical.
4. SQLAlchemy URI (trailing slash required; broker hostname because Superset
   is inside the Docker network):

   ```
   druid://druid-broker:8082/druid/v2/sql/
   ```

5. **Test Connection** → save.

## Create a Dataset

Fastest route is SQL Lab:

1. **SQL Lab**, select the Druid database, run:

   ```sql
   SELECT * FROM iot_telemetry LIMIT 100
   ```

2. Click **Create Chart** under the results, and name the dataset
   (e.g. `iot_telemetry_raw`).
3. **Immediately edit the dataset** (Datasets → hover → pencil) and remove
   the `LIMIT 100` from its SQL. A LIMIT left in a virtual dataset silently
   caps every chart built on it.

## Build the First Chart (events per minute)

In the chart builder:

| Setting | Value |
|---|---|
| Visualization type | Line Chart |
| X-AXIS | `__time` |
| Time grain | Minute |
| Metric | Simple: column `__time`, aggregate COUNT (equivalent to COUNT(*); the `*` shortcut may be unavailable on virtual datasets — Custom SQL tab with `COUNT(*)` also works) |
| Dimensions | empty first; then add a categorical column (warehouse, sensor type) to split the line |
| Time range | **No filter** initially |

Hit **Update Chart**, then **Save → Add to new dashboard**.

## Gotchas

- **Time range filters vs. simulated time.** The generator's speed multiplier
  (e.g. 60x) makes event timestamps drift ahead of wall clock. If a chart is
  blank, clear the time range filter first; find where data actually lives
  with `SELECT MAX(__time) FROM <datasource>`.
- **Event time, not ingestion time.** Charts plot `__time` from inside each
  event. Druid outages leave no gaps (Kafka backfills); gaps and rate changes
  in charts reflect the *generator's* behavior only.
- **Reserved words in Druid SQL.** Aliases like `minute`, `hour`, `day`,
  `count`, `value`, `timestamp` break the parser ("Incorrect syntax near").
  Rename or double-quote them (Druid uses double quotes, never backticks).
- **Blank line chart with one data point.** A line chart cannot render a
  single point — usually means the query collapsed to one row (leftover
  LIMIT, or a pure aggregate with no GROUP BY).
- **Druid missing from the engine list.** The list is cached at process
  start: `docker compose restart superset`, hard-refresh the browser, or use
  the **Other** engine option.

## Secret Key Note

`SUPERSET_SECRET_KEY` is currently hardcoded in `docker-compose.yaml`. Before
publishing the repo, move it to a git-ignored `.env`:

```yaml
# docker-compose.yaml
SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY}
```

```bash
# .env  (git-ignored)
SUPERSET_SECRET_KEY=<long random string>
```
