# Real-Time Supply Chain Analytics Pipeline

A streaming analytics platform that simulates a multi-warehouse supply chain
and ingests its event streams into **Apache Druid** for sub-second OLAP
queries. A Python generator produces six independent event streams —
inventory, shipments, production, demand, supplier performance, and IoT
telemetry — into **Apache Kafka**, where Druid's Kafka indexing service
consumes them continuously and makes them queryable via SQL within seconds
of production.

Built entirely on open-source images (no Bitnami dependencies) and runs
locally on Docker Compose, including on Apple Silicon.

## Architecture

```
┌─────────────────────┐        ┌──────────────────┐        ┌─────────────────────────┐
│  Data Generator     │        │   Apache Kafka   │        │      Apache Druid       │
│  (Python, asyncio)  │        │   (KRaft mode)   │        │                         │
│                     │        │                  │        │  ┌─ coordinator/       │
│  6 event generators ├───────►│  6 topics        ├───────►│  │  overlord           │
│  stateful simulation│ JSON   │  supply_chain.*  │ Kafka  │  ├─ middleManager      │
│  configurable rates │        │  19 partitions   │ index  │  │  (peon tasks)       │
│  anomaly injection  │        │                  │ service│  ├─ historical         │
└─────────────────────┘        └──────────────────┘        │  ├─ broker ◄── SQL     │
                                                           │  └─ router ◄── console │
                               ┌──────────────────┐        │                         │
                               │  PostgreSQL      │◄───────┤  metadata               │
                               │  ZooKeeper       │◄───────┤  service discovery      │
                               └──────────────────┘        └─────────────────────────┘
```

**Data flow:** generator (host) → Kafka external listener `localhost:9092` →
topics → Druid supervisors consume via internal listener `kafka:29092` →
peon tasks build segments in memory (queryable immediately) → segments hand
off to deep storage (`/opt/shared`, a shared Docker volume) → historicals
serve them long-term.

## Event Streams

| Topic | Partitions | Content |
|---|---|---|
| `supply_chain.inventory_events` | 3 | Stock level changes, replenishments, adjustments across warehouses |
| `supply_chain.shipment_events` | 3 | Shipment lifecycle: created, in transit, delayed, delivered |
| `supply_chain.production_events` | 2 | Production runs, yields, line status |
| `supply_chain.demand_events` | 3 | Orders and demand signals by product/region |
| `supply_chain.supplier_events` | 2 | Supplier performance: lead times, fill rates, quality |
| `supply_chain.iot_telemetry` | 6 | High-frequency sensor readings (temperature, vibration, utilization) |

The generator maintains consistent cross-stream state (a shipment references
real inventory at a real warehouse) and supports configurable event rates,
time-compression (e.g. 60x speed), and anomaly injection for realistic
analytics scenarios.

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Stream transport | Apache Kafka 3.9 (KRaft) | No ZooKeeper dependency for Kafka; dual listeners for host + container clients |
| OLAP store | Apache Druid 29.0 | Kafka indexing service, streaming ingestion, SQL queries |
| Metadata | PostgreSQL 15 | Druid metadata store |
| Coordination | ZooKeeper 3.9 | Druid service discovery only |
| Data generation | Python 3.12, asyncio, kafka-python | Stateful simulation, pluggable output adapters (Kafka / file) |
| Orchestration | Docker Compose | Single-command stack, healthcheck-gated topic initialization |

## Quickstart

```bash
# 1. Start the stack (topics are auto-created once Kafka is healthy)
docker compose up -d

# 2. Wait ~60-90s for Druid to settle, then submit the ingestion supervisors
python setup_druid.py

# 3. Start the generator (override any stream's rate from the CLI)
python -m src.main --kafka-only --demand-rate 2 --iot-rate 10
```

Open the Druid console at **http://localhost:8888** — all six supervisors
should show `RUNNING` with active `index_kafka_*` tasks.

### Verify end-to-end

In the console's Query tab (rows are queryable seconds after production —
no need to wait for segment handoff):

```sql
SELECT MAX(__time) AS latest_event, COUNT(*) AS total_rows
FROM "iot_telemetry"
```

## Configuration Highlights

- **Kafka dual listeners** — `INTERNAL://kafka:29092` for containers (Druid,
  topic init, healthchecks) and `EXTERNAL://localhost:9092` for host clients
  (the generator). See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for why this
  split is required.
- **Coordinator-as-overlord** — the Druid `coordinator` container also runs
  the overlord role (matching the official Druid Compose example); there is
  deliberately no standalone overlord service.
- **Shared deep storage** — `/opt/shared` is one Docker volume mounted on the
  coordinator, historical, and middleManager so segments written by ingestion
  tasks are visible to the historical.
- **Peon sizing for Apple Silicon** — processing buffers and task JVM heaps
  are sized so each peon's direct-memory requirement
  (`buffer.sizeBytes × (numThreads + numMergeBuffers + 1)`) fits within its
  `MaxDirectMemorySize`, with `druid_worker_capacity=8` allowing all six
  streams to ingest concurrently under amd64 emulation.

## Troubleshooting

Full operational runbook — lifecycle commands, Kafka and Druid diagnostics,
and a symptom→cause→fix map for every failure mode encountered while
building this — in **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)**.

## Lessons Learned

- **Kafka's advertised listeners are the #1 hybrid-networking trap.** A
  client's *first* connection can succeed while every subsequent one fails,
  because the broker's metadata response redirects clients to the advertised
  address — which must be resolvable *from the client's network*, not the
  broker's.
- **Druid env-var properties fail silently.** A misspelled property name
  (`druid_zk_serviceHost` vs `druid_zk_service_host`) is ignored without any
  warning, and malformed JSON in `javaOptsArray` crashes the service at
  startup. Validate the environment file line by line.
- **The Druid docker image's `coordinator` command runs
  coordinator-as-overlord.** Adding a separate overlord container creates
  dual overlords and split task leadership — tasks run but never appear in
  the console.
- **Task failures are invisible until task logs have a home.** Configuring
  `druid.indexer.logs.directory` on a shared volume turns "tasks silently
  vanish" into a readable stack trace naming the exact problem.
