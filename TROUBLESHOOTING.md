# Troubleshooting Guide

Operational runbook for the supply chain analytics stack (Kafka + Druid +
Postgres + ZooKeeper on Docker Compose). See [README.md](README.md) for the
project overview and quickstart.

Runs on Apple Silicon (Druid images execute under `linux/amd64` emulation).

---

## Architecture Quick Reference

| Component | Container | Address (inside Docker) | Address (from host/Mac) |
|---|---|---|---|
| Kafka broker (INTERNAL listener) | `kafka` | `kafka:29092` | — |
| Kafka broker (EXTERNAL listener) | `kafka` | — | `localhost:9092` |
| Druid console (router) | `druid-router` | `druid-router:8888` | `http://localhost:8888` |
| Druid broker (SQL API) | `druid-broker` | `druid-broker:8082` | `http://localhost:8082` |
| Postgres metadata | `druid-metadata` | `druid-metadata:5432` | `localhost:5432` |
| ZooKeeper | `zookeeper` | `zookeeper:2181` | `localhost:2181` |

**The golden rule:** anything running *inside* Docker (Druid supervisor specs,
`kafka-init`, healthchecks) uses `kafka:29092`. Anything running *on the Mac*
(the data generator) uses `localhost:9092`. Mixing these up causes DNS
failures (host side) or CONNECTING_TO_STREAM (Druid side).

---

## Stack Lifecycle Commands

| Command | What it does | When to use |
|---|---|---|
| `docker compose up -d` | Start all services in the background | Normal startup |
| `docker compose down` | Stop and remove containers (volumes preserved) | Config changes to compose file |
| `docker compose down -v` | Stop and **wipe all volumes** (Kafka data, Postgres metadata, Druid segments) | Full clean reset; requires re-running `setup_druid.py` after |
| `docker compose up -d --force-recreate druid-middlemanager` | Recreate one service, picking up `environment` file changes | After editing the `environment` file (a plain restart does NOT reload env vars) |
| `docker compose restart <service>` | Restart a container without recreating it | Transient hang; does NOT pick up env changes |
| `docker compose ps` | Show status of all stack services | Quick health overview |

---

## Inspection & Diagnosis Commands

| Command | What it does | When to use |
|---|---|---|
| `docker ps --format '{{.Names}}\t{{.Status}}'` | Compact list of running containers and uptime/health | First check when anything misbehaves |
| `docker logs druid-middlemanager --tail 50` | Last 50 log lines from a container | Service crashed or won't register (e.g. env parse errors) |
| `docker logs -f kafka` | Follow logs live | Watching startup or reproducing an error |
| `docker exec -it kafka sh` | Open a shell inside a container | Interactive poking around |
| `docker inspect kafka --format '{{json .State.Health}}'` | Show healthcheck status and recent probe results | Container stuck in `starting`/`unhealthy` |
| `docker stats --no-stream` | One-shot CPU/memory usage per container | Suspected memory pressure (emulated Druid JVMs) |

---

## Kafka Troubleshooting Commands

| Command | What it does | When to use |
|---|---|---|
| `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list` | List all topics | Verify `kafka-init` created the six `supply_chain.*` topics |
| `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic supply_chain.iot_telemetry` | Show partitions/replication for a topic | Confirm partition counts match expectations |
| `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic supply_chain.demand_events --from-beginning --max-messages 5` | Print sample messages from a topic | Prove the generator's events actually landed in Kafka |
| `docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic supply_chain.iot_telemetry` | Show latest offset per partition | Check whether the topic is growing (run twice, compare) |
| `nc -zv localhost 9092` | Test EXTERNAL listener reachability from the Mac | Generator reports DNS/connection errors |
| `docker exec druid-coordinator sh -c "timeout 3 bash -c '</dev/tcp/kafka/29092' && echo OK \|\| echo FAIL"` | Test INTERNAL listener reachability from a Druid container | Supervisors stuck in CONNECTING_TO_STREAM |

---

## Druid Troubleshooting Commands

| Command | What it does | When to use |
|---|---|---|
| `curl -s http://localhost:8888/status \| jq .version` | Confirm the router/console is up | Console page won't load |
| `curl -s http://localhost:8888/druid/indexer/v1/supervisor \| jq .` | List supervisor IDs via API | Scripted health checks |
| `curl -s http://localhost:8888/druid/indexer/v1/supervisor/demand_events/status \| jq '.payload.detailedState'` | One supervisor's detailed state | Distinguish CONNECTING_TO_STREAM vs UNHEALTHY_TASKS without the UI |
| `docker exec druid-middlemanager sh -c 'ls -t /opt/shared/indexing-logs \| head -5'` | List newest task log files | Find failed-task logs |
| `docker exec druid-middlemanager sh -c 'tail -60 /opt/shared/indexing-logs/$(ls -t /opt/shared/indexing-logs \| head -1)'` | Tail the most recent task log | **The** command for diagnosing failing peons (memory errors, parse errors) |
| `curl -s -X POST http://localhost:8888/druid/indexer/v1/supervisor/demand_events/reset` | Reset a supervisor (clears failure backoff and offsets state) | Supervisor stuck UNHEALTHY after the root cause is fixed |
| `curl -s "http://localhost:8888/druid/v2/sql" -H 'Content-Type: application/json' -d '{"query":"SELECT COUNT(*) FROM \"iot_telemetry\""}'` | Run SQL from the terminal | End-to-end ingestion verification without the console |

---

## Failure Symptom → Likely Cause Map

| Symptom | Likely cause | Fix |
|---|---|---|
| Generator: `DNS lookup failed for kafka:29092` | Host client received Docker-internal advertised listener | Generator must use `localhost:9092`; broker needs dual INTERNAL/EXTERNAL listeners |
| Supervisor: `CONNECTING_TO_STREAM` forever | Supervisor spec `bootstrap.servers` unreachable from inside Docker (e.g. `localhost:9092`) | Set `kafka:29092` in `setup_druid.py`, resubmit specs |
| Supervisor: `UNHEALTHY_TASKS` | Tasks spawn but crash — check the newest task log | Commonly peon direct-memory: `buffer.sizeBytes × (threads + mergeBuffers + 1)` must fit `MaxDirectMemorySize` |
| Console: `2 overlords` on Services tile | Separate overlord container alongside coordinator (the image's `coordinator` command already runs coordinator-as-overlord) | Remove the standalone overlord service |
| Tasks run but never appear in console | Split overlord leadership (see above) | Same fix; clean reset (`down -v`) clears confused metadata |
| Supervisors RUNNING but some show "No running tasks" | Task demand exceeds worker slots (supervisors × taskCount > `druid.worker.capacity`) | Raise `druid_worker_capacity` in `environment` and force-recreate the middleManager |
| middleManager missing from Services tab | Crashed at startup — often a malformed `environment` line (invalid JSON in `javaOptsArray`) | `docker logs druid-middlemanager`, fix the line, force-recreate |
| Segments never hand off / historicals empty | Deep storage path not on a shared volume | `/opt/shared` must be the same Docker volume on coordinator, historical, and middleManager |

---

## Standard Startup Sequence

```bash
docker compose up -d          # kafka-init auto-creates the six topics
# wait for Druid services to settle (~60-90s under amd64 emulation)
python setup_druid.py         # submit the six Kafka ingestion supervisors
python -m src.main --kafka-only   # start the data generator (from the Mac)
```

Verify in the console at `http://localhost:8888`:
Supervisors → all six `RUNNING` → Tasks → `index_kafka_*` tasks RUNNING →
Query tab → `SELECT MAX(__time), COUNT(*) FROM "iot_telemetry"`.
