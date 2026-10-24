# log-db

A prototype database whose only durable state is a shared log (currently [S2](https://s2.dev)). No WAL tier, no consensus cluster, no replication protocol.

- Transactions are stored procedures (`put`, `delete`, `incr`, `transfer`).
- A node batches submitted transactions into an epoch and commits it with one append. The log records transaction inputs, not effects.
- Every node applies the log in order. Execution is deterministic, so every node reaches the same state and computes the same outcomes. A replica is any node that reads the log.
- Any number of nodes can write. `--append conditional` appends only at the tail the node has applied up to and retries at the reported tail on a lost race; `--append unconditional` lets the log order appends and needs no races.
- Each batch carries its writer's ID and a batch number. Appends with an unknown outcome are retried; every node skips a batch number it has already applied.
- Queries run against the node's in-memory state. Passing `min_seq` waits until the node has applied that far (read-your-writes across nodes).

Background and design rationale: [docs/research.md](docs/research.md). Log benchmarks: [bench/](bench/).

## Run

`.env` holds `S2_API_KEY`; set `S2_BASIN` there or pass `--basin`.

```bash
cargo build --release
```

In-process benchmark (bank transfers; verifies all nodes end identical and money is conserved):

```bash
./target/release/logdb bench                                  # in-memory log, 30 ms simulated RTT
./target/release/logdb bench --log s2 --basin logdb-bench-976641 --clients 2000
./target/release/logdb bench --log s2 --basin logdb-bench-976641 --writers 2 --replicas 2
```

HTTP node (run several on the same `--stream`):

```bash
./target/release/logdb serve --log s2 --basin logdb-bench-976641 --stream demo --listen 127.0.0.1:7000
curl -XPOST localhost:7000/txn -H 'content-type: application/json' -d '{"op":"put","key":"bal/1","value":100}'
curl -XPOST localhost:7000/txn -H 'content-type: application/json' -d '{"op":"transfer","from":"bal/1","to":"bal/2","amount":30}'
curl 'localhost:7000/scan?prefix=bal/&min_seq=2'
curl localhost:7000/get/bal%2F1
curl localhost:7000/status
```

### Native distributed benchmark

Each worker generates bank transfers through native `Node::submit`; HTTP carries only control and reports, not transactions. The control API is unauthenticated: bind it only on a trusted local/tailnet network. Synchronize clocks on all hosts, including the coordinator.

Start all workers on the same **fresh S2 stream**, with matching append/epoch settings. Export `S2_API_KEY` and `S2_BASIN` on each writer host first. For example, on two writer hosts (replace the tailnet IPs and `bench-UNIQUE`):

```bash
# Writer A
./target/release/logdb bench-worker --log s2 --basin "$S2_BASIN" --stream bench-UNIQUE --append unconditional --listen 100.64.0.11:7100
# Writer B
./target/release/logdb bench-worker --log s2 --basin "$S2_BASIN" --stream bench-UNIQUE --append unconditional --listen 100.64.0.12:7100

# Coordinator: 2,000 clients total, split across the workers
./target/release/logdb bench --worker http://100.64.0.11:7100 --worker http://100.64.0.12:7100 \
  --clients 2000 --accounts 10000 --seconds 20 --output distributed.json
```

The coordinator seeds once, schedules a common measurement window, drains outstanding requests, and verifies identical state, balances, and transaction counts. Reports separate timed throughput from drained acknowledgements and pool worker latency histograms. `--output` saves JSON; failures exit nonzero. Workers are one-shot: restart every worker with a new fresh stream for each run. [Five-VM S2 results](bench/results/writers-orbs.md).

## First numbers (laptop → S2 Express us-west-2, ~30 ms RTT)

| Setup | Throughput | p50 | p99 |
|---|---|---|---|
| 1 writer, 2,000 clients | 23.5k txn/s | 82 ms | 150 ms |
| 1 writer, 10,000 clients | 51k txn/s | 171 ms | 360 ms |
| 2 writers, 2,000 clients | 11k txn/s | 171 ms | 359 ms |

All runs: replicas identical to writers, money conserved.

- One log commits ~12 epochs/s from the laptop (one append in flight at a time). Throughput comes from batch size: up to ~10k transfers per epoch.
- The 10k-client run is bound by laptop upload (750 KB epochs).
- Multiple writers add latency, not throughput: they alternate epochs and nearly every epoch costs one lost race.

Epoch wait (`--epoch-ms`, default 0): with 0, a node sends whatever queued during the previous append as soon as it returns. Against a 5 ms wait, 0 gives ~5 ms lower p50 and 5–15% more throughput. With 2,000 closed-loop clients its p99 can be worse: stragglers miss a wave and form a small epoch of their own. On S2 from the laptop the difference is within run-to-run noise.

| Log, clients | `--epoch-ms` | Throughput | p50 | p99 |
|---|---|---|---|---|
| mem 30 ms, 100 | 0 | 2,882 txn/s | 34.6 ms | 41.2 ms |
| mem 30 ms, 100 | 5 | 2,480 txn/s | 40.5 ms | 42.4 ms |
| mem 30 ms, 2,000 | 0 | 50,704 txn/s | 38.9 ms | 73.2 ms |
| mem 30 ms, 2,000 | 5 | 44,000 txn/s | 45.4 ms | 52.7 ms |
| S2, 100 (2 runs) | 0 | 1,629 / 1,520 txn/s | 60.3 / 62.0 ms | 86 / 157 ms |
| S2, 100 (2 runs) | 5 | 1,470 / 1,440 txn/s | 65.7 / 65.6 ms | 164 / 220 ms |
| S2, 2,000 (2 runs) | 0 | 24,081 / 26,117 txn/s | 77.2 / 73.3 ms | 157 / 145 ms |
| S2, 2,000 (2 runs) | 5 | 24,700 / 25,800 txn/s | 79.4 / 77.7 ms | 142 / 120 ms |

## Multiple writers

`bench/scripts/writers.sh 15 2000 --append <mode> --log s2 --basin logdb-bench-976641`: 2,000 clients spread over 1–10 writer nodes, 1 replica, laptop → S2. Full tables, both epoch waits and the in-memory log: [bench/results/writers-s2.md](bench/results/writers-s2.md), [bench/results/writers-mem.md](bench/results/writers-mem.md). Epoch wait 0 below.

| writers | conditional txn/s | p50 ms | p99 ms | lost races/epoch | unconditional txn/s | p50 ms | p99 ms | epochs/s |
|---|---|---|---|---|---|---|---|---|
| 1 | 27,811 | 70.2 | 150.9 | 0 | 26,167 | 68.7 | 340.9 | 13.5 |
| 2 | 12,121 | 164.3 | 380.4 | 0.97 | 23,379 | 74.2 | 188.5 | 23.6 |
| 4 | 7,421 | 238.9 | 604.6 | 2.58 | 18,443 | 113.0 | 300.6 | 37.6 |
| 6 | 5,504 | 255.7 | 1,698.2 | 4.71 | 15,786 | 132.0 | 319.9 | 48.6 |
| 8 | 4,129 | 312.1 | 2,189.4 | 6.70 | 12,566 | 159.4 | 389.3 | 51.1 |
| 10 | 3,364 | 413.2 | 3,965.6 | 8.50 | 8,917 | 187.2 | 814.8 | 45.3 |

All 48 runs (S2 and in-memory): every node identical, money conserved. With 10% of in-memory acknowledgements dropped (`--mem-lost-acks 0.1`), retried batches landed twice and every node skipped the same duplicates.

- **Conditional:** the log commits a fixed ~15 epochs/s however many nodes write. Each epoch costs about writers − 1 lost races; p99 reaches 4 s at 10 writers.
- **Unconditional:** no races. Epochs/s grows with writers up to ~50/s, then flattens; on the in-memory log it keeps growing (165/s at 10 writers). The S2 ceiling is unexplained from the laptop: S2 under concurrent appends, the 10 in-process nodes each downloading every epoch, or the laptop link.
- **Latency floor is two round trips** with several writers: the append, then reading the other writers' records that landed before it. One writer needs only the append.
- Throughput still falls with writers because each writer batches only its own clients; total clients stay at 2,000.

## SQL mode: SQLite over a command log and an effect log

`src/sql/`. Two S2 streams per database: `<stream>-cmd` and `<stream>-fx`.

- **Writers** append SQL transactions to the command log, unconditionally, and report the position. They keep no state.
- **Producers** execute command records on SQLite and append the changed pages plus every transaction's outcome to the effect log, conditionally at the next effect position. With several producers the first append wins; losers discard their work, apply the winner's effect and, when it covers the same commands, compare digests.
- **Readers** (every node) apply effects to a versioned in-memory page store and never run SQL writes. Queries see snapshots of it.
- Clients get outcomes only from the effect log, so no client sees a result that isn't canonical. A transaction is committed once its command is durable; its outcome follows with the effect.

Details:

- **VFS** (`vfs.rs`): SQLite reads and writes pages through a custom VFS backed by the page store. The producer's connection writes into a pending overlay that becomes the next version when its effect lands. Query connections pin the latest version when SQLite takes its shared lock, so a read transaction sees one version while new ones are installed.
- **Execution:** one SQLite transaction per command record; each client transaction runs in a savepoint, so a failing one (constraint violation, SQL error) aborts alone. A SQLite authorizer rejects transaction control, `ATTACH`, pragmas, `load_extension`, and writes to logdb's own `_logdb_*` tables, including writes from triggers.
- **JavaScript transactions** (`js.rs`): instead of statements, a transaction can be JavaScript run on QuickJS as a function body with `args` and `db` in scope. `db.query(sql, params)` returns rows as objects, `db.exec(sql, params)` the number of rows changed; the return value, as JSON, is the transaction's result, and a throw aborts it. Each thread keeps one context, frozen after setup so transactions can't leave state behind; promise jobs run before the transaction ends. Memory, stack and operation limits abort runaway code.
- **Dedup and progress live in the database:** `_logdb_writers` holds each writer's last batch number, `_logdb_meta` the next command position. Both change in the same transaction as the commands, so they travel with the pages.
- **Effects:** each page is stored in full or as XOR against its previous version, whichever leaves fewer non-zero bytes; the whole record is MessagePack compressed with zstd. Commands are compressed the same way. An effect stops taking command records at ~512 KB encoded. An effect over 1 MB (S2's record limit) is redone on half as many command records; a single record still over the limit is redone admitting its transactions one at a time, and those that don't fit abort. Effects don't spill to object storage yet.

```bash
./target/release/logdb sql-bench                                    # in-memory logs
./target/release/logdb sql-bench --log s2 --basin logdb-bench-976641 --clients 2000 --writers 3 --producers 2 --readers 2
./target/release/logdb sql-serve --log s2 --basin logdb-bench-976641 --stream demo --producer --listen 127.0.0.1:7000
./target/release/logdb sql-serve --log s2 --basin logdb-bench-976641 --stream demo --listen 127.0.0.1:7001
curl -XPOST localhost:7001/txn -H 'content-type: application/json' \
  -d '{"stmts":[{"sql":"CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER)"}]}'
curl -XPOST localhost:7001/txn -H 'content-type: application/json' \
  -d '{"stmts":[{"sql":"INSERT INTO kv VALUES (?1, ?2) RETURNING *","params":["a",1]}]}'
curl -XPOST localhost:7001/txn -H 'content-type: application/json' \
  -d '{"js":"for (const k of args) db.exec(\"INSERT INTO kv VALUES (?1, 0) ON CONFLICT DO UPDATE SET v = v + 1\", [k]); return db.query(\"SELECT * FROM kv\");","args":["a","b"]}'
curl -XPOST localhost:7000/query -H 'content-type: application/json' -d '{"sql":"SELECT * FROM kv","min_cmd":2}'
```

`sql-bench` runs bank transfers (two `UPDATE`s, `CHECK (bal >= 0)`; with `--js`, a JavaScript transaction that checks the balance itself), then checks that every node holds byte-identical pages, `PRAGMA integrity_check` passes and money is conserved.

| Setup (2,000 clients, 10k accounts) | txn/s | commit p50 | result p50 | result p99 | effects/s | KB/effect (raw) |
|---|---|---|---|---|---|---|
| mem 30 ms, 1 writer, 1 producer | 20,430 | 39 ms | 86 ms | 125 ms | 25.3 | 4.9 (112) |
| mem, 3 writers, 3 producers, 2 readers, 10% lost acks | 17,558 | 35 ms | 113 ms | 191 ms | 22.7 | 4.0–4.8 (106–112) |
| mem, 100k accounts | 20,809 | 36 ms | 87 ms | 130 ms | 23.8 | 8.7 (1,027) |
| S2, 1 writer, 1 producer, 1 reader | 7,478 | 133 ms | 280 ms | 693 ms | 13.7 | 3.5 (115) |
| S2, clients on the producer | 8,065 | 75 ms | 199 ms | 957 ms | 9.8 | 4.8 (111) |
| S2, 3 writers, 2 producers, 2 readers | 8,018 | 95 ms | 238 ms | 382 ms | 11.9 | 4.2 (114) |
| mem, JS transfers | 17,957 | 35 ms | 109 ms | 158 ms | 18.6 | 8.0 (115) |
| mem, JS, 3 writers, 2 producers, 2 readers, 10% lost acks | 15,814 | 35 ms | 115 ms | 195 ms | 17.1 | 6.8–6.9 (111–112) |
| S2, JS transfers, clients on the producer | 9,673 | 74 ms | 196 ms | 634 ms | 10.0 | 7.4 (115) |

All runs pass. S2 appends took 60–90 ms during these runs (laptop), about twice the day before. A JavaScript transfer costs ~18 µs to execute, against ~6 µs for the two `UPDATE`s.

- Changed pages compress 25–100×: a transfer touches few bytes per page, so nearly every page goes out as XOR.
- With racing producers, losers verified 35–48 effects per run against the winner's digest (43 in the in-memory run with 3 producers); none diverged. The producer that fetches ahead tends to win every race, and the other becomes a verifier.
- Result latency is about command append + effect append + reading each: two to three round trips plus waiting for the effect in flight.

## Not yet

- Pipelined appends (S2 append sessions); currently one append in flight per node.
- Checkpoints: nodes replay the whole log on start.
- SQL mode: spilling effects over 1 MB to object storage; lazy page fetch in the VFS.
- S3 Express backend.
- Runs from EC2 in-region.
