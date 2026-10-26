# Handoff — Part A.2: Recommender Systems with Spark Streaming

## Objective

Complete a production-minded, reproducible version of the Part A.2 notebook while preserving the original demonstration notebook. The deliverable must demonstrate both a file-based synthetic baseline and an end-to-end Kafka run on the real Retail Rocket data.

## Verified local state

- Repository path is ASCII-safe for Windows Spark batch scripts: `C:\work\hoc_ki_7\big data\Kafka_and_Spark_streaming_subject`.
- Java 17, Python virtual environment, and PySpark 4.0.1 are installed. A local `SparkSession` successfully ran `range(3).count()`.
- Native Windows emits a `winutils.exe` warning, but it has not blocked a local Spark job. Do not add Hadoop/winutils unless a later operation actually fails because of it.
- Docker Kafka is running from the existing Compose file (`apache/kafka:4.3.1`), with the host listener `127.0.0.1:9092` and internal listener `kafka:19092`.
- Topic `retail-rocket-events` has two partitions. A `kafka-python` producer and consumer smoke test succeeded; events with the same `visitorid` key went to the same partition.
- Real data is available at `data/events.csv`: 2,756,101 rows; schema `timestamp,visitorid,event,itemid,transactionid`.

## Non-negotiable working rules

1. Never modify `Part_A/A2_Recommender_Systems_Spark_Streaming.ipynb`. Create and work only on `Part_A/A2_Recommender_Systems_Spark_Streaming_v2.ipynb`.
2. Locate notebook cells by a stable first-line marker, never by numeric cell index. Use `nbformat` for programmatic edits.
3. Do not hand-edit notebook outputs. Final outputs must come from a fresh kernel restart and a full real-data run.
4. After each logical workstream, rerun the file/synthetic baseline before continuing. If a validation check fails, stop and report the observed result; do not weaken the check.
5. Notebook prose remains English. This handoff is implementation guidance only.
6. Preserve the user's existing modification to `docker-compose.yml`; do not replace it with a Kafka 3.9 example.

Suggested cell lookup helper:

```python
import nbformat

PATH = "Part_A/A2_Recommender_Systems_Spark_Streaming_v2.ipynb"
nb = nbformat.read(PATH, as_version=4)

def find(marker):
    hits = [i for i, cell in enumerate(nb.cells)
            if cell.source.lstrip().startswith(marker)]
    assert len(hits) == 1, (marker, hits)
    return hits[0]
```

## Confirmed design decisions

### Intent and reporting

- Recommendation triggers remain at `(visitorid, itemid, window)`.
- Add `visitor_score` as the sum of every item-level `intent_score` for that visitor/window. It is report-only; it is not a replacement for the item-level trigger or seed item.

### Deduplication and cooldown

- Cooldown is 30 minutes in **event time** for the same `(visitorid, seed_item)`.
- Do not use `current_timestamp()` to decide the cooldown: replay runs faster than real time.
- A previously recommended `rec_item` is suppressed only during the same event-time cooldown, not forever.
- Continue to exclude items seen in the active visitor windows from recommendations.

### Data modes and work directories

- Replace `REQUIRE_REAL` with `DATA_MODE = os.environ.get("DATA_MODE", "synthetic")`.
- Allowed values are `synthetic` and `real`; reject anything else.
- `DATA_MODE="real"` must raise `FileNotFoundError` if `data/events.csv` is absent.
- Use separate work roots: `WORK_DIR = Path("work") / DATA_MODE`. A synthetic run must never delete a real run's checkpoints, Parquet output, logs, or sent log.

### Runtime and resource policy

- Start with `SPARK_MASTER=local[2]`, `DRIVER_MEMORY=4g`, and `SHUFFLE_PARTITIONS=16`.
- The machine has about 7.8 GB RAM; the RocksDB state store and Windows/Docker use memory outside the Java driver heap.
- Before a full run, run (a) load/funnel/item-similarity on real data, (b) a file replay for a few hours, then (c) the full Kafka run.
- If item similarity is too slow or out of memory, first change the active-basket upper bound from 100 to 50. Only then consider `DRIVER_MEMORY=5g`, and document the adjustment in notebook prose.

### Kafka client and Compose integration

- Keep `kafka-python`; it is already installed and verified.
- Host Python must use `127.0.0.1:9092`, not `localhost:9092`.
- Use `docker compose exec -T kafka ...`, not `docker exec kafka ...`; the current Compose project uses generated container names.
- Retain Kafka 4.3.1 and its existing dual-listener configuration. Add `KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m"` to the existing Kafka environment when editing Compose.

## Workstreams

### WS1 — Environment, dependencies, and copy

1. Copy the original notebook to the v2 filename.
2. Update `requirements.txt` to include the Python dependencies used by the v2 notebook and its validation tooling:

   ```text
   kafka-python==3.0.11
   pyspark==4.0.1
   pandas
   numpy
   matplotlib
   pyarrow
   nbformat
   nbconvert
   notebook
   ipykernel
   ```

3. Run a smoke execution of the unmodified v2 copy in synthetic/file mode. On Windows use PowerShell environment syntax and invoke the venv executable, for example:

   ```powershell
   $env:DATA_MODE='synthetic'; $env:SOURCE='file'
   .\.venv\Scripts\jupyter.exe nbconvert --to notebook --execute Part_A\A2_Recommender_Systems_Spark_Streaming_v2.ipynb --output smoke.ipynb --ExecutePreprocessor.timeout=3600
   ```

   This smoke run comes after the configuration changes in WS2 if the original Windows `tzset` call prevents execution.

### WS2 — Configuration and Spark session

In `# 5.0  Configuration`:

```python
import os, json, time, shutil, logging
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

DATA_MODE = os.environ.get("DATA_MODE", "synthetic").lower()
if DATA_MODE not in {"synthetic", "real"}:
    raise ValueError("DATA_MODE must be 'synthetic' or 'real'")

SOURCE = os.environ.get("SOURCE", "file").lower()  # file | kafka
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
SHUFFLE_PARTS = int(os.environ.get("SHUFFLE_PARTITIONS", "16"))
DRIVER_MEMORY = os.environ.get("DRIVER_MEMORY", "4g")
STREAM_HOURS = int(os.environ.get("STREAM_HOURS", "3"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
KAFKA_STARTING = "earliest"
COOLDOWN_MIN = 30

WORK_DIR = Path("work") / DATA_MODE
STREAM_DIR = WORK_DIR / "stream_in"
CKPT_DIR = WORK_DIR / "checkpoints"
OUT_DIR = WORK_DIR / "recommendations_parquet"
LOG_DIR = WORK_DIR / "logs"
SENT_LOG_DIR = WORK_DIR / "sent_log"
```

Include `SENT_LOG_DIR` in the directory creation loop. Do not use one shared `_rec_demo` directory.

In `# 5.1  Spark session`, use `.master(SPARK_MASTER)`, set `spark.sql.shuffle.partitions` from `SHUFFLE_PARTS`, and add `spark.driver.memory=DRIVER_MEMORY`.

In the data-loading cell, use `DATA_MODE` explicitly:

```python
if DATA_MODE == "real":
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"{DATA_PATH} missing while DATA_MODE='real'")
    src = DATA_PATH
else:
    if not SAMPLE_PATH.exists():
        generate_retailrocket_like(SAMPLE_PATH)
    src = SAMPLE_PATH

print(f"DATA MODE: {DATA_MODE.upper()} | using: {src}")
```

### WS3 — Similarity normalization

Keep co-occurrence and its denominator on the same visitor population:

```python
vi = vi.join(active, "visitorid")
pop_sim = vi.groupBy("itemid").agg(F.sum("w").alias("pop"))
```

Use `pop_sim` for the two denominator joins that calculate `similarity`. Keep a separate `pop_all`, calculated from all of history, for the popularity fallback. Validate that maximum similarity is no greater than 1.0 and record item-similarity row counts before and after the change.

### WS4 — Visitor-window aggregation

First attempt the following stateful aggregation in `# ---- EXTRACT`:

```python
intent = (parsed
    .withWatermark("event_time", WATERMARK)
    .groupBy(F.window("event_time", WINDOW, SLIDE).alias("window"), "visitorid")
    .agg(
        F.collect_list(F.struct("itemid", "weight", "is_view", "is_cart", "is_txn")).alias("items"),
        F.max("event_time").alias("last_event_time"),
    ))
```

Validate that it is streaming and its schema contains `window`, `visitorid`, `items`, and `last_event_time`. If Spark rejects `collect_list` in this streaming plan or state becomes impractical, stop and obtain approval before changing to a durable `seen_log` fallback.

### WS5 — Recommendation batch, seen items, cooldown, and empty batches

Replace the `# ---- structured logger` cell. Keep the existing logger, `batch_results`, and `batch_meta`, then implement:

- immediate return for `batch_df.isEmpty()`;
- explode `items` into per-item intent scores;
- exclude seed items with a transaction in any window present in the batch;
- event-time cooldown for seeds and recommendation items;
- idempotent Parquet writes by `batch_id`;
- a durable Parquet `sent_log`.

Use this sent-log schema and helpers:

```python
from pyspark.sql.types import TimestampType

SENT_SCHEMA = StructType([
    StructField("visitorid", IntegerType()),
    StructField("seed_item", IntegerType()),
    StructField("rec_item", IntegerType()),
    StructField("sent_event_time", TimestampType()),
    StructField("batch_id", IntegerType()),
])
COOL = F.expr(f"INTERVAL {COOLDOWN_MIN} MINUTES")

def read_sent_log(exclude_batch_id):
    if not any(SENT_LOG_DIR.glob("batch_id=*")):
        return spark.createDataFrame([], SENT_SCHEMA)
    return (spark.read.schema(SENT_SCHEMA).parquet(str(SENT_LOG_DIR))
        .filter(F.col("batch_id") != exclude_batch_id))

def explode_items(batch_df):
    ex = (batch_df.select("visitorid", "window", F.explode("items").alias("it"))
        .select("visitorid", "window", "it.*"))
    per_item = (ex.groupBy("visitorid", "window", "itemid")
        .agg(
            F.sum("weight").alias("intent_score"),
            F.sum("is_view").alias("views"),
            F.sum("is_cart").alias("carts"),
            F.sum("is_txn").alias("txns"),
        ))
    seen_win = ex.select("visitorid", F.col("itemid").alias("rec_item")).distinct()
    return per_item, seen_win

def drop_recently_sent(df, sent_log):
    recent = (sent_log.select(
        F.col("visitorid").alias("r_v"),
        F.col("rec_item").alias("r_i"),
        F.col("sent_event_time").alias("r_t"),
    ).distinct())
    return df.join(
        recent,
        (df.visitorid == recent.r_v)
        & (df.rec_item == recent.r_i)
        & (df.window_end < recent.r_t + COOL),
        "left_anti",
    )
```

Implementation requirements:

1. Materialize `batch_df` only after the empty-batch early return.
2. Compute `visitor_score` from all `per_item` rows in the visitor/window; include it in report/context output, but do not use it to replace the item trigger.
3. The seed cooldown is on `(visitorid, seed_item)` and uses `window_end` against `sent_event_time`.
4. `cand` must select `window_end` from `seeds`, then call `drop_recently_sent(cand, sent_log)` before ranking.
5. Do **not** union every historical `sent_log.rec_item` into `seen`. Historical recommended items are not permanently excluded.
6. For popularity fallback, carry a `window_end` per missing visitor (for example, `max(window_end)`), call `drop_recently_sent`, then drop its temporary `window_end` before joining context to avoid duplicate column names.
7. Write the sent log after recommendation output. Exclude the current batch ID when reading it so a crash/replay does not suppress its own batch.
8. Preserve `seeds_raw`, `seeds_after_cooldown`, and the existing batch metrics in `batch_meta`.

Add two checks after the curated-sink cell:

```python
out = spark.read.parquet(str(OUT_DIR))

# The same recommendation may recur, but never inside its event-time cooldown.
d = (out.select("visitorid", "rec_item", "window_end").distinct().toPandas()
    .sort_values(["visitorid", "rec_item", "window_end"]))
gap = d.groupby(["visitorid", "rec_item"])["window_end"].diff().dropna()
assert (gap >= pd.Timedelta(minutes=COOLDOWN_MIN)).all()

# A recommended item was not seen in the trigger's active 10-minute window.
chk = out.select("visitorid", "rec_item", "window_end").distinct().toPandas()
ev = live_pdf.assign(t=pd.to_datetime(live_pdf.timestamp, unit="ms"))
m = chk.merge(ev, left_on=["visitorid", "rec_item"], right_on=["visitorid", "itemid"])
bad = m[(m.t <= m.window_end) & (m.t > m.window_end - pd.Timedelta(minutes=10))]
assert bad.empty, bad.head()
print("cooldown + seen checks passed")
```

Normalize timestamp timezone representations if pandas compares aware and naive timestamps.

The cooldown regression test must publish three new cart events for an already-triggered visitor and confirm output count does not grow inside the cooldown. It must work through the same `publish` abstraction as file or Kafka mode.

### WS6 — Kafka end-to-end

Create `kafka_producer.py` beside the notebook. It must use the verified `kafka-python` API and wait for delivery futures:

```python
import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer

def make_producer(bootstrap="127.0.0.1:9092"):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        retries=5,
        linger_ms=20,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: value.encode("utf-8"),
    )

def to_payload(row):
    return json.dumps({
        "timestamp": int(row.timestamp),
        "visitorid": int(row.visitorid),
        "event": row.event,
        "itemid": int(row.itemid),
        "transactionid": None if pd.isna(row.transactionid) else int(row.transactionid),
    })

def publish_df(producer, topic, df):
    futures = [
        producer.send(topic, key=row.visitorid, value=to_payload(row))
        for row in df.itertuples(index=False)
    ]
    producer.flush()
    for future in futures:
        future.get(timeout=30)
    return len(futures)
```

The CLI may load, time-sort, and publish the final N hours in batches. Keys must remain `visitorid` so events for one visitor stay ordered in one partition.

Notebook changes:

1. In the Kafka source, use `.option("startingOffsets", KAFKA_STARTING)`.
2. Start a fresh kernel whenever switching `SOURCE`, because `spark.jars.packages` is read only when the first Spark session is created.
3. Add a `publish(idx, frame)` abstraction: file mode calls the existing atomic-file publisher; Kafka mode calls `kafka_producer.publish_df`.
4. Replace every direct `publish_slice(...)` call in the replay, late-data, cooldown, and restart paths with `publish(...)`.
5. In Kafka replay output, print `lastProgress["sources"][0]["endOffset"]`, with a guard for missing progress and JSON-string offsets.
6. Reset the Kafka topic only in Kafka mode before the query starts, then print `--describe` evidence. Invoke Kafka CLI through `docker compose exec -T kafka`, not a hard-coded container name.

Kafka acceptance evidence:

- `--describe` reports `PartitionCount: 2`.
- `endOffset` contains partitions `0` and `1`, and the total reflects produced records.
- Parquet contains recommendations.
- Restart with the same checkpoint reads zero old rows and continues batch IDs.

### WS7 — Real-data run

Run short real-data stages first. For final native-Windows execution use PowerShell, not bash:

```powershell
$env:DATA_MODE='real'; $env:SOURCE='kafka'; $env:SPARK_MASTER='local[2]'
$env:DRIVER_MEMORY='4g'; $env:SHUFFLE_PARTITIONS='16'; $env:STREAM_HOURS='24'
.\.venv\Scripts\jupyter.exe nbconvert --to notebook --execute Part_A\A2_Recommender_Systems_Spark_Streaming_v2.ipynb --output A2_final_real_kafka.ipynb --ExecutePreprocessor.timeout=-1
```

The live slice must be large enough to make the replay meaningful. Report the actual live-event count and adjust `STREAM_HOURS` if necessary; do not invent metrics from the synthetic run.

### WS8 — Offline evaluation

After the sink, add an offline proxy evaluation: HitRate@5 over the next 60 minutes for item-CF against a popularity baseline. Report both:

- all future actions (`view`, `addtocart`, `transaction`);
- higher-intent actions only (`addtocart`, `transaction`).

Always report the trigger count `n` and state that this is an offline proxy, not causal uplift. `item_sim` must be trained from history only.

### WS9 — Prose and observability corrections

1. Section 6 must describe an idempotent/effectively-once Parquet sink per `batch_id`, not end-to-end exactly-once.
2. State that deterministic behavior applies to business fields; `generated_at` and `collect_set` ordering are exceptions.
3. Section 7.3 must say that state is per visitor-window, recommendations exclude active-window seen items, and the same seed/recommendation is suppressed only for the event-time cooldown. Do not claim permanent exclusion of recommended items.
4. Section 3.3 must clarify that `cart_no_purchase` means no purchase observed **yet in the window**, not confirmed abandonment.
5. Print per-new-batch late-data state metrics: batch ID, input rows, rows dropped by watermark, rows updated, and total state rows. Do not equate an aggregate operator metric directly to the number of late input events.
6. Label the "no prior history" statistic with `DATA_MODE`; synthetic data must not be presented as a real-data conclusion.
7. Section 5.5 must state that local-demo latency is not a production benchmark.

### WS10 — Final acceptance

The final run must verify all of the following:

1. `DATA MODE: REAL` appears in output.
2. Kafka topic evidence shows two partitions and offsets from both partitions.
3. Seen-item and cooldown assertions pass.
4. Empty batches do not run the expensive recommendation work or produce `foreachBatch_ms` metadata.
5. The checkpoint restart reads zero historic rows and continues batch IDs.
6. The late-event demo shows accepted versus too-late behavior and prints the relevant state metrics.
7. HitRate@5 for item-CF and popularity is printed with trigger counts.
8. No notebook prose claims "end-to-end exactly-once".
9. Deliverables include the final executed real/Kafka notebook, `kafka_producer.py`, the updated Compose and requirements files, and the untouched original notebook.

## Known failure modes

## Linux-container execution path

Native Windows Spark reached a real `NativeIO$Windows.access0` failure while opening a streaming file source. The repository now includes `Dockerfile.spark` and a Compose Spark service based on `apache/spark:4.2.0-python3`. The container installs the notebook dependencies, mounts `./data`, and uses the internal Kafka listener `kafka:19092`.

After Docker Desktop is healthy again, use:

```powershell
docker compose build spark
docker compose up -d kafka
docker compose run --rm -e DATA_MODE=synthetic -e SOURCE=file -e SYNTHETIC_SESSIONS=500 -e STREAM_HOURS=1 spark jupyter nbconvert --to notebook --execute Part_A\A2_Recommender_Systems_Spark_Streaming_v2.ipynb --output smoke_docker_small.ipynb --ExecutePreprocessor.timeout=3600
```

For the real Kafka run, keep the Compose defaults (`DATA_MODE=real`, `SOURCE=kafka`, `KAFKA_BOOTSTRAP=kafka:19092`) and execute the notebook from the Spark container. The notebook selects the Kafka connector version from `SPARK_KAFKA_PACKAGE_VERSION` so it matches Spark 4.2.0 in the container; host execution defaults to 4.0.1.

| Symptom | Required response |
| --- | --- |
| Spark rejects `collect_list` in the streaming aggregation | Stop and request approval for the durable `seen_log` fallback. |
| Cannot resolve the Spark Kafka connector | Confirm internet access; report the blocked Maven download rather than inventing a workaround. |
| Producer cannot connect | Check `docker compose ps`, then use `127.0.0.1:9092`; do not revert the verified advertised listener. |
| Kafka source reads zero rows | Restart the notebook kernel after `SOURCE='kafka'`, reset the topic, and verify `publish` actually used Kafka mode. |
| Item-similarity job runs out of memory | Lower basket cap from 100 to 50 before increasing driver memory. Document any cap change. |
| `winutils.exe` warning becomes an actual failure | Report the exact operation and error before adding a Windows Hadoop workaround. |
