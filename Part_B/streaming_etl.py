"""Kafka -> validated events -> intent windows -> Item-CF recommendations."""

import json
import os
from pathlib import Path
import signal
import time
import uuid

from pyspark.sql import Row, SparkSession, Window, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

from recommendation import (
    RECOMMENDATION_COLUMNS,
    eligible_expression,
    generate_recommendations,
    load_model,
)


EVENT_SCHEMA = T.StructType([
    T.StructField("timestamp", T.LongType()),
    T.StructField("visitorid", T.LongType()),
    T.StructField("event", T.StringType()),
    T.StructField("itemid", T.LongType()),
    T.StructField("transactionid", T.LongType()),
])

INTENT_SCHEMA = T.StructType([
    T.StructField("window_start", T.TimestampType(), False),
    T.StructField("window_end", T.TimestampType(), False),
    T.StructField("visitorid", T.LongType(), False),
    T.StructField("itemid", T.LongType(), False),
    T.StructField("event_count", T.LongType(), False),
    T.StructField("views", T.LongType(), False),
    T.StructField("carts", T.LongType(), False),
    T.StructField("transactions", T.LongType(), False),
    T.StructField("intent_score", T.LongType(), False),
    T.StructField("recommendation_active", T.BooleanType(), False),
    T.StructField("recommendation_reason", T.StringType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("last_event_time", T.TimestampType(), True),
    T.StructField("last_kafka_time", T.TimestampType(), True),
    T.StructField("last_updated_batch", T.LongType(), False),
])

RECOMMENDATION_SCHEMA = T.StructType([
    T.StructField("visitorid", T.LongType(), False),
    T.StructField("window_start", T.TimestampType(), False),
    T.StructField("window_end", T.TimestampType(), False),
    T.StructField("candidate_itemid", T.LongType(), False),
    T.StructField("rank", T.IntegerType(), False),
    T.StructField("source", T.StringType(), False),
    T.StructField("rec_score", T.DoubleType(), True),
    T.StructField("popularity_score", T.DoubleType(), True),
    T.StructField("contributing_seed_itemids", T.ArrayType(T.LongType()), False),
    T.StructField("last_updated_batch", T.LongType(), False),
])

PUBLICATION_SCHEMA = T.StructType([
    T.StructField("kind", T.StringType(), False),
    T.StructField("schema_version", T.IntegerType(), False),
    T.StructField("pipeline_id", T.StringType(), False),
    T.StructField("batch_id", T.LongType(), False),
    T.StructField("predecessor_batch_id", T.LongType(), True),
    T.StructField("artifact_path", T.StringType(), True),
    T.StructField("model_version", T.StringType(), True),
    T.StructField("policy", T.StringType(), True),
    T.StructField("updated_window_rows", T.LongType(), True),
    T.StructField("intent_snapshot_rows", T.LongType(), True),
    T.StructField("recommendation_snapshot_rows", T.LongType(), True),
    T.StructField("visitorid", T.LongType(), True),
    T.StructField("window_start", T.TimestampType(), True),
    T.StructField("window_end", T.TimestampType(), True),
    T.StructField("eligible_seed_itemids", T.ArrayType(T.LongType()), True),
    T.StructField("generation_status", T.StringType(), True),
    T.StructField("recommendations", T.ArrayType(T.StructType([
        T.StructField("rank", T.IntegerType(), False),
        T.StructField("candidate_itemid", T.LongType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("rec_score", T.DoubleType(), True),
        T.StructField("popularity_score", T.DoubleType(), True),
    ])), True),
])

INTENT_KEYS = ["visitorid", "itemid", "window_start", "window_end"]
WINDOW_KEYS = ["visitorid", "window_start", "window_end"]


def log_event(kind, **fields):
    print(json.dumps({"kind": kind, **fields}, ensure_ascii=False), flush=True)


def _positive_int(name, default):
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def load_config():
    """Read all runtime configuration once and validate it before Spark starts."""
    policy = os.getenv("RECOMMENDATION_POLICY", "strict_cart")
    if policy not in {"strict_cart", "extended"}:
        raise ValueError("RECOMMENDATION_POLICY must be strict_cart or extended")
    pipeline_id = os.getenv("PIPELINE_ID", "recommender-v3")
    if not pipeline_id or any(char in pipeline_id for char in r'\/:*?"<>|'):
        raise ValueError("PIPELINE_ID is empty or contains path-unsafe characters")
    return {
        "pipeline_id": pipeline_id,
        "policy": policy,
        "threshold": _positive_int("INTENT_THRESHOLD", 5),
        "top_k": _positive_int("RECOMMENDATION_TOP_K", 3),
        "window": os.getenv("STREAM_WINDOW", "10 minutes"),
        "slide": os.getenv("STREAM_SLIDE", "5 minutes"),
        "watermark": os.getenv("STREAM_WATERMARK", "5 minutes"),
        "trigger": os.getenv("STREAM_TRIGGER", "5 seconds"),
        "model_path": Path(os.getenv("MODEL_PATH", "/opt/project/models/item-cf-v1")),
        "output_root": Path(os.getenv("STREAM_OUTPUT", "/opt/project/output/recommender-v3")),
        "checkpoint_root": Path(os.getenv(
            "STREAM_CHECKPOINT_ROOT", "/opt/project/checkpoints/recommender-v3")),
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        "topic": os.getenv("KAFKA_TOPIC", "retail-rocket-events"),
        "max_offsets": _positive_int("MAX_OFFSETS_PER_TRIGGER", 1000),
        "available_now": os.getenv("STREAM_AVAILABLE_NOW") == "1",
    }


def pipeline_metadata(config, model_manifest):
    return {
        "schema_version": 1,
        "pipeline_id": config["pipeline_id"],
        "algorithm_version": "item-cf-v1",
        "model_version": model_manifest["model_version"],
        "policy": config["policy"],
        "threshold": config["threshold"],
        "top_k": config["top_k"],
        "window": config["window"],
        "slide": config["slide"],
        "watermark": config["watermark"],
    }


def ensure_pipeline_metadata(config, model_manifest):
    """Prevent a checkpoint/output namespace from silently changing semantics."""
    root = config["output_root"]
    root.mkdir(parents=True, exist_ok=True)
    expected = pipeline_metadata(config, model_manifest)
    target = root / "pipeline.json"
    if target.is_file():
        actual = json.loads(target.read_text(encoding="utf-8"))
        if actual != expected:
            raise ValueError(
                f"Pipeline metadata differs in {target}; use a new PIPELINE_ID, "
                "output root and checkpoint root."
            )
        return
    temporary = root / ".pipeline.json.tmp"
    temporary.write_text(json.dumps(expected, indent=2), encoding="utf-8")
    temporary.replace(target)


def kafka_stream(spark, config):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config["bootstrap_servers"])
            .option("subscribe", config["topic"])
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", str(config["max_offsets"]))
            .load())


def parse_events(raw):
    """Parse valid Retail Rocket payloads and keep replay/audit metadata."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_time"),
        "topic", "partition", "offset",
    ).select("data.*", "kafka_time", "topic", "partition", "offset")
    valid = parsed.where(
        (F.col("timestamp") > 0)
        & (F.col("timestamp") <= F.unix_millis(F.current_timestamp()) + 300_000)
        & (F.col("visitorid") >= 0)
        & (F.col("itemid") >= 0)
        & F.col("event").isin("view", "addtocart", "transaction")
        & ((F.col("event") != "transaction") | F.col("transactionid").isNotNull())
    )
    return valid.withColumn("event_time", F.timestamp_millis("timestamp"))


def compute_intent(events, config=None):
    """Compute visitor/item intent and the configured seed eligibility rule."""
    config = config or load_config()
    counts = (
        events.withWatermark("event_time", config["watermark"])
        .groupBy(F.window("event_time", config["window"], config["slide"]),
                 "visitorid", "itemid")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0))
             .alias("transactions"),
            F.max("event_time").alias("last_event_time"),
            F.max("kafka_time").alias("last_kafka_time"),
        )
        .withColumn("intent_score",
                    F.col("views") + F.col("carts") * 5 + F.col("transactions") * 10)
        .withColumn("recommendation_active",
                    eligible_expression(config["policy"], config["threshold"]))
        .withColumn(
            "recommendation_reason",
            F.when(F.col("recommendation_active"),
                   F.when(F.col("carts") > 0, "cart_no_purchase")
                    .otherwise("heavy_browsing")))
        .withColumn(
            "status",
            F.when(F.col("transactions") > 0, "purchased")
             .when(F.col("recommendation_active"), "recommend")
             .otherwise("browsing"))
    )
    return counts.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "visitorid", "itemid", "event_count", "views", "carts", "transactions",
        "intent_score", "recommendation_active", "recommendation_reason", "status",
        "last_event_time", "last_kafka_time",
    )


def _publication_path(output_root, batch_id):
    return output_root / "published" / f"batch_{batch_id:020d}.jsonl"


def _read_commit(path):
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as stream:
        first = stream.readline()
    record = json.loads(first)
    if record.get("kind") != "batch_commit":
        raise ValueError(f"Invalid publication: {path}")
    return record


def latest_commit(output_root, before_batch_id=None):
    published = output_root / "published"
    if not published.is_dir():
        return None
    candidates = []
    for path in published.glob("batch_*.jsonl"):
        try:
            batch_id = int(path.stem.split("_", 1)[1])
        except (IndexError, ValueError):
            continue
        if before_batch_id is None or batch_id < before_batch_id:
            candidates.append((batch_id, path))
    if not candidates:
        return None
    return _read_commit(max(candidates)[1])


def _empty(spark, schema):
    return spark.createDataFrame([], schema)


def _load_previous_snapshots(spark, output_root, batch_id):
    commit = latest_commit(output_root, before_batch_id=batch_id)
    if commit is None:
        return _empty(spark, INTENT_SCHEMA), _empty(spark, RECOMMENDATION_SCHEMA), None
    artifact = output_root / commit["artifact_path"]
    if not artifact.is_dir():
        raise FileNotFoundError(f"Published artifact is missing: {artifact}")
    intent = (spark.read.schema(INTENT_SCHEMA).parquet(str(artifact / "intent"))
              .select(*INTENT_SCHEMA.fieldNames()))
    recommendations = (spark.read.schema(RECOMMENDATION_SCHEMA)
                       .parquet(str(artifact / "recommendations"))
                       .select(*RECOMMENDATION_SCHEMA.fieldNames()))
    return intent, recommendations, commit


def _merge_intent(previous, updates, batch_id):
    updates = updates.withColumn("last_updated_batch", F.lit(batch_id).cast("long"))
    combined = previous.unionByName(updates.select(*INTENT_SCHEMA.fieldNames()))
    newest = Window.partitionBy(*INTENT_KEYS).orderBy(F.desc("last_updated_batch"))
    return (combined.withColumn("_newest", F.row_number().over(newest))
            .where(F.col("_newest") == 1).drop("_newest"))


def _merge_recommendations(previous, generated, affected_keys, batch_id):
    retained = previous.join(affected_keys, WINDOW_KEYS, "left_anti")
    generated = generated.withColumn("last_updated_batch", F.lit(batch_id).cast("long"))
    return retained.unionByName(generated.select(*RECOMMENDATION_SCHEMA.fieldNames()))


def _recommendation_updates(context, recommendations, affected_keys, config, batch_id):
    affected_context = context.join(affected_keys, WINDOW_KEYS, "inner")
    seed_summary = (affected_context.groupBy(*WINDOW_KEYS)
        .agg(F.sort_array(F.collect_set(
            F.when(F.col("recommendation_active"), F.col("itemid"))
        )).alias("eligible_seed_itemids")))
    rec_summary = (recommendations.join(affected_keys, WINDOW_KEYS, "inner")
        .groupBy(*WINDOW_KEYS)
        .agg(F.sort_array(F.collect_list(F.struct(
            "rank", "candidate_itemid", "source", "rec_score", "popularity_score"
        ))).alias("recommendations")))
    return (affected_keys.join(seed_summary, WINDOW_KEYS, "left")
        .join(rec_summary, WINDOW_KEYS, "left")
        .withColumn("eligible_seed_itemids",
                    F.coalesce("eligible_seed_itemids", F.array().cast("array<long>")))
        .withColumn("recommendations", F.coalesce(
            "recommendations",
            F.array().cast(
                "array<struct<rank:int,candidate_itemid:bigint,source:string,"
                "rec_score:double,popularity_score:double>>")))
        .withColumn(
            "generation_status",
            F.when(F.size("eligible_seed_itemids") == 0, "no_eligible_seed")
             .when(F.size("recommendations") == 0, "no_candidate")
             .otherwise("recommended"))
        .withColumn("kind", F.lit("recommendation_update"))
        .withColumn("schema_version", F.lit(1))
        .withColumn("pipeline_id", F.lit(config["pipeline_id"]))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .select("kind", "schema_version", "pipeline_id", "batch_id", *WINDOW_KEYS,
                "eligible_seed_itemids", "generation_status", "recommendations"))


def _publish_jsonl(output_root, batch_id, commit, update_json):
    published = output_root / "published"
    temporary_dir = output_root / ".tmp"
    published.mkdir(parents=True, exist_ok=True)
    temporary_dir.mkdir(parents=True, exist_ok=True)
    target = _publication_path(output_root, batch_id)
    temporary = temporary_dir / f"batch_{batch_id:020d}_{uuid.uuid4().hex}.jsonl.tmp"
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(commit, ensure_ascii=False, separators=(",", ":")) + "\n")
        for record in update_json:
            stream.write(record + "\n")
    temporary.replace(target)


def write_batch(batch, batch_id, output_root, similarity, popularity,
                config, model_manifest):
    """Materialize current state, rank changed windows and publish one batch atomically."""
    started = time.monotonic()
    output_root = Path(output_root)
    target = _publication_path(output_root, batch_id)
    batch = batch.persist()
    try:
        updated_rows = batch.count()  # Required even for an empty stateful batch.
        existing = _read_commit(target)
        if existing is not None:
            if (existing.get("pipeline_id") != config["pipeline_id"]
                    or existing.get("model_version") != model_manifest["model_version"]
                    or existing.get("policy") != config["policy"]):
                raise ValueError(f"Existing publication does not match runtime: {target}")
            log_event("sink_batch_reused", batch_id=batch_id,
                      updated_window_rows=updated_rows)
            return

        spark = batch.sparkSession
        previous_intent, previous_recommendations, predecessor = (
            _load_previous_snapshots(spark, output_root, batch_id))
        current_intent = _merge_intent(previous_intent, batch, batch_id).persist()
        affected_keys = batch.select(*WINDOW_KEYS).distinct().persist()
        affected_context = current_intent.join(affected_keys, WINDOW_KEYS, "inner")
        generated = generate_recommendations(
            affected_context, similarity, popularity,
            config["policy"], config["threshold"], config["top_k"])
        current_recommendations = _merge_recommendations(
            previous_recommendations, generated, affected_keys, batch_id).persist()

        attempt_relative = Path("artifacts") / f"batch_{batch_id:020d}" / uuid.uuid4().hex
        attempt = output_root / attempt_relative
        intent_output = current_intent.withColumn("event_date", F.to_date("window_start"))
        recommendation_output = current_recommendations.withColumn(
            "event_date", F.to_date("window_start"))
        intent_output.write.mode("errorifexists").partitionBy("event_date").parquet(
            str(attempt / "intent"))
        recommendation_output.write.mode("errorifexists").partitionBy("event_date").parquet(
            str(attempt / "recommendations"))

        intent_rows = current_intent.count()
        recommendation_rows = current_recommendations.count()
        update_json = _recommendation_updates(
            current_intent, current_recommendations, affected_keys, config, batch_id
        ).toJSON().collect()
        commit = {
            "kind": "batch_commit",
            "schema_version": 1,
            "pipeline_id": config["pipeline_id"],
            "batch_id": int(batch_id),
            "predecessor_batch_id": (
                predecessor["batch_id"] if predecessor is not None else None),
            "artifact_path": attempt_relative.as_posix(),
            "model_version": model_manifest["model_version"],
            "policy": config["policy"],
            "updated_window_rows": updated_rows,
            "intent_snapshot_rows": intent_rows,
            "recommendation_snapshot_rows": recommendation_rows,
        }
        _publish_jsonl(output_root, batch_id, commit, update_json)
        log_event(
            "sink_batch", batch_id=batch_id, updated_window_rows=updated_rows,
            affected_user_windows=len(update_json),
            recommendation_snapshot_rows=recommendation_rows,
            sink_duration_ms=round((time.monotonic() - started) * 1000, 1),
        )
        affected_keys.unpersist()
        current_recommendations.unpersist()
        current_intent.unpersist()
    finally:
        batch.unpersist()


class BatchMetrics(StreamingQueryListener):
    def __init__(self, output_root):
        super().__init__()
        self.output_root = Path(output_root)

    def onQueryStarted(self, event):
        log_event("query_started", query_id=str(event.id), run_id=str(event.runId),
                  name=event.name)

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        name = progress.get("name") or "unnamed"
        record = {
            "kind": "batch_metrics",
            "query_name": name,
            "query_id": progress["id"],
            "run_id": progress["runId"],
            "batch_id": progress["batchId"],
            "timestamp": progress["timestamp"],
            "input_records": progress["numInputRows"],
            "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
            "trigger_duration_ms": progress.get("durationMs", {}).get("triggerExecution", 0),
            "watermark": progress.get("eventTime", {}).get("watermark"),
            "state_rows": sum(s.get("numRowsTotal", 0)
                              for s in progress.get("stateOperators", [])),
            "late_rows_dropped": sum(s.get("numRowsDroppedByWatermark", 0)
                                     for s in progress.get("stateOperators", [])),
            "state_memory_bytes": sum(s.get("memoryUsedBytes", 0)
                                      for s in progress.get("stateOperators", [])),
        }
        folder = self.output_root / "metrics" / name
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"batch_{progress['batchId']:020d}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2),
                             encoding="utf-8")
        temporary.replace(target)
        print(json.dumps(record, ensure_ascii=False), flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log_event("query_terminated", query_id=str(event.id), error=event.exception)


def _with_trigger(writer, config):
    if config["available_now"]:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=config["trigger"])


def clean_writer(spark, config):
    clean = (parse_events(kafka_stream(spark, config))
             .withColumn("event_date", F.to_date("event_time"))
             .select("timestamp", "event_time", "kafka_time", "visitorid", "event",
                     "itemid", "transactionid", "topic", "partition", "offset",
                     "event_date"))
    return _with_trigger(
        clean.writeStream.queryName("retail_clean_events_v3")
        .outputMode("append").format("parquet")
        .option("path", str(config["output_root"] / "clean_events"))
        .option("checkpointLocation", str(config["checkpoint_root"] / "clean")),
        config)


def intent_writer(spark, config, similarity, popularity, model_manifest):
    intent = compute_intent(parse_events(kafka_stream(spark, config)), config)
    return _with_trigger(
        intent.writeStream.queryName("retail_intent_v3").outputMode("update")
        .option("checkpointLocation", str(config["checkpoint_root"] / "intent"))
        .foreachBatch(lambda batch, batch_id: write_batch(
            batch, batch_id, config["output_root"], similarity, popularity,
            config, model_manifest)),
        config)


def console_writer(spark, config):
    publications = config["output_root"] / "published"
    publications.mkdir(parents=True, exist_ok=True)
    updates = (spark.readStream.schema(PUBLICATION_SCHEMA).json(str(publications))
               .where(F.col("kind") == "recommendation_update")
               .select("batch_id", "visitorid", "window_start", "window_end",
                       "generation_status", "eligible_seed_itemids", "recommendations"))
    return _with_trigger(
        updates.writeStream.queryName("retail_recommendation_console_v3")
        .outputMode("append").format("console")
        .option("truncate", "false").option("numRows", "50")
        .option("checkpointLocation", str(config["checkpoint_root"] / "console")),
        config)


def main():
    config = load_config()
    spark = (SparkSession.builder.appName("RetailRocketRealtimeRecommender")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    similarity, popularity, model_manifest = load_model(spark, config["model_path"])
    ensure_pipeline_metadata(config, model_manifest)
    listener = BatchMetrics(config["output_root"])
    spark.streams.addListener(listener)
    queries = []

    def stop_queries(signum=None, frame=None):
        for query in queries:
            if query.isActive:
                query.stop()

    signal.signal(signal.SIGTERM, stop_queries)
    signal.signal(signal.SIGINT, stop_queries)
    try:
        if config["available_now"]:
            clean = clean_writer(spark, config).start()
            intent = intent_writer(
                spark, config, similarity, popularity, model_manifest).start()
            queries.extend([clean, intent])
            clean.awaitTermination()
            intent.awaitTermination()
            console = console_writer(spark, config).start()
            queries.append(console)
            console.awaitTermination()
        else:
            console = console_writer(spark, config).start()
            clean = clean_writer(spark, config).start()
            intent = intent_writer(
                spark, config, similarity, popularity, model_manifest).start()
            queries.extend([console, clean, intent])
            spark.streams.awaitAnyTermination()
    finally:
        stop_queries()
        spark.streams.removeListener(listener)
        spark.stop()


if __name__ == "__main__":
    main()
