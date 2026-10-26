"""Kafka -> validated events -> sliding intent windows -> Parquet + JSON logs."""

import json
import os
from pathlib import Path
import signal
import time

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener


EVENT_SCHEMA = T.StructType([
    T.StructField("timestamp", T.LongType()),
    T.StructField("visitorid", T.LongType()),
    T.StructField("event", T.StringType()),
    T.StructField("itemid", T.LongType()),
    T.StructField("transactionid", T.LongType()),
])


def log_event(kind, **fields):
    print(json.dumps({"kind": kind, **fields}, ensure_ascii=False), flush=True)


def parse_events(raw):
    """Drop invalid payloads before stateful aggregation; preserve Kafka time."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_time"),
    ).select("data.*", "kafka_time")
    valid = parsed.where(
        (F.col("timestamp") > 0)
        & (F.col("timestamp") <= F.unix_millis(F.current_timestamp()) + 300_000)
        & (F.col("visitorid") >= 0)
        & (F.col("itemid") >= 0)
        & F.col("event").isin("view", "addtocart", "transaction")
        & ((F.col("event") != "transaction") | F.col("transactionid").isNotNull())
    )
    return valid.withColumn("event_time", F.timestamp_millis("timestamp"))


def compute_intent(events):
    """Track each visitor/item in overlapping event-time windows with watermarking.
    
    Assignment Specification:
      - 10-minute sliding window with 5-minute watermark
      - Engagement scoring: view = 1, addtocart = 5, transaction = 10
      - Trigger recommendation when high intent is detected (addtocart without transaction)
    """
    watermark_duration = os.getenv("STREAM_WATERMARK", "5 minutes")
    window_duration = os.getenv("STREAM_WINDOW", "10 minutes")
    slide_duration = os.getenv("STREAM_SLIDE", "2 minutes")

    counts = (
        events.withWatermark("event_time", watermark_duration)
        .groupBy(F.window("event_time", window_duration, slide_duration), "visitorid", "itemid")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event") == "addtocart", 1).otherwise(0)).alias("carts"),
            F.sum(F.when(F.col("event") == "transaction", 1).otherwise(0)).alias("transactions"),
            F.max("event_time").alias("last_event_time"),
            F.max("kafka_time").alias("last_kafka_time"),
        )
    )
    return (
        counts.withColumn(
            "intent_score",
            F.col("views") * 1 + F.col("carts") * 5 + F.col("transactions") * 10
        )
        .withColumn(
            "recommendation_active",
            (F.col("carts") > 0) & (F.col("transactions") == 0) & (F.col("intent_score") >= 5)
        )
        .withColumn(
            "recommended_itemid",
            F.when(F.col("recommendation_active"), F.col("itemid"))
        )
        .withColumn(
            "status",
            F.when(F.col("transactions") > 0, "purchased")
            .when(F.col("recommendation_active"), "recommend")
            .otherwise("browsing")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "visitorid", "itemid", "event_count", "views", "carts", "transactions",
            "intent_score", "recommendation_active", "recommended_itemid", "status",
            "last_event_time", "last_kafka_time"
        )
    )


def write_batch(batch, batch_id, output_dir):
    """A replay overwrites the same batch directory instead of appending duplicates."""
    started = time.monotonic()
    batch = batch.withColumn("batch_id", F.lit(batch_id)).persist()
    try:
        rows = batch.count()  # Fully consume even an empty stateful batch.
        recommendations = batch.where("recommendation_active").count()
        if rows:
            batch.write.mode("overwrite").parquet(
                str(output_dir / "intent" / f"batch_{batch_id:020d}"))
            batch.select("batch_id", "window_start", "visitorid", "itemid",
                         "intent_score", "status", "recommended_itemid").show(20, truncate=False)
        log_event("sink_batch", batch_id=batch_id, updated_window_rows=rows,
                  active_recommendation_rows=recommendations,
                  sink_duration_ms=round((time.monotonic() - started) * 1000, 1))
    finally:
        batch.unpersist()


class BatchMetrics(StreamingQueryListener):
    """Spark progress counts source records, not overlapping aggregate rows."""

    def __init__(self, output_dir):
        super().__init__()
        self.metrics_dir = output_dir / "metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event):
        log_event("query_started", query_id=str(event.id), run_id=str(event.runId))

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        record = {
            "kind": "batch_metrics", "query_id": progress["id"],
            "run_id": progress["runId"], "batch_id": progress["batchId"],
            "timestamp": progress["timestamp"],
            "input_records": progress["numInputRows"],
            "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
            "trigger_duration_ms": progress.get("durationMs", {}).get("triggerExecution", 0),
            "watermark": progress.get("eventTime", {}).get("watermark"),
            "state_rows": sum(s.get("numRowsTotal", 0) for s in progress.get("stateOperators", [])),
            "late_rows_dropped": sum(s.get("numRowsDroppedByWatermark", 0)
                                     for s in progress.get("stateOperators", [])),
            "state_memory_bytes": sum(s.get("memoryUsedBytes", 0)
                                      for s in progress.get("stateOperators", [])),
            "sources": progress.get("sources", []),
        }
        target = self.metrics_dir / f"batch_{progress['batchId']:020d}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(target)
        print(json.dumps(record, ensure_ascii=False), flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log_event("query_terminated", query_id=str(event.id), error=event.exception)


def main():
    output_dir = Path(os.getenv("STREAM_OUTPUT", "/opt/project/output"))
    checkpoint = os.getenv("STREAM_CHECKPOINT", "/opt/project/checkpoints/retail-intent-v1")
    spark = (SparkSession.builder.appName("RetailRocketSessionIntent")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    listener = BatchMetrics(output_dir)
    spark.streams.addListener(listener)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"))
           .option("subscribe", os.getenv("KAFKA_TOPIC", "retail-rocket-events"))
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")
           .option("maxOffsetsPerTrigger", "1000").load())
    writer = (compute_intent(parse_events(raw)).writeStream
              .queryName("retail_rocket_intent").outputMode("update")
              .option("checkpointLocation", checkpoint)
              .foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, output_dir)))
    if os.getenv("STREAM_AVAILABLE_NOW") == "1":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")
    query = writer.start()

    def stop_query(signum, frame):
        query.stop()

    signal.signal(signal.SIGTERM, stop_query)
    signal.signal(signal.SIGINT, stop_query)
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
        spark.streams.removeListener(listener)
        spark.stop()


if __name__ == "__main__":
    main()
