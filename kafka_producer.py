"""Publish Retail Rocket CSV events to the local Kafka topic.

The visitor id is the Kafka key, preserving each visitor's event order in one
partition.  Delivery futures are awaited so failed sends cannot be silent.
"""
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/events.csv")
    parser.add_argument("--topic", default="retail-rocket-events")
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--hours", type=int, default=24, help="publish the final N event-time hours")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--sleep-seconds", type=float, default=0)
    args = parser.parse_args()

    events = pd.read_csv(args.input).sort_values("timestamp")
    cutoff = events.timestamp.max() - args.hours * 3_600_000
    events = events.loc[events.timestamp >= cutoff]
    producer = make_producer(args.bootstrap)
    try:
        total = 0
        for start in range(0, len(events), args.batch_size):
            sent = publish_df(producer, args.topic, events.iloc[start:start + args.batch_size])
            total += sent
            print(f"published {total:,}/{len(events):,}")
            if args.sleep_seconds:
                time.sleep(args.sleep_seconds)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
