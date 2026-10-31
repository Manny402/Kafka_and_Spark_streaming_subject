"""Replay Retail Rocket CSV rows to Kafka, or run a short mocked session demo."""

import argparse
import csv
from pathlib import Path
import time

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer


_PARENT = Path(__file__).resolve().parent

if (_PARENT / "retailrocket" / "events.csv").is_file():
    CSV_PATH = _PARENT / "retailrocket" / "events.csv"
else:
    CSV_PATH = _PARENT.parent / "retailrocket" / "events.csv"

TOPIC = "retail-rocket-events"


def parse_event(row):
    transaction_id = row["transactionid"].strip()
    return {
        "timestamp": int(row["timestamp"]),
        "visitorid": int(row["visitorid"]),
        "event": row["event"],
        "itemid": int(row["itemid"]),
        "transactionid": int(transaction_id) if transaction_id else None,
    }


def read_events(csv_path, demo=False):
    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if not demo:
            for row in reader:
                yield parse_event(row)
            return
        # Mock two sessions using item IDs from the dataset.
        first = parse_event(next(reader))
        second = next(parse_event(row) for row in reader
                      if int(row["visitorid"]) != first["visitorid"])
        session_seed = time.time_ns() // 1_000_000
        first["visitorid"] = session_seed * 2
        second["visitorid"] = session_seed * 2 + 1
        for base, event_type in (
            (first, "view"), (first, "addtocart"),
            (second, "view"), (second, "addtocart"),
            (first, "transaction"),
        ):
            yield {**base, "event": event_type,
                   "transactionid": 1 if event_type == "transaction" else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=CSV_PATH)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--interval", type=float, default=0.1,
                        help="Seconds between events (default: 0.1).")
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum events; 0 replays the entire CSV.")
    parser.add_argument("--replay-clock", action="store_true",
                        help="Replace historical event time with current replay time.")
    parser.add_argument("--demo", action="store_true",
                        help="Mock five events from CSV items, with fresh visitors and current time.")
    args = parser.parse_args()
    if args.interval < 0 or args.limit < 0:
        parser.error("--interval and --limit must be non-negative")
    if not args.csv.is_file():
        parser.error(f"CSV not found: {args.csv}")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
        acks="all",
        retries=3,
    )
    try:
        for sent, event in enumerate(read_events(args.csv, args.demo), start=1):
            if args.replay_clock or args.demo:
                event["timestamp"] = time.time_ns() // 1_000_000
            metadata = producer.send(
                args.topic, key=str(event["visitorid"]), value=event,
            ).get(timeout=10)
            print(f"Sent={sent} visitor={event['visitorid']} event={event['event']} "
                  f"partition={metadata.partition} offset={metadata.offset}", flush=True)
            if args.limit and sent >= args.limit:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
