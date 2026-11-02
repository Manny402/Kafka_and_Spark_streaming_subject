"""Shared Kafka producer for CSV replay, automatic and step-by-step demos."""
import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
import threading
import time

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer

BASE = Path(__file__).resolve().parent
CSV_PATH = (BASE / "retailrocket/events.csv" if (BASE / "retailrocket/events.csv").is_file()
            else BASE.parent / "retailrocket/events.csv")
TOPIC = "retail-rocket-events"


def parse_event(row):
    transaction_id = str(row.get("transactionid") or "").strip()
    return {"timestamp": int(row["timestamp"]), "visitorid": int(row["visitorid"]),
            "event": row["event"], "itemid": int(row["itemid"]),
            "transactionid": int(transaction_id) if transaction_id else None}


def build_live_event(visitorid, itemid, event, transactionid=None, timestamp_ms=None):
    if event not in {"view", "addtocart", "transaction"}:
        raise ValueError("Invalid event type")
    if int(visitorid) < 0 or int(itemid) < 0:
        raise ValueError("Visitor/item must be non-negative")
    if event == "transaction" and transactionid is None:
        raise ValueError("A transaction requires transactionid")
    timestamp_ms = time.time_ns() // 1_000_000 if timestamp_ms is None else int(timestamp_ms)
    if timestamp_ms <= 0:
        raise ValueError("Timestamp must be positive")
    return {"timestamp": timestamp_ms, "visitorid": int(visitorid), "event": event,
            "itemid": int(itemid),
            "transactionid": int(transactionid) if event == "transaction" else None}


def create_producer(bootstrap_servers="127.0.0.1:9092"):
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                         key_serializer=DefaultSerializer(), value_serializer=JsonSerializer(),
                         acks="all", retries=3, max_block_ms=10_000, request_timeout_ms=10_000)


def send_event(producer, topic, event):
    # Resolve the future before publishing anything to the desktop interaction log.
    metadata = producer.send(topic, key=str(event["visitorid"]), value=event).get(timeout=15)
    return {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset,
            "acknowledged_at": datetime.now(timezone.utc).isoformat()}


def _session_events(first_item, second_item, session_id):
    for _ in range(5):
        yield {"visitorid": session_id, "itemid": first_item, "event": "view"}
    yield {"visitorid": session_id, "itemid": first_item, "event": "transaction",
           "transactionid": session_id}
    for event in ("view", "addtocart", "transaction"):
        yield {"visitorid": session_id + 1, "itemid": second_item, "event": event,
               "transactionid": session_id + 1 if event == "transaction" else None}


def read_events(csv_path, demo=False):
    with csv_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if not demo:
            for row in reader:
                yield parse_event(row)
            return
        first = parse_event(next(reader))
        second = next((parse_event(row) for row in reader if int(row["itemid"]) != first["itemid"]), first)
        for template in _session_events(first["itemid"], second["itemid"], time.time_ns() // 1000):
            yield build_live_event(**template)


def choose_demo_items(groups, csv_path=None):
    eligible = {item for items in groups.values() if len(items) >= 4 for item in items}
    selected = []
    if csv_path is not None:
        with Path(csv_path).open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                item = int(row["itemid"])
                if item in eligible and item not in selected:
                    selected.append(item)
                    if len(selected) == 2:
                        break
    else:
        selected = sorted(eligible)[:2]
    if not selected:
        raise ValueError("No demo item with at least three same-category candidates")
    return selected[0], selected[-1]


def iter_demo_interactions(groups, demo_session_id, csv_path=None, repeat=True):
    first, second = choose_demo_items(groups, csv_path)
    cycle = 0
    while True:
        yield from _session_events(first, second, demo_session_id + cycle * 2)
        if not repeat:
            return
        cycle += 1


def next_deadline(previous, now, interval):
    """Skip missed ticks instead of emitting a catch-up burst."""
    deadline = previous + interval
    if deadline <= now:
        deadline += (int((now - deadline) // interval) + 1) * interval
    return deadline


def run_live_demo(producer, templates, topic=TOPIC, interval=5.0, stop_event=None,
                  on_sent=None, on_error=None):
    if interval <= 0:
        raise ValueError("Demo interval must be positive")
    stop_event = stop_event or threading.Event()
    deadline, sent = time.monotonic() + interval, 0
    try:
        while not stop_event.wait(max(0, deadline - time.monotonic())):
            try:
                template = next(templates)
            except StopIteration:
                return sent
            event = build_live_event(**template)
            ack = send_event(producer, topic, event)
            sent += 1
            if on_sent:
                on_sent(event, ack)
            deadline = next_deadline(deadline, time.monotonic(), interval)
    except Exception as error:
        if on_error:
            on_error(error)
        else:
            raise
    return sent


def replay_events(producer, csv_path, topic=TOPIC, interval=0.1, limit=0,
                  replay_clock=False, demo=False, stop_event=None, on_sent=None):
    if interval < 0 or limit < 0:
        raise ValueError("Interval and limit must be non-negative")
    stop_event = stop_event or threading.Event()
    sent = 0
    for event in read_events(csv_path, demo=demo):
        if stop_event.is_set():
            break
        if replay_clock or demo:
            event = {**event, "timestamp": time.time_ns() // 1_000_000}
        ack = send_event(producer, topic, event)
        sent += 1
        if on_sent:
            on_sent(event, ack)
        if (limit and sent >= limit) or stop_event.wait(interval):
            break
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=CSV_PATH)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--replay-clock", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--catalog", type=Path, default=BASE / "catalog/category-v1")
    args = parser.parse_args()
    interval = args.interval if args.interval is not None else (5.0 if args.demo else 0.1)
    if interval < 0 or args.limit < 0 or (args.demo and interval == 0):
        parser.error("Invalid interval/limit")

    def report(event, ack):
        print(f"visitor={event['visitorid']} item={event['itemid']} event={event['event']} "
              f"partition={ack['partition']} offset={ack['offset']}", flush=True)

    producer = create_producer(args.bootstrap_servers)
    try:
        if args.demo:
            from category_catalog import load_ui_catalog
            groups, _ = load_ui_catalog(args.catalog)
            templates = iter_demo_interactions(groups, time.time_ns() // 1000, args.csv, repeat=False)
            run_live_demo(producer, templates, args.topic, interval, on_sent=report)
        else:
            replay_events(producer, args.csv, args.topic, interval, args.limit,
                          args.replay_clock, on_sent=report)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
