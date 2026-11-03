"""Local demo lifecycle and incremental publication reader; no Spark dependency."""
from collections import Counter, deque
from datetime import datetime, timezone
from itertools import islice
import json
from pathlib import Path
import threading
import time

from category_catalog import load_ui_catalog
from producer import (BASE, CSV_PATH, TOPIC, build_live_event, create_producer,
                      iter_demo_interactions, read_events, run_live_demo, send_event)


def utc_millis(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def window_key(row):
    return (int(row["visitorid"]), utc_millis(row["window_start"]), utc_millis(row["window_end"]))


class PublicationReader:
    def __init__(self, output_root, catalog_version=None):
        self.output_root = Path(output_root)
        self.catalog_version = catalog_version
        self.pipeline = None
        self.last_batch = -1
        self.intents, self.recommendations = {}, {}
        self.commit = None
        self.error = None

    def refresh(self):
        try:
            metadata_path = self.output_root / "pipeline.json"
            if not metadata_path.is_file():
                return
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("schema_version") != 2:
                raise ValueError("Publication schema must be v2")
            if self.catalog_version and metadata["catalog_version"] != self.catalog_version:
                raise ValueError("UI catalog differs from the running Spark catalog")
            if self.pipeline is not None and metadata != self.pipeline:
                raise ValueError("Pipeline metadata changed; restart UI with matching output/catalog")
            self.pipeline = metadata
            for path in sorted((self.output_root / "published").glob("batch_*.jsonl")):
                batch_id = int(path.stem.split("_", 1)[1])
                if batch_id <= self.last_batch:
                    continue
                rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                if not rows or rows[0].get("kind") != "batch_commit":
                    raise ValueError(f"Missing batch commit: {path.name}")
                commit = rows[0]
                if (commit.get("schema_version") != 2 or commit.get("batch_id") != batch_id
                        or commit.get("pipeline_id") != metadata["pipeline_id"]
                        or commit.get("catalog_version") != metadata["catalog_version"]
                        or commit.get("policy") != metadata["policy"]):
                    raise ValueError(f"Publication metadata mismatch: {path.name}")
                artifact = (self.output_root / commit["artifact_path"]).resolve()
                if not artifact.is_relative_to(self.output_root.resolve()) or not artifact.is_dir():
                    raise ValueError("Publication artifact is missing or outside output root")
                # Validate a complete publication before mutating the visible cache.
                for row in rows[1:]:
                    if (row.get("schema_version") != 2 or row.get("batch_id") != batch_id
                            or row.get("pipeline_id") != metadata["pipeline_id"]):
                        raise ValueError("Invalid update metadata")
                    window_key(row)
                    if row["kind"] not in {"intent_update", "recommendation_update"}:
                        raise ValueError("Unknown publication record")
                for row in rows[1:]:
                    key = window_key(row)
                    if row["kind"] == "intent_update":
                        self.intents[(*key, int(row["itemid"]))] = row
                    else:
                        self.recommendations[key] = row  # [] explicitly clears prior top-K.
                self.last_batch, self.commit = batch_id, commit
            # Keep bounded UI history; Parquet/publications remain the durable audit.
            for cache in (self.intents, self.recommendations):
                if len(cache) > 10_000:
                    for key in sorted(cache, key=lambda key: cache[key]["batch_id"])[:-10_000]:
                        del cache[key]
            self.error = None
        except (OSError, ValueError, KeyError, TypeError) as error:
            self.error = str(error)

    def metrics(self):
        result = []
        for folder in sorted((self.output_root / "metrics").glob("*")):
            paths = sorted(folder.glob("batch_*.json"))
            if paths:
                try:
                    result.append(json.loads(paths[-1].read_text(encoding="utf-8")))
                except (OSError, ValueError):
                    pass
        return result

    def queries(self):
        result = []
        for path in sorted((self.output_root / "queries").glob("*.json")):
            try:
                result.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                pass
        return result


class DemoRuntime:
    def __init__(self, catalog_path=None, output_root=None, csv_path=CSV_PATH,
                 bootstrap_servers="127.0.0.1:9092", topic=TOPIC, producer_factory=None):
        self.catalog_path = Path(catalog_path or BASE / "catalog/category-v1")
        self.output_root = Path(output_root or BASE / "output/category-realtime-v1")
        self.csv_path, self.bootstrap_servers, self.topic = csv_path, bootstrap_servers, topic
        self.producer_factory = producer_factory or create_producer
        self.producer = None
        self.lock = threading.RLock()
        self.worker = None
        self.stop_event = threading.Event()
        self.interactions = deque(maxlen=200)
        self.sent = 0
        self.mode, self.running, self.error = "Tự động", False, None
        self.templates = None
        self.pending = None
        self.expected = Counter()
        self.anchors = {}
        self.reader = PublicationReader(self.output_root)
        self.groups, self.item_categories = {}, {}
        self.session_id = None
        self.session_state = "idle"
        self.interval = 5.0
        self.visible_visitors = set()
        self.logs = deque(maxlen=120)
        self._load_tail()

    def log(self, message):
        self.logs.append({"time": datetime.now(timezone.utc).isoformat(), "message": str(message)})

    def _load_tail(self):
        path = self.output_root / "demo/interactions.jsonl"
        if path.is_file():
            # Streaming tail avoids loading a long previous demo into memory.
            with path.open(encoding="utf-8") as stream:
                for line in deque(stream, maxlen=200):
                    try:
                        self.interactions.append(json.loads(line))
                    except ValueError:
                        pass

    def _ensure_resources(self):
        if not self.groups:
            self.groups, manifest = load_ui_catalog(self.catalog_path)
            self.item_categories = {item: category for category, items in self.groups.items()
                                    for item in items}
            self.reader.catalog_version = manifest["catalog_version"]
        if self.producer is None:
            self.producer = self.producer_factory(self.bootstrap_servers)

    def _on_sent(self, event, ack):
        with self.lock:
            row = {**event, **ack, "categoryid": self.item_categories.get(event["itemid"])}
            path = self.output_root / "demo/interactions.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            self.interactions.append(row)
            self.visible_visitors.add(event["visitorid"])
            self.sent += 1
            self.log(f"Kafka ACK · visitor {event['visitorid']} · {event['event']} · "
                     f"partition {ack['partition']} offset {ack['offset']}")
            key = (event["visitorid"], event["itemid"])
            self.expected[key] += 1
            self.anchors.setdefault(key, event["timestamp"] // 300_000 * 300_000)
            self.pending = {**event, "expected_count": self.expected[key],
                            "window_start_ms": self.anchors[key]}

    def _on_error(self, error):
        with self.lock:
            self.error, self.running = str(error), False
            self.session_state = "error"
            self.log(f"Lỗi producer: {error}")
            self.stop_event.set()

    def _run(self):
        try:
            run_live_demo(self.producer, self.templates, self.topic, self.interval,
                          self.stop_event, self._on_sent, self._on_error)
        finally:
            with self.lock:
                self.running = False
                if self.session_state == "running" and not self.stop_event.is_set():
                    self.session_state = "complete"
                    self.log("Đã phát hết dữ liệu CSV.")

    def start(self, mode="Tự động"):
        if mode not in {"Tự động", "Từng bước"}:
            raise ValueError("Unknown demo mode")
        with self.lock:
            if self.running or (self.worker and self.worker.is_alive()):
                return "Demo đang chạy; không tạo thêm worker."
            self._ensure_resources()
            self.session_id = time.time_ns() // 1000
            self.templates = iter_demo_interactions(self.groups, self.session_id, self.csv_path)
            self.pending = None
            self.expected.clear()
            self.anchors.clear()
            self.interactions.clear()
            self.visible_visitors = {self.session_id, self.session_id + 1}
            self.sent, self.interval, self.session_state = 0, 5.0, "running"
            self.mode, self.running, self.error = mode, True, None
            self.stop_event.clear()
            if mode == "Tự động":
                self.worker = threading.Thread(target=self._run, name="demo-producer", daemon=True)
                self.worker.start()
            return f"Đã bắt đầu {mode}; visitor U1 = {self.session_id}."

    def _halt_worker(self):
        self.stop_event.set()
        worker = self.worker
        if worker and worker.is_alive():
            worker.join(timeout=20)
        if worker and worker.is_alive():
            raise RuntimeError("Lần gửi còn đang chờ ACK; chưa thể đổi phiên.")

    def pause(self):
        with self.lock:
            if self.session_state != "running":
                return "Phiên không đang phát."
            self.session_state = "paused"
        self._halt_worker()
        with self.lock:
            self.running = False
            self.log("Tạm dừng; giữ visitor và vị trí kịch bản.")
            return "Đã tạm dừng. Bấm Tiếp tục để giữ nguyên phiên."

    def resume(self):
        with self.lock:
            if self.session_state == "running":
                return "Phiên đang chạy; không tạo thêm worker."
            if self.session_state != "paused" or self.templates is None:
                raise ValueError("Không có phiên tạm dừng để tiếp tục.")
            self.running, self.session_state = True, "running"
            self.stop_event.clear()
            if self.mode != "Từng bước":
                self.worker = threading.Thread(target=self._run, name="demo-producer", daemon=True)
                self.worker.start()
            self.log("Tiếp tục cùng visitor và vị trí kịch bản.")
            return "Đã tiếp tục phiên cũ."

    def finish(self):
        with self.lock:
            self.session_state = "ended"
        self._halt_worker()
        with self.lock:
            self.running = False
            self.templates = None
            self.log("Đã kết thúc phiên; giữ dữ liệu và checkpoint.")
            return "Đã kết thúc phiên. Phiên mới sẽ tạo visitor mới."

    def stop(self):
        return self.finish()

    def new_session(self, mode="Từng bước"):
        self.finish()
        return self.start(mode)

    def start_csv(self, limit=20, interval=0.1):
        if not 1 <= int(limit) <= 10_000 or not 0.05 <= float(interval) <= 60:
            raise ValueError("CSV: số bản ghi 1–10000, interval 0,05–60 giây.")
        self.finish()
        with self.lock:
            self._ensure_resources()
            self.session_id = time.time_ns() // 1000
            self.mode, self.interval = "CSV", float(interval)
            rows = islice(read_events(Path(self.csv_path)), int(limit))
            self.templates = ({key: row[key] for key in
                               ("visitorid", "itemid", "event", "transactionid")} for row in rows)
            self.pending = None
            self.expected.clear()
            self.anchors.clear()
            self.visible_visitors.clear()
            self.interactions.clear()
            self.sent, self.error = 0, None
            self.running, self.session_state = True, "running"
            self.stop_event.clear()
            self.worker = threading.Thread(target=self._run, name="csv-producer", daemon=True)
            self.worker.start()
            self.log(f"Phát {limit} dòng CSV, interval {interval} giây; timestamp thời gian phát.")
            return f"Đang phát tối đa {limit} interaction từ events.csv."

    def emit(self, visitorid, itemid, event, transactionid=None, timestamp_ms=None):
        self._ensure_resources()
        payload = build_live_event(visitorid, itemid, event, transactionid, timestamp_ms)
        ack = send_event(self.producer, self.topic, payload)
        self._on_sent(payload, ack)
        return payload, ack

    def pending_processed(self):
        if self.pending is None:
            return True
        target = self.pending
        rows = [row for key, row in self.reader.intents.items()
                if key[0] == target["visitorid"] and key[1] == target["window_start_ms"]
                and key[3] == target["itemid"]]
        return any(row["event_count"] >= target["expected_count"]
                   and utc_millis(row["last_event_time"]) >= target["timestamp"] for row in rows)

    def step(self, require_commit=True):
        with self.lock:
            if not self.running or self.mode != "Từng bước":
                raise ValueError("Bắt đầu demo ở chế độ Từng bước trước.")
            self.reader.refresh()
            if self.reader.error:
                raise ValueError(self.reader.error)
            if require_commit and not self.pending_processed():
                raise ValueError("Đang chờ Spark commit kết quả bước trước; chưa gửi thêm event.")
            template = next(self.templates)
            key = (template["visitorid"], template["itemid"])
            if key in self.anchors and time.time_ns() // 1_000_000 >= self.anchors[key] + 600_000:
                self.running = False
                raise ValueError("Window của phiên đã hết; bấm Bắt đầu để tạo phiên mới.")
            event = build_live_event(**template)
            try:
                ack = send_event(self.producer, self.topic, event)
            except Exception as error:
                self._on_error(error)
                raise
            self._on_sent(event, ack)
            return f"Kafka ACK: visitor {event['visitorid']}, {event['event']}; đang chờ Spark."

    def snapshot(self, show_history=False):
        with self.lock:
            self.reader.refresh()
            include = lambda row: show_history or self.session_id is None or row["visitorid"] in self.visible_visitors
            return {"running": self.running, "mode": self.mode, "sent": self.sent,
                    "session_state": self.session_state, "anchors": dict(self.anchors),
                    "pending": self.pending, "logs": list(self.logs),
                    "error": self.error or self.reader.error,
                    "processed": self.pending_processed(), "session_id": self.session_id,
                    "interactions": list(self.interactions),
                    "intents": [row for row in self.reader.intents.values() if include(row)],
                    "recommendations": [row for row in self.reader.recommendations.values() if include(row)],
                    "pipeline": self.reader.pipeline, "commit": self.reader.commit,
                    "metrics": self.reader.metrics(), "queries": self.reader.queries()}

    def close(self):
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=20)
        if self.producer:
            self.producer.close(timeout=5)
