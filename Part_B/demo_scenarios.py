"""Real Kafka/Spark scenarios shared by the UI and CLI verification.

Every completed step is supported by ACKs, committed publications or metrics.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import uuid

from demo_runtime import DemoRuntime, utc_millis
from producer import BASE, choose_demo_items


class DemoScenario:
    def __init__(self, controller, kind, isolated_recovery=False, tag=None):
        if kind not in {"Recovery", "Watermark"}:
            raise ValueError("Kịch bản không hợp lệ")
        self.hub, self.kind = controller, kind
        self.tag = tag or uuid.uuid4().hex[:10]
        if not self.tag.replace("-", "").isalnum():
            raise ValueError("Tag chỉ gồm chữ, số và dấu gạch ngang")
        self.isolated = kind == "Watermark" or isolated_recovery
        self.runtime = None
        self.container = None
        self.stopped_spark = False
        self.phase, self.status = 0, "preparing"
        self.observations = []
        self.before_query = None
        self.visitor, self.item, self.base = None, None, None
        self.baseline = -1
        self.closed = False
        self.container_started = False

    @property
    def complete(self):
        return self.status == "complete"

    def observe(self, label, **fields):
        self.observations.append({"label": label, **fields})
        self.hub.log(f"{self.kind}: {label} · {fields}")
        path = self.runtime.output_root / "demo" / f"scenario-{self.tag}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.snapshot(), ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    def prepare(self):
        live = self.hub.live
        if self.isolated:
            name = f"retail-ui-{self.kind.lower()}-{self.tag}"
            relative = Path("output/verification-category") / f"{self.kind.lower()}-{self.tag}"
            root = BASE / relative
            if root.exists():
                raise ValueError("Namespace đã tồn tại; tạo kịch bản mới để giữ nguyên bằng chứng cũ.")
            root.mkdir(parents=True)
            self.runtime = DemoRuntime(live.catalog_path, root, live.csv_path,
                                       live.bootstrap_servers, name)
            self.container = name
            self.runtime._ensure_resources()
            self.hub.active = self.runtime
            from kafka.admin import KafkaAdminClient, NewTopic
            client = KafkaAdminClient(bootstrap_servers=live.bootstrap_servers,
                                      request_timeout_ms=10000, bootstrap_timeout_ms=5000)
            try:
                client.create_topics([NewTopic(name, 2, 1)])
            finally:
                client.close()
            self.hub.log("Đang khởi động Spark kiểm chứng riêng; demo chính được giữ nguyên.")
            self.hub.compose(["run", "--no-deps", "-d", "--name", name,
                "-e", f"KAFKA_TOPIC={name}", "-e", f"PIPELINE_ID={name}",
                "-e", f"STREAM_OUTPUT=/opt/project/{relative.as_posix()}",
                "-e", f"STREAM_CHECKPOINT_ROOT=/opt/project/checkpoints/ui-scenarios/{name}", "spark"])
            self.container_started = True
        else:
            self.runtime = live
            live.new_session("Từng bước")
            self.container = self.hub.container_id()
            self.hub.active = live
        self.runtime._ensure_resources()
        self.item, _ = choose_demo_items(self.runtime.groups, live.csv_path)
        if self.isolated:
            self.runtime.session_id = time.time_ns() // 1000
            self.runtime.session_state = "scenario"
        self.visitor = self.runtime.session_id
        self.runtime.visible_visitors = {self.visitor}
        self.base = int(time.time()) // 300 * 300_000 - 30 * 60_000
        if self.kind == "Recovery":
            self.hub.log("Gửi bốn view; chờ Spark commit score 4 trước khi dừng Spark…")
            for index in range(4):
                if self.isolated:
                    self.emit(self.visitor, "view", self.base + 60_000 + index)
                else:
                    self.runtime.step(require_commit=False)
            self.wait_intent(4)
            self.wait_recommendation(False)
            self.before_query = self.query_identity()
            self.phase = 1
            self.status = "ready"
            self.observe("Bốn view đã commit; chưa recommend", score=4, visitor=self.visitor,
                         item=self.item, query_id=self.before_query["query_id"],
                         run_id=self.before_query["run_id"])
        else:
            self.status = "ready"
            self.observe("Môi trường Watermark riêng đã được tạo", topic=self.runtime.topic)

    def emit(self, visitor, action, timestamp):
        return self.runtime.emit(visitor, self.item, action,
                                 transactionid=visitor if action == "transaction" else None,
                                 timestamp_ms=timestamp)

    def refresh(self):
        self.runtime.reader.refresh()
        if self.runtime.reader.error:
            raise RuntimeError(self.runtime.reader.error)

    def intent(self, score, visitor=None):
        self.refresh()
        return next((row for row in self.runtime.reader.intents.values()
                     if row["visitorid"] == (visitor or self.visitor) and row["itemid"] == self.item
                     and row["intent_score"] == score), None)

    def wait_intent(self, score, visitor=None):
        return self.hub.wait_for(lambda: self.intent(score, visitor), f"Spark commit score {score}")

    def recommendation(self, active, after=-1):
        self.refresh()
        return next((row for row in self.runtime.reader.recommendations.values()
                     if row["visitorid"] == self.visitor and row["batch_id"] > after
                     and bool(row["recommendations"]) == active), None)

    def wait_recommendation(self, active, after=-1):
        return self.hub.wait_for(lambda: self.recommendation(active, after), "recommendation/clear được commit")

    def query_identity(self):
        path = self.runtime.output_root / "queries/retail_intent_v3.json"
        def read():
            if path.is_file():
                value = json.loads(path.read_text())
                if value.get("status") == "running":
                    return value
            return None
        return self.hub.wait_for(read, "query intent đang chạy")

    def metrics(self):
        return [row for row in self.runtime.reader.metrics() if row["query_name"] == "retail_intent_v3"]

    def advance(self):
        if self.complete or self.status != "ready":
            raise ValueError("Kịch bản đã xong hoặc đang lỗi; bấm Trở về demo chính.")
        self.hub.check_cancel()
        return self._recovery() if self.kind == "Recovery" else self._watermark()

    def _recovery(self):
        if self.phase == 1:
            # Mark ownership before stop: cancellation after this command must
            # always restore the container, even if no next UI step is pressed.
            self.stopped_spark = True
            self.hub.stop_spark(self.container)
            state = json.loads(self.hub.command(["inspect", self.container]))[0]["State"]
            assert not state["Running"], "Spark chưa dừng"
            self.phase = 2
            self.observe("Spark đã dừng; Kafka vẫn tiếp nhận event", score=4)
        elif self.phase == 2:
            if self.isolated:
                _, ack = self.emit(self.visitor, "view", self.base + 60_004)
            else:
                self.runtime.step()
                ack = self.runtime.interactions[-1]
            assert self.intent(4) and not self.intent(5)
            self.phase = 3
            self.observe("View thứ năm đã được Kafka ACK khi Spark dừng", score_before=4,
                         partition=ack["partition"], offset=ack["offset"])
        elif self.phase == 3:
            self.hub.start_spark(self.container)
            self.stopped_spark = False
            five = self.wait_intent(5)
            rec = self.wait_recommendation(True)
            after = self.query_identity()
            assert self.before_query["query_id"] == after["query_id"], "Query ID không khớp checkpoint cũ"
            assert self.before_query["run_id"] != after["run_id"], "Run ID chưa thay đổi"
            assert five["carts"] == 0 and five["recommendation_reason"] == "heavy_browsing"
            assert len(rec["recommendations"]) == 3
            assert self.item not in [row["candidate_itemid"] for row in rec["recommendations"]]
            self.phase = 4
            self.observe("Khôi phục score 4 → 5, carts = 0 và top-K", score_before=4, score_after=5,
                         query_id=after["query_id"], previous_run_id=self.before_query["run_id"],
                         run_id=after["run_id"], recommendations=rec["recommendations"])
        elif self.phase == 4:
            previous = self.wait_recommendation(True)
            if self.isolated:
                self.emit(self.visitor, "transaction", self.base + 60_005)
            else:
                self.runtime.step()
            self.wait_intent(15)
            self.wait_recommendation(False, after=previous["batch_id"])
            self.phase, self.status = 5, "complete"
            self.runtime.running, self.runtime.session_state = False, "complete"
            self.observe("Transaction: score 15 và thu hồi top-K", score=15, recommendations=[])
            self.save_result()
        return "Bước recovery đã được xác nhận bằng kết quả thực."

    def _watermark(self):
        if self.phase == 0:
            self.emit(self.visitor, "view", self.base + 60_000)
            self.emit(self.visitor + 1, "view", self.base + 180_000)
            self.wait_intent(1)
            self.wait_intent(1, self.visitor + 1)
            self.phase = 1
            self.observe("Event ban đầu và event tiến đồng hồ đã commit", score=1,
                         event_time=self.base + 60_000, max_event_time=self.base + 180_000)
        elif self.phase == 1:
            self.emit(self.visitor, "view", self.base + 90_000)
            self.wait_intent(2)
            self.phase = 2
            self.observe("Event đến sai thứ tự vẫn được tính", score_before=1, score_after=2,
                         event_time=self.base + 90_000, max_event_time=self.base + 180_000)
        elif self.phase == 2:
            self.emit(self.visitor + 2, "view", self.base + 18 * 60_000)
            metric = self.hub.wait_for(lambda: next((row for row in self.metrics()
                if row.get("watermark") and utc_millis(row["watermark"]) >= self.base + 13 * 60_000), None),
                "watermark vượt thời điểm kết thúc cả hai window cũ")
            def removed():
                paths = (self.runtime.output_root / "metrics/retail_intent_v3").glob("batch_*.json")
                return sum(json.loads(path.read_text()).get("state_rows_removed", 0) for path in paths)
            removed_rows = self.hub.wait_for(removed, "Spark dọn state đã hết hạn")
            self.baseline = metric["batch_id"]
            self.phase = 3
            self.observe("Watermark tiến và state cũ được dọn", watermark=metric["watermark"],
                         state_rows_removed=removed_rows, old_window_end=self.base + 600_000)
        elif self.phase == 3:
            # Refresh the baseline immediately before the send, not from an
            # earlier UI phase which may be several batches behind.
            self.baseline = max(row["batch_id"] for row in self.metrics())
            self.emit(self.visitor, "view", self.base + 60_000)
            metric = self.hub.wait_for(lambda: next((row for row in self.metrics()
                if row["batch_id"] > self.baseline and row["late_rows_dropped"] > 0), None),
                "late event bị loại khỏi các window đã hết hạn")
            assert self.intent(2) and not self.intent(3)
            self.phase, self.status = 4, "complete"
            self.observe("Event quá trễ bị loại; score giữ nguyên 2", watermark=metric["watermark"],
                         late_rows=metric["late_rows_dropped"], score_after=2)
            self.save_result()
        return "Bước watermark đã được xác nhận bằng metrics/publication thực."

    def save_result(self):
        result = {"result": "PASS", **self.snapshot(),
                  "source_sha256": {f"Part_B/{name}": hashlib.sha256((BASE / name).read_bytes()).hexdigest()
                      for name in ("demo_controller.py", "demo_scenarios.py", "demo_runtime.py",
                                   "gradio_ui.py", "producer.py", "streaming_etl.py", "recommendation.py")}}
        (self.runtime.output_root / "demo" / f"result-{self.tag}.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    def rollback(self):
        if self.stopped_spark:
            self.hub.start_spark(self.container)
            self.stopped_spark = False
            self.hub.log("Đã mở lại Spark bị dừng bởi recovery; giữ nguyên checkpoint.")
        self.status = "interrupted"

    def close(self):
        if self.closed:
            return
        self.rollback() if self.stopped_spark else None
        if self.isolated and self.container_started:
            self.hub.stop_spark(self.container)
        if self.isolated and self.runtime:
            self.runtime.close()
        self.closed = True

    def snapshot(self):
        steps = (["Dừng Spark", "Gửi view thứ năm khi Spark dừng", "Khởi động lại và đối chiếu 4 → 5",
                  "Gửi transaction và kiểm tra thu hồi"] if self.kind == "Recovery" else
                 ["Gửi event ban đầu", "Gửi event đến trễ", "Tiến event-time và dọn state", "Gửi event quá trễ"])
        index = self.phase - 1 if self.kind == "Recovery" else self.phase
        return {"kind": self.kind, "tag": self.tag, "phase": self.phase, "status": self.status,
                "next_step": "Đã hoàn tất" if self.complete else steps[min(max(index, 0), 3)],
                "visitorid": self.visitor, "itemid": self.item, "container": self.container,
                "topic": self.runtime.topic if self.runtime else None,
                "output": str(self.runtime.output_root) if self.runtime else None,
                "observations": list(self.observations)}
