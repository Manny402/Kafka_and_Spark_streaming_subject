"""Local control panel: fixed Docker actions and one background operation.

No user-supplied shell command is accepted. UI refresh reads cached health and
never waits for Docker, connector downloads, or a Spark micro-batch.
"""
from collections import deque
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import threading
import time
import uuid

from category_catalog import catalog_location
from demo_runtime import DemoRuntime
from producer import BASE


class DemoController:
    def __init__(self, runtime=None, runner=None, monitor=True):
        self.live = runtime or DemoRuntime()
        self.active = self.live
        self.runner = runner or subprocess.run
        self.lock = threading.RLock()
        self.job = None
        self.job_name = None
        self.job_state = "idle"
        self.job_error = None
        self.cancel_event = threading.Event()
        self.shutdown = threading.Event()
        self.logs = deque(maxlen=160)
        self.health = {"docker": False, "ready": False, "services": {}, "message": "Đang kiểm tra hệ thống…"}
        self.scenario = None
        self.topic_info = None
        self.console_log = "Bấm Đọc log Spark để xem log thực từ container."
        self.control_root = self.live.output_root.parent / "control-panel"
        self.monitor = None
        if monitor:
            self.monitor = threading.Thread(target=self._monitor, name="demo-health", daemon=True)
            self.monitor.start()

    def log(self, message):
        row = {"time": datetime.now(timezone.utc).isoformat(), "message": str(message)}
        with self.lock:
            self.logs.append(row)
            self.control_root.mkdir(parents=True, exist_ok=True)
            with (self.control_root / "actions.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")

    def command(self, arguments, timeout=120):
        # Callers construct these lists inside this module/scenario module.
        result = self.runner(["docker", *arguments], cwd=BASE, capture_output=True,
                             encoding="utf-8", errors="replace", timeout=timeout,
                             **({"creationflags": subprocess.CREATE_NO_WINDOW} if __import__("os").name == "nt" else {}))
        if result.returncode:
            raise RuntimeError((result.stderr or result.stdout or "Docker command failed")[-3500:])
        return result.stdout.strip()

    def compose(self, arguments, timeout=120):
        return self.command(["compose", "-f", str(BASE / "docker-compose.yml"), *arguments], timeout)

    def container_id(self, service="spark"):
        services = self.parse_services(self.compose(["ps", "-a", "--format", "json"], timeout=15))
        if service not in services:
            raise RuntimeError(f"Chưa có container {service}; bấm Chuẩn bị hệ thống.")
        return services[service]["ID"]

    @staticmethod
    def parse_services(text):
        if not text.strip():
            return {}
        try:
            rows = json.loads(text)
            if isinstance(rows, dict):
                rows = [rows]
        except ValueError:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        return {row["Service"]: row for row in rows
                if "com.docker.compose.oneoff=True" not in row.get("Labels", "")}

    def refresh_health(self):
        try:
            self.command(["version", "--format", "{{.Server.Version}}"], timeout=8)
            services = self.parse_services(self.compose(["ps", "-a", "--format", "json"], timeout=8))
            missing = [name for name in ("events.csv", "category_tree.csv", "item_properties_part1.csv",
                                         "item_properties_part2.csv")
                       if not (Path(self.live.csv_path).parent / name).is_file()]
            kafka = services.get("kafka", {})
            spark = services.get("spark", {})
            catalog = False
            try:
                _, manifest = catalog_location(self.live.catalog_path)
                catalog = True
            except (OSError, ValueError, KeyError):
                manifest = {}
            queries = self.live.reader.queries()
            required = {"retail_clean_events_v3", "retail_intent_v3", "retail_recommendation_console_v3"}
            running = {row["query_name"] for row in queries if row.get("status") == "running"}
            query_error = next((row.get("error") for row in queries if row.get("error")), None)
            ready = (not missing and catalog and kafka.get("State") == "running"
                     and kafka.get("Health") == "healthy" and spark.get("State") == "running"
                     and required <= running)
            # Running JSON files from an earlier run are not readiness evidence.
            if ready:
                details = json.loads(self.command(["inspect", spark["ID"]], timeout=8))[0]
                started = datetime.fromisoformat(details["State"]["StartedAt"].replace("Z", "+00:00")).timestamp()
                ready = all((self.live.output_root / "queries" / f"{name}.json").stat().st_mtime >= started - 1
                            for name in required)
            message = ("Hệ thống sẵn sàng" if ready else "Bấm Chuẩn bị hệ thống hoặc chờ Spark khởi động.")
            if missing:
                message = "Thiếu dataset: " + ", ".join(missing)
            elif query_error and spark.get("State") != "running":
                message = "Spark đã dừng/lỗi; xem Chi tiết hoặc log Spark."
            health = {"docker": True, "ready": bool(ready), "services": services,
                      "catalog": catalog, "catalog_version": manifest.get("catalog_version"),
                      "missing_data": missing, "message": message, "query_error": query_error}
        except Exception as error:
            health = {"docker": False, "ready": False, "services": {},
                      "message": "Chưa kết nối Docker Desktop. Mở Docker Desktop rồi bấm Chuẩn bị hệ thống.",
                      "detail": str(error)[-1500:]}
        with self.lock:
            self.health = health
        return health

    def _monitor(self):
        while not self.shutdown.is_set():
            self.refresh_health()
            self.shutdown.wait(3)

    def check_cancel(self):
        if self.cancel_event.is_set() or self.shutdown.is_set():
            raise InterruptedError("Đã yêu cầu hủy thao tác.")

    def wait_for(self, predicate, description, timeout=240):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.check_cancel()
            value = predicate()
            if value:
                return value
            self.cancel_event.wait(0.75)
        raise TimeoutError(f"Chưa đạt: {description}. Xem log Spark để kiểm tra.")

    def dispatch(self, name, action):
        with self.lock:
            if self.job_state == "running" or (self.job and self.job.is_alive()):
                return f"Đang thực hiện {self.job_name}; chờ hoàn tất hoặc bấm Hủy thao tác."
            self.job_name, self.job_state, self.job_error = name, "running", None
            self.cancel_event.clear()

            def execute():
                try:
                    self.log(f"Bắt đầu: {name}")
                    result = action()
                    self.check_cancel()
                    self.log(result or f"Hoàn tất: {name}")
                    with self.lock:
                        self.job_state = "complete"
                except Exception as error:
                    canceled = isinstance(error, InterruptedError)
                    self.log(f"{'Đã hủy' if canceled else 'Lỗi'}: {error}")
                    with self.lock:
                        self.job_state = "canceled" if canceled else "error"
                        self.job_error = str(error)
                    if self.scenario and not self.scenario.complete:
                        try:
                            self.scenario.rollback()
                        except Exception as rollback_error:
                            self.log(f"Cần khôi phục Spark: {rollback_error}")
                finally:
                    self.refresh_health()
            self.job = threading.Thread(target=execute, name="demo-control", daemon=True)
            self.job.start()
            return f"Đang thực hiện: {name}. Theo dõi tiến độ trên màn hình."

    def _require_live(self):
        if self.scenario is not None:
            raise ValueError("Bấm Trở về demo chính trước khi đổi phiên/phát CSV.")
        if not self.health.get("ready"):
            raise ValueError("Hệ thống chưa sẵn sàng; bấm Chuẩn bị hệ thống.")

    def _prepare(self):
        self.log("Kiểm tra Docker Desktop, dataset và cấu hình Compose…")
        health = self.refresh_health()
        if not health["docker"]:
            raise RuntimeError(health["message"])
        if health["missing_data"]:
            raise FileNotFoundError(health["message"])
        if health["ready"]:
            return "Kafka, catalog và ba query Spark đã sẵn sàng."
        self.log("Khởi động Kafka, chuẩn bị catalog và chạy Spark; lần đầu có thể mất vài phút…")
        output = self.compose(["up", "-d"], timeout=600)
        self.log(output[-1800:] or "Compose đã khởi động các service.")
        self.wait_for(lambda: self.refresh_health()["ready"], "ba query Spark sẵn sàng", timeout=300)
        return "Hệ thống sẵn sàng. Chọn Phiên mới hoặc một kịch bản kiểm chứng."

    def prepare(self):
        def action():
            if self.scenario is not None:
                raise ValueError("Trở về demo chính trước khi chuẩn bị lại hệ thống.")
            return self._prepare()
        return self.dispatch("Chuẩn bị hệ thống", action)

    def new_session(self, mode):
        def action():
            self._require_live()
            return self.live.new_session(mode)
        return self.dispatch("Phiên mới", action)

    def pause(self):
        def action():
            self._require_live()
            return self.live.pause()
        return self.dispatch("Tạm dừng", action)

    def resume(self):
        def action():
            self._require_live()
            return self.live.resume()
        return self.dispatch("Tiếp tục", action)

    def finish(self):
        def action():
            if self.scenario:
                raise ValueError("Bấm Trở về demo chính để kết thúc kịch bản kiểm chứng.")
            return self.live.finish()
        return self.dispatch("Kết thúc phiên", action)

    def step(self):
        def action():
            self._require_live()
            return self.live.step()
        return self.dispatch("Bước tiếp theo", action)

    def replay_csv(self, limit, interval):
        def action():
            self._require_live()
            return self.live.start_csv(int(limit), float(interval))
        return self.dispatch("Phát CSV", action)

    def begin_scenario(self, kind, isolated_recovery=False, tag=None):
        def action():
            if self.scenario is not None:
                raise ValueError("Kết thúc kịch bản hiện tại bằng Trở về demo chính trước.")
            self._require_live()
            from demo_scenarios import DemoScenario
            self.live.pause()
            self.scenario = DemoScenario(self, kind, isolated_recovery=isolated_recovery, tag=tag)
            self.scenario.prepare()
            return "Kịch bản sẵn sàng; xem bước tiếp theo bên dưới."
        return self.dispatch(f"Chuẩn bị {kind}", action)

    def scenario_next(self):
        def action():
            if self.scenario is None:
                raise ValueError("Chọn Recovery hoặc Watermark trước.")
            return self.scenario.advance()
        return self.dispatch("Bước kiểm chứng tiếp theo", action)

    def return_live(self):
        def action():
            if self.scenario:
                self.scenario.close()
                self.live.pause()
                self.scenario = None
            self.active = self.live
            return "Đã trở về demo chính. Phiên cũ tạm dừng; có thể Tiếp tục hoặc tạo Phiên mới."
        return self.dispatch("Trở về demo chính", action)

    def cancel(self):
        with self.lock:
            busy = bool(self.job and self.job.is_alive())
        if not busy and self.scenario is not None:
            return self.return_live()
        self.cancel_event.set()
        return "Đã yêu cầu hủy. Chờ lệnh đang chạy kết thúc; Spark bị dừng bởi recovery sẽ được mở lại."

    def stop_spark(self, identifier=None):
        return self.command(["stop", "--time", "60", identifier or self.container_id()], timeout=90)

    def start_spark(self, identifier=None):
        return self.command(["start", identifier or self.container_id()], timeout=60)

    def load_logs(self):
        def action():
            target = self.scenario.container if self.scenario else self.container_id()
            # Spark emits much of its log on stderr; subprocess capture below is
            # merged for the explicit log action only.
            result = self.runner(["docker", "logs", "--tail", "100", target], cwd=BASE,
                                 capture_output=True, encoding="utf-8", errors="replace", timeout=15,
                                 **({"creationflags": subprocess.CREATE_NO_WINDOW} if __import__("os").name == "nt" else {}))
            if result.returncode:
                raise RuntimeError(result.stderr[-2000:] or "Chưa đọc được log Spark")
            self.console_log = (result.stdout + "\n" + result.stderr)[-22000:]
            return "Đã đọc log thực từ Spark, gồm ConsoleSink và thông báo query."
        return self.dispatch("Đọc log Spark", action)

    def describe_topic(self):
        def action():
            from kafka.admin import KafkaAdminClient
            client = KafkaAdminClient(bootstrap_servers=self.active.bootstrap_servers,
                                      request_timeout_ms=5000, bootstrap_timeout_ms=5000)
            try:
                self.topic_info = client.describe_topics([self.active.topic])
            finally:
                client.close()
            return "Đã đọc metadata Kafka topic."
        return self.dispatch("Kiểm tra Kafka topic", action)

    def snapshot(self):
        with self.lock:
            return {"health": dict(self.health), "job": self.job_name, "job_state": self.job_state,
                    "busy": bool(self.job and self.job.is_alive()),
                    "error": self.job_error, "logs": list(self.logs), "topic": self.topic_info,
                    "console": self.console_log,
                    "scenario": self.scenario.snapshot() if self.scenario else None,
                    "source": "Demo chính" if self.active is self.live else "Watermark / kiểm chứng riêng"}

    def close(self):
        self.shutdown.set()
        self.cancel_event.set()
        if self.job:
            self.job.join(timeout=8)
        if self.scenario:
            try:
                self.scenario.close()
            except Exception:
                pass
        self.live.close()
