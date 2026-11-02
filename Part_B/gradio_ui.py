"""Local Gradio dashboard. All intent/ranking comes from Spark publications."""
import argparse
import atexit
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import time

import gradio as gr

from demo_runtime import DemoRuntime, utc_millis, window_key
from producer import BASE, CSV_PATH, TOPIC

VN_TIME = timezone(timedelta(hours=7))
CSS = """
.gradio-container { max-width: 1450px !important; }
.hero {padding: 20px 24px; border-radius: 16px; background: #12283b; color: #f2f8fc;}
.hero h1 {margin: 0 0 6px; color: #f2f8fc; font-size: 28px;}
.hero p {margin: 0; color: #a8d8db;}
.badges {display: flex; gap: 10px; flex-wrap: wrap; margin: 12px 0;}
.badge {padding: 8px 13px; background: #e6f4f1; color: #12564f; border-radius: 10px;}
"""


def local_time(value):
    if value is None:
        return ""
    stamp = (datetime.fromtimestamp(value / 1000, timezone.utc) if isinstance(value, (int, float))
             else datetime.fromisoformat(value.replace("Z", "+00:00")))
    return stamp.astimezone(VN_TIME).strftime("%d/%m %H:%M:%S.%f")[:-3]


def refresh_dashboard(runtime, visitor=None, selected_window="Hiện tại", session_token=None,
                      show_history=False, follow_latest=False):
    snapshot = runtime.snapshot(show_history=show_history)
    now = int(time.time() * 1000)
    visitors = sorted({row["visitorid"] for row in snapshot["interactions"]}
                      | {row["visitorid"] for row in snapshot["intents"]})
    if snapshot["session_id"] is not None:
        visitors = sorted(set(visitors) | {snapshot["session_id"]})
    # Per-browser session token prevents a concurrent old timer response from
    # leaving the selector on a previous visitor after Start creates a new demo.
    if snapshot["session_id"] is not None and str(session_token) != str(snapshot["session_id"]):
        visitor = str(snapshot["session_id"])
        selected_window = "Phiên"
    if follow_latest and snapshot.get("pending"):
        visitor = str(snapshot["pending"]["visitorid"])
    if visitor is None or str(visitor) not in {str(value) for value in visitors}:
        visitor = str(snapshot["session_id"] or (visitors[-1] if visitors else ""))
    rows = [row for row in snapshot["intents"] if str(row["visitorid"]) == str(visitor)]
    windows = sorted({window_key(row)[1:] for row in rows}, reverse=True)
    values = ["Phiên", "Hiện tại"] + [f"{start}|{end}" for start, end in windows]
    choices = [("Window của phiên / kịch bản", "Phiên"), ("Window mới nhất theo đồng hồ", "Hiện tại")]
    choices += [(f"{local_time(start)} → {local_time(end)}", f"{start}|{end}") for start, end in windows]
    if selected_window not in values:
        selected_window = "Phiên"
    if selected_window == "Phiên":
        anchors = {anchor for (v, _), anchor in snapshot.get("anchors", {}).items() if str(v) == str(visitor)}
        matches = [pair for pair in windows if pair[0] in anchors]
        chosen = matches[0] if matches else (windows[0] if windows else None)
    elif selected_window == "Hiện tại":
        active = [pair for pair in windows if pair[0] <= now < pair[1]]
        chosen = active[0] if active else None
    else:
        chosen = tuple(map(int, selected_window.split("|")))
    intent_rows = [row for row in rows if window_key(row)[1:] == chosen]
    recommendation = next((row for row in snapshot["recommendations"]
                           if str(row["visitorid"]) == str(visitor)
                           and window_key(row)[1:] == chosen), None)
    reason_names = {"heavy_browsing": "Quan tâm qua nhiều lượt xem",
                    "cart_no_purchase": "Đã thêm giỏ, chưa thấy thanh toán"}
    intent = [[row["itemid"], runtime.item_categories.get(row["itemid"], ""), row["views"],
               row["carts"], row["transactions"], row["intent_score"], row["status"],
               reason_names.get(row.get("recommendation_reason"), ""), row["batch_id"]]
              for row in sorted(intent_rows, key=lambda row: row["itemid"])]
    recs = [[row["rank"], row["candidate_itemid"], row["candidate_categoryid"],
             round(row["category_similarity"], 4), round(row["rec_score"], 4),
             ", ".join(map(str, row["contributing_seed_itemids"])), row["source"]]
            for row in (recommendation or {}).get("recommendations", [])]
    interactions = [[local_time(row["timestamp"]), str(row["visitorid"]), row["itemid"],
                     row.get("categoryid"), row["event"], row["partition"], row["offset"]]
                    for row in reversed(snapshot["interactions"])]
    metrics = [[row["query_name"], row["batch_id"], row["input_records"],
                row["trigger_duration_ms"], row.get("watermark"), row.get("state_rows"),
                row.get("late_rows_dropped"), row.get("state_memory_bytes")]
               for row in snapshot["metrics"]]
    pipeline = snapshot["pipeline"] or {}
    status = {"idle": "Chưa có phiên", "running": "Đang chạy", "paused": "Tạm dừng",
              "ended": "Đã kết thúc", "complete": "Đã hoàn tất", "scenario": "Đang kiểm chứng"}.get(
                  snapshot.get("session_state"), "Đang chạy" if snapshot["running"] else "Đã dừng phát")
    if snapshot["error"]:
        status = "Lỗi: " + snapshot["error"]
    processing = "Đã có kết quả Spark cho lần gửi gần nhất" if snapshot["processed"] else "Đang chờ Spark xử lý"
    if snapshot.get("session_state") == "idle" or snapshot["sent"] == 0:
        processing = "Chưa gửi interaction trong phiên này."
    batch = (snapshot["commit"] or {}).get("batch_id", "chưa có")
    summary = f"**{status}** · {snapshot['mode']} · ACK trong tiến trình: **{snapshot['sent']}** · Intent batch commit: **{batch}**\n\n{processing}"
    if chosen:
        summary += f"\n\nWindow đang xem: **{local_time(chosen[0])} → {local_time(chosen[1])}** (UTC+7)."
    if recommendation:
        summary += f"\n\nKết quả: `{recommendation['generation_status']}` · {len(recs)} sản phẩm."
    elif intent_rows:
        summary += "\n\nChưa có publication recommendation cho window này."
    config = {"output": str(runtime.output_root), "topic": runtime.topic,
              "pipeline": pipeline, "queries": snapshot["queries"],
              "latest_commit": snapshot["commit"],
              "latest_sent_event": snapshot["interactions"][-1] if snapshot["interactions"] else None}
    return (summary, interactions, intent, recs, metrics,
            gr.update(choices=[str(value) for value in visitors], value=visitor or None),
            gr.update(choices=choices, value=selected_window), config, snapshot["session_id"])


def read_storage(runtime):
    import pyarrow.parquet as pq
    snapshot = runtime.snapshot()
    commit = snapshot["commit"]
    if not commit:
        return [], [], "Chưa có batch được commit."
    artifact = runtime.output_root / commit["artifact_path"]
    def read_sample(name):
        rows = []
        for path in sorted((artifact / name).rglob("*.parquet")):
            # Windows bind-mount artifact paths can exceed MAX_PATH. Give Arrow
            # an already-open Python file rather than its own path resolver.
            filename = str(path.resolve())
            if os.name == "nt":
                filename = "\\\\?\\" + filename
            with open(filename, "rb") as source:
                parquet = pq.ParquetFile(source)
                for batch in parquet.iter_batches(batch_size=10):
                    rows.extend(batch.to_pylist())
                    if len(rows) >= 10:
                        return rows[:10]
        return rows
    intents, recs = read_sample("intent"), read_sample("recommendations")
    return (json.loads(json.dumps(intents, default=str)),
            json.loads(json.dumps(recs, default=str)),
            f"Đã đọc trực tiếp Parquet: {artifact} · partition event_date · tối đa 10 dòng/bảng.")


def describe_topic(runtime):
    from kafka.admin import KafkaAdminClient
    client = KafkaAdminClient(bootstrap_servers=runtime.bootstrap_servers,
                              request_timeout_ms=5000, bootstrap_timeout_ms=5000)
    try:
        return client.describe_topics([runtime.topic])
    finally:
        client.close()


def build_app(runtime=None, controller=None):
    from demo_controller import DemoController
    hub = controller or DemoController(runtime)
    with gr.Blocks(title="Retail Rocket · Demo Control") as app:
        session_token = gr.State(None)
        gr.HTML('<div class="hero"><h1>Retail Rocket · Demo Control</h1>'
                '<p>Kafka → Stateful Spark → Category recommendations</p></div>'
                '<div class="badges"><span class="badge">Window 10 phút · slide 5 phút · watermark 5 phút</span>'
                '<span class="badge">5 views → score 5 → recommend</span>'
                '<span class="badge">Interaction tự động mỗi 5 giây</span></div>')
        with gr.Row():
            prepare = gr.Button("Chuẩn bị hệ thống", variant="primary")
            cancel = gr.Button("Hủy thao tác")
        system = gr.Markdown("Đang kiểm tra Docker, Kafka, catalog và Spark…", elem_id="system-status")
        notice = gr.Markdown("Bấm Chuẩn bị hệ thống, chờ sẵn sàng rồi chọn Phiên mới.", elem_id="action-notice")
        with gr.Row():
            mode = gr.Radio(["Tự động", "Từng bước"], value="Từng bước", label="Chế độ phiên mới")
            new = gr.Button("Phiên mới", variant="primary")
            pause = gr.Button("Tạm dừng")
            resume = gr.Button("Tiếp tục")
            finish = gr.Button("Kết thúc phiên")
            step = gr.Button("Bước tiếp theo")
        summary = gr.Markdown("Chưa có phiên.", elem_id="demo-summary")
        with gr.Tabs():
            with gr.Tab("Intent & Recommendation"):
                with gr.Row():
                    history = gr.Checkbox(label="Xem thêm lịch sử các phiên", value=False)
                    follow = gr.Checkbox(label="Theo dõi interaction mới nhất", value=True)
                interactions = gr.Dataframe(headers=["Thời gian UTC+7", "Visitor", "Item", "Category",
                    "Hành động", "Partition", "Offset"], interactive=False, max_height=260,
                    label="Interaction đã được Kafka ACK", elem_id="interactions")
                with gr.Row():
                    visitor = gr.Dropdown(label="Visitor", choices=[], allow_custom_value=False)
                    window = gr.Dropdown(label="Window đang xem", choices=[("Window của phiên / kịch bản", "Phiên")], value="Phiên")
                intents = gr.Dataframe(headers=["Item", "Category", "Views", "Carts", "Transactions",
                    "Intent", "Trạng thái", "Lý do", "Batch"], interactive=False,
                    label="Intent do Spark tính", elem_id="intents")
                recs = gr.Dataframe(headers=["Rank", "Item gợi ý", "Category", "Similarity lớn nhất",
                    "Score", "Seed đóng góp", "Nguồn"], interactive=False, label="Top-K category", elem_id="recommendations")
                gr.Markdown("**Quan sát:** 4 views chưa gợi ý → view thứ 5: score 5, carts 0, top-K → "
                            "transaction: score 15 và thu hồi gợi ý. Kafka ACK và Spark commit là hai trạng thái riêng.")
            with gr.Tab("Kafka & CSV"):
                with gr.Row():
                    csv_limit = gr.Number(value=20, precision=0, minimum=1, maximum=10000, label="Số dòng CSV")
                    csv_interval = gr.Number(value=0.1, minimum=0.05, maximum=60, label="Interval CSV (giây)")
                    csv_button = gr.Button("Phát CSV")
                topic_button = gr.Button("Kiểm tra Kafka topic / partitions")
                topic_info = gr.JSON(label="Metadata Kafka thật")
                gr.Markdown("CSV giữ visitor/item/action từ events.csv và dùng thời gian phát hiện tại. "
                            "ACK xuất hiện trong cùng bảng interaction. Tạm dừng/Tiếp tục giữ vị trí CSV.")
            with gr.Tab("Recovery & Watermark"):
                with gr.Row():
                    recovery = gr.Button("Bắt đầu Recovery")
                    watermark = gr.Button("Bắt đầu Watermark")
                    scenario_next = gr.Button("Bước kiểm chứng tiếp theo", variant="primary")
                    back = gr.Button("Trở về demo chính")
                scenario_status = gr.Markdown("Chọn một kịch bản. Mỗi bước chờ bằng chứng thực từ Spark.", elem_id="scenario-status")
                scenario_data = gr.JSON(label="Kết quả đối chiếu trước/sau", elem_id="scenario-evidence")
                gr.Markdown("**Recovery:** 4 views đã commit → dừng Spark → Kafka ACK view thứ 5 → "
                            "khởi động lại cùng checkpoint, score 5 → transaction clear.\n\n"
                            "**Watermark:** dùng topic/checkpoint riêng, tự chuyển bảng và metrics sang nguồn kiểm chứng. "
                            "Các timestamp là thời gian quá khứ có kiểm soát. Trở về demo chính sẽ dừng container riêng.")
            with gr.Tab("Logging & Storage"):
                metrics = gr.Dataframe(headers=["Query", "Batch", "Input records", "Trigger ms", "Watermark UTC",
                    "State rows", "Late rows", "State bytes"], interactive=False, label="Micro-batch metrics thật")
                with gr.Row():
                    log_button = gr.Button("Đọc log Spark")
                    storage_button = gr.Button("Đọc lại Parquet")
                console = gr.Textbox(label="Log Spark / native ConsoleSink", lines=12, interactive=False)
                storage_notice = gr.Markdown()
                storage_intent = gr.JSON(label="Intent đọc trực tiếp từ Parquet")
                storage_recs = gr.JSON(label="Recommendation đọc trực tiếp từ Parquet")
                gr.Markdown("Counts của các query độc lập không cộng thành tổng event duy nhất. "
                            "Trigger 5 giây không bảo đảm batch hoàn tất trong 5 giây.")
        with gr.Accordion("Tiến độ và nhật ký điều khiển", open=True):
            action_log = gr.Textbox(label="Thao tác gần nhất", lines=7, interactive=False)
        with gr.Accordion("Chi tiết kỹ thuật: JSON, query ID, run ID, nguồn dữ liệu", open=False):
            config = gr.JSON(label="Nguồn dữ liệu đang xem")
            controls = gr.JSON(label="Trạng thái hệ thống và tác vụ")

        prepare.click(hub.prepare, outputs=notice)
        cancel.click(hub.cancel, outputs=notice, queue=False)
        new.click(hub.new_session, mode, notice)
        pause.click(hub.pause, outputs=notice)
        resume.click(hub.resume, outputs=notice)
        finish.click(hub.finish, outputs=notice)
        step.click(hub.step, outputs=notice)
        csv_button.click(hub.replay_csv, [csv_limit, csv_interval], notice)
        topic_button.click(hub.describe_topic, outputs=notice)
        recovery.click(lambda: hub.begin_scenario("Recovery"), outputs=notice)
        watermark.click(lambda: hub.begin_scenario("Watermark"), outputs=notice)
        scenario_next.click(hub.scenario_next, outputs=notice)
        back.click(hub.return_live, outputs=notice)
        log_button.click(hub.load_logs, outputs=notice)

        def storage():
            try:
                return read_storage(hub.active)
            except Exception as error:
                return [], [], f"Không đọc được Parquet: {error}"
        storage_button.click(storage, outputs=[storage_intent, storage_recs, storage_notice])

        dashboard_outputs = [summary, interactions, intents, recs, metrics, visitor, window, config, session_token]
        outputs = dashboard_outputs + [system, scenario_status, scenario_data, action_log, console, topic_info,
                                       controls, scenario_next, step, new, pause, resume, finish, prepare,
                                       recovery, watermark, back, csv_button]
        def render(selected_visitor, selected_window, token, show_history, follow_latest):
            state = hub.snapshot()
            source = hub.active
            scenario = state["scenario"]
            if scenario:
                selected_visitor, follow_latest = str(scenario["visitorid"] or ""), False
            data = refresh_dashboard(source, selected_visitor, selected_window, token, show_history, follow_latest)
            health = state["health"]
            service = health.get("services", {})
            def label(name):
                row = service.get(name, {})
                return row.get("Health") or row.get("State") or "chưa chạy"
            sys_text = (f"**{health['message']}** · Docker: {'kết nối' if health.get('docker') else 'chưa kết nối'} "
                        f"· Kafka: {label('kafka')} · Catalog: {'sẵn sàng' if health.get('catalog') else 'chưa có'} "
                        f"· Spark: {label('spark')}\n\nNguồn đang xem: **{state['source']}**")
            if state["busy"]:
                sys_text += f" · **Đang thực hiện: {state['job']}**"
            if state["error"]:
                sys_text += f"\n\n**Thao tác chưa hoàn tất:** {state['error']}"
            sc_text = "Chọn Recovery hoặc Watermark để bắt đầu kiểm chứng."
            if scenario:
                sc_text = (f"**{scenario['kind']} · bước {scenario['phase']} · {scenario['status']}**\n\n"
                           f"{'Kết quả đã xác nhận.' if scenario['status'] == 'complete' else 'Tiếp theo: ' + scenario['next_step']}")
            recent = state["logs"][-35:] + source.snapshot().get("logs", [])[-12:]
            recent.sort(key=lambda row: row["time"])
            log_text = "\n".join(f"{local_time(row['time'])}  {row['message']}" for row in recent[-40:])
            snap = source.snapshot()
            idle = not state["busy"]
            live = idle and scenario is None
            ready = live and health.get("ready", False)
            return (*data, sys_text, sc_text, scenario or {}, log_text, state["console"], state["topic"],
                    {key: value for key, value in state.items() if key not in {'logs', 'console', 'scenario'}},
                    gr.update(interactive=idle and bool(scenario) and scenario['status'] == 'ready'),
                    gr.update(interactive=ready and snap['mode'] == 'Từng bước' and snap['running'] and snap['processed']),
                    gr.update(interactive=ready), gr.update(interactive=ready and snap['running']),
                    gr.update(interactive=ready and snap.get('session_state') == 'paused'),
                    gr.update(interactive=live), gr.update(interactive=live),
                    gr.update(interactive=ready), gr.update(interactive=ready),
                    gr.update(interactive=idle and bool(scenario)), gr.update(interactive=ready))
        inputs = [visitor, window, session_token, history, follow]
        gr.Timer(1.0).tick(render, inputs, outputs, queue=False)
        app.load(render, inputs, outputs)
        window.input(render, inputs, outputs)
        history.input(render, inputs, outputs)
        follow.input(render, inputs, outputs)
        def select_visitor(v, w, t, h, f):
            return (*render(v, w, t, h, False), gr.update(value=False))
        visitor.input(select_visitor, inputs, outputs + [follow])
    app.demo_controller = hub
    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=BASE / "catalog/category-v1")
    parser.add_argument("--output", type=Path, default=BASE / "output/category-realtime-v1")
    parser.add_argument("--csv", type=Path, default=CSV_PATH)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--port", type=int, default=7860)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    from demo_controller import DemoController
    runtime = DemoRuntime(args.catalog, args.output, args.csv, args.bootstrap_servers, args.topic)
    controller = DemoController(runtime)
    atexit.register(controller.close)
    build_app(controller=controller).queue().launch(server_name="127.0.0.1", server_port=args.port,
        share=False, css=CSS, inbrowser=not args.no_browser)


if __name__ == "__main__":
    main()
