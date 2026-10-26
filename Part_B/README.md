# PHẦN B: HIỆN THỰC THỰC HÀNH & PIPELINE (70 ĐIỂM)

> **Mục tiêu:** Xây dựng và vận hành pipeline xử lý dữ liệu luồng thực tế (End-to-End Real-Time Pipeline) từ Kafka Producer $\rightarrow$ Kafka Broker (KRaft mode, $\ge 2$ partitions) $\rightarrow$ PySpark Structured Streaming (Sliding Window 10 phút, Watermark 5 phút, Intent Score, Recommender, Logging) $\rightarrow$ Parquet Sink & Checkpointing.

---

## 1. CẤU TRÚC THƯ MỤC PHẦN B

```
Part_B/
├── README.md               # Hướng dẫn chi tiết chạy thực hành Part B
├── docker-compose.yml      # Cụm Kafka KRaft + Spark Structured Streaming
├── producer.py             # Event Producer đọc events.csv và bắn message JSON
├── streaming_etl.py        # PySpark ETL core: Window 10m, Watermark 5m, Intent Score, Recommender, Sink
├── requirements.txt        # Thư viện Python cho Producer (kafka-python)
├── output/                 # Thư mục sinh ra khi chạy: Lưu file Parquet (intent/) và JSON metrics (metrics/)
└── checkpoints/            # Thư mục lưu trạng thái phục hồi lỗi (Fault Tolerance)
```

---

## 2. HƯỚNG DẪN CHẠY DEMO TỪNG BƯỚC (STEP-BY-STEP)

### Bước 1: Chuẩn bị dữ liệu `events.csv`
Đặt file `events.csv` (Retail Rocket dataset tải từ Kaggle) vào thư mục:
- `../retailrocket/events.csv` (tại thư mục gốc của dự án), HOẶC
- `./retailrocket/events.csv` (ngay trong thư mục `Part_B/`).

### Bước 2: Chuẩn bị môi trường Python cho Producer
Mở PowerShell tại thư mục `Part_B/`:
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### Bước 3: Khởi động Cụm Kafka & Spark
Tại thư mục `Part_B/`, chạy lệnh khởi động Docker:
```powershell
docker compose up -d
```

Kiểm tra trạng thái các container:
```powershell
docker compose ps
docker compose logs -f spark
```
*Đợi đến khi thấy Spark in dòng log `"kind": "query_started"` nghĩa là pipeline đã sẵn sàng tiếp nhận luồng sự kiện.*

### Bước 4: Phát luồng sự kiện (Kafka Producer)
Mở một cửa sổ PowerShell mới tại thư mục `Part_B/`:

* **Cách 1: Chạy Demo nhanh (5 sự kiện mô phỏng phiên khách hàng):**
  ```powershell
  .\.venv\Scripts\python.exe producer.py --demo
  ```

* **Cách 2: Phát dữ liệu thực tế từ `events.csv` kèm điều tiết tốc độ (Pacing):**
  ```powershell
  # Bắn 200 sự kiện, cách nhau 0.1 giây, gán timestamp hiện tại
  .\.venv\Scripts\python.exe producer.py --replay-clock --interval 0.1 --limit 200
  ```

### Bước 5: Quan sát kết quả xử lý thời gian thực
* **Xem bảng kết quả trên màn hình console:**
  ```powershell
  docker compose logs -f spark
  ```
  Quan sát bảng intent score, cột `status` (`recommend`, `purchased`, `browsing`) và `recommended_itemid`.
* **Kiểm tra file Parquet đã ghi:**
  Xem thư mục `output/intent/`.
* **Kiểm tra chỉ số đo lường hiệu năng:**
  Xem các file JSON tại `output/metrics/`.

### Bước 6: Dọn dẹp sau khi demo xong
```powershell
docker compose down
```
*(Nếu muốn xóa cả checkpoint để chạy mới hoàn toàn: xóa thư mục `checkpoints/` và `output/`).*
