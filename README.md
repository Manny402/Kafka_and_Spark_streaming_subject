# Retail Rocket — Kafka & PySpark Structured Streaming Pipeline

Dự án Big Data xây dựng đường ống xử lý luồng sự kiện thời gian thực (End-to-End Real-Time Data Streaming Pipeline) trên tập dữ liệu thương mại điện tử **Retail Rocket**:
* **Thu nhận dữ liệu (Ingestion):** Sử dụng **Apache Kafka 4.3.1** (chế độ **KRaft mode**, không phụ thuộc ZooKeeper, phân vùng $\ge 2$ partitions) tiếp nhận luồng sự kiện clickstream JSON.
* **Xử lý luồng có trạng thái (Stateful Stream Processing):** **PySpark Structured Streaming 4.2.0** áp dụng mô hình Bảng vô hạn (Unbounded Table), **Sliding Window 10 phút** và **Watermark 5 phút** để tính điểm tương tác tích lũy (**Intent Score**).
* **Hệ thống gợi ý thời gian thực (Real-time Recommender):** Tự động phát hiện người dùng có ý định mua hàng cao nhưng bỏ rơi giỏ hàng (`carts > 0` và `transactions == 0`) để kích hoạt gợi ý sản phẩm tức thì (`status = recommend`).
* **Lưu trữ & Chịu lỗi (Sink & Fault Tolerance):** Lưu trữ dữ liệu tinh chế định dạng **Parquet**, ghi nhận log hiệu năng vi mô (micro-batch latency, watermark, record count) và đảm bảo phục hồi trạng thái toàn vẹn qua **Checkpointing**.

---

## 1. Thông tin nhóm thực hiện (Nhóm 5)

| STT | Họ và tên | Mã sinh viên |
| :---: | :--- | :---: |
| 1 | **Nguyễn Phú Nam** | 11230574 |
| 2 | **Hồ Đức Mạnh** | 11230565 |
| 3 | **Nguyễn Phương Đông** | 11230530 |
| 4 | **Vũ Tuấn Đạt** | 11230529 |
| 5 | **Nguyễn Đại Quân** | 11230586 |
| 6 | **Trần Minh Đức** | 11230531 |
| 7 | **Nguyễn Tuấn Phong** | 11230580 |

---

## 2. Cấu trúc thư mục dự án

```text
Kafka_and_Spark_streaming_subject/
│
├── Part_A/                         # PHẦN A: NỀN TẢNG LÝ THUYẾT & NGHIÊN CỨU HỌC THUẬT (30 ĐIỂM)
│   ├── README.md                   # Mục lục và tổng quan nội dung Part A
│   ├── A1_Spark_Architecture/      # Đề mục A.1: DStream vs Structured Streaming & Continuous ETL
│   │   ├── A1_1_DStream_vs_Structured_Streaming.ipynb
│   │   ├── A1_1_DStream_vs_Structured_Streaming.html
│   │   └── A1_2_ETL_Data_Architecture.ipynb
│   ├── A2_Recommender_Systems/     # Đề mục A.2: Hệ thống gợi ý & Thuật toán Live Intent Scoring
│   │   └── A2_Recommender_Systems_Spark_Streaming.ipynb
│   └── A3_Ingestion_Kafka_vs_Flume/# Đề mục A.3: Thu nhận phân tán Kafka vs. Apache Flume
│       └── A3_Kafka_vs_Flume.ipynb
│
├── Part_B/                         # PHẦN B: HIỆN THỰC THỰC HÀNH & PIPELINE (70 ĐIỂM)
│   ├── README.md                   # Hướng dẫn chi tiết chạy thực hành Part B
│   ├── docker-compose.yml          # Dựng cụm Kafka KRaft (2 partitions) & Spark Structured Streaming
│   ├── producer.py                 # Bộ phát sự kiện JSON từ events.csv vào Kafka
│   ├── streaming_etl.py            # PySpark ETL core: Window 10m, Watermark 5m, Intent Score, Sink
│   ├── requirements.txt            # Thư viện Python cho Producer (kafka-python==3.0.11)
│   ├── output/                     # Thư mục sinh khi chạy: Lưu Parquet (intent/) và Metrics JSON (metrics/)
│   └── checkpoints/                # Thư mục lưu trạng thái phục hồi lỗi (retail-intent-v1)
│
├── retailrocket/                   # Thư mục chứa tập tin dữ liệu từ Kaggle
│   ├── events.csv                  # Dữ liệu chính dùng cho stream (~2.75M dòng, 94.2 MB)
│   └── category_tree.csv           # Cây phân cấp ngành hàng
│
├── docs/images/                    # 11 sơ đồ kiến trúc trích xuất từ tài liệu học thuật
├── REPORT.md                       # Báo cáo kỹ thuật tổng thể cuối kỳ (Full Part A + Part B + Demo)
├── DATA_DICTIONARY_AND_EDA.md      # Từ điển dữ liệu toàn diện & Phân tích khám phá dữ liệu (EDA)
├── DATA_DICTIONARY_AND_EDA.ipynb   # Notebook Jupyter chạy thực tế toàn bộ code EDA
├── LAM_MIN_YEU_CAU_DE_TAI.md       # Bản làm mịn yêu cầu đề bài và barem chấm điểm chi tiết
└── README.md                       # Tài liệu hướng dẫn chung này
```

---

## 3. Chuẩn bị môi trường & Dữ liệu

### 3.1. Yêu cầu phần mềm
| Phần mềm | Phiên bản khuyến nghị | Mục đích |
| :--- | :--- | :--- |
| **Docker Desktop** | Bản mới nhất (Linux containers) | Chạy Kafka Broker và Spark Master/Worker |
| **Python** | 3.10 – 3.13 | Chạy script phát sự kiện `producer.py` |
| **PowerShell / Bash** | Có sẵn trên Windows/Linux | Thực thi các câu lệnh điều khiển |

Kiểm tra Docker Desktop đã sẵn sàng:
```powershell
docker --version
docker compose version
```

### 3.2. Tải và đặt bộ dữ liệu Retail Rocket
1. Tải dữ liệu từ [Kaggle Retail Rocket E-Commerce Dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset).
2. Giải nén và đặt tệp `events.csv` vào thư mục `retailrocket/events.csv` tại gốc dự án (hoặc `Part_B/retailrocket/events.csv`).

Kiểm tra từ thư mục `Part_B/`:
```powershell
Test-Path ../retailrocket/events.csv
```
*(Kết quả trả về `True` là hợp lệ).*

---

## 4. Khởi động hệ thống (Kafka KRaft & Spark Streaming)

Toàn bộ hạ tầng phân tán được khai báo trong `Part_B/docker-compose.yml`. Mở PowerShell và di chuyển vào thư mục `Part_B/`:

```powershell
cd Part_B

# Khởi động cụm container chạy ngầm
docker compose up -d
```

Kiểm tra trạng thái các container:
```powershell
docker compose ps
```

| Service | Trạng thái mong đợi | Vai trò |
| :--- | :--- | :--- |
| `kafka` | `Up ... (healthy)` | Broker Kafka 4.3.1 chế độ KRaft (cổng `9092` cho host, `19092` nội bộ) |
| `kafka-init` | `Exited (0)` | Tự động tạo topic `retail-rocket-events` với **2 Partitions** |
| `spark` | `Up ...` | Container Spark 4.2.0 tự nạp connector `spark-sql-kafka` và chạy `streaming_etl.py` |

Theo dõi log khởi động của Spark:
```powershell
docker compose logs -f --tail 30 spark
```
*Khi thấy log xuất hiện `"kind": "query_started"`, Spark Streaming đã sẵn sàng tiếp nhận luồng sự kiện.*  
*(Có thể truy cập Spark UI tại: [http://localhost:4040](http://localhost:4040)).*

---

## 5. Kịch bản chạy Live Demo trực tiếp (Step-by-Step)

Mục tiêu demo: Chứng minh hệ thống tiếp nhận hành vi, tính toán điểm ý định theo cửa sổ trượt, kích hoạt gợi ý sản phẩm cho người dùng bỏ rơi giỏ hàng và cập nhật trạng thái khi khách hàng hoàn tất thanh toán.

### Bước 1: Cài đặt thư viện cho Producer
Mở một cửa sổ PowerShell mới tại thư mục `Part_B/`:
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### Bước 2: Kiểm tra topic Kafka đã có 2 Partitions
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic retail-rocket-events
```
*Kết quả hiển thị `PartitionCount: 2` xác nhận dữ liệu có thể phân phối song song.*

### Bước 3: Phát luồng sự kiện mô phỏng 2 khách hàng (`--demo`)
Chạy script producer ở chế độ mô phỏng phiên với khoảng nghỉ 5 giây giữa các thao tác để tiện theo dõi:

```powershell
.\.venv\Scripts\python.exe producer.py --demo --interval 5
```

Script sẽ phát tuần tự 5 sự kiện của 2 khách hàng $A$ và $B$ (sử dụng ID sản phẩm thật từ dataset):
1. **Khách A: `view` sản phẩm X** $\rightarrow$ Điểm = 1, `status = browsing`.
2. **Khách A: `addtocart` sản phẩm X** $\rightarrow$ Điểm = 6, `status = recommend`, kích hoạt gợi ý sản phẩm X.
3. **Khách B: `view` sản phẩm Y** $\rightarrow$ Điểm = 1, `status = browsing`.
4. **Khách B: `addtocart` sản phẩm Y** $\rightarrow$ Điểm = 6, `status = recommend`, kích hoạt gợi ý sản phẩm Y.
5. **Khách A: `transaction` sản phẩm X** $\rightarrow$ Điểm = 16, `status = purchased`, tắt gợi ý đối với khách A.

### Bước 4: Đọc bảng kết quả trên console Spark
Quan sát terminal đang chạy `docker compose logs -f spark`, Spark sẽ in bảng tổng hợp theo micro-batch:

| Cột hiển thị | Ý nghĩa kỹ thuật |
| :--- | :--- |
| `window_start`, `window_end` | Khung thời gian của cửa sổ trượt 10 phút |
| `visitorid`, `itemid` | Định danh khách hàng và mã sản phẩm tương tác |
| `views`, `carts`, `transactions` | Số lượt tương tác lũy kế trong cửa sổ |
| `intent_score` | $(\text{views} \times 1) + (\text{carts} \times 5) + (\text{transactions} \times 10)$ |
| `status` | `browsing` (đang xem), `recommend` (cần gợi ý), hoặc `purchased` (đã mua) |
| `recommended_itemid` | Mã sản phẩm được đề xuất gửi về cho khách (hoặc `NULL` khi đã mua) |

---

## 6. Kịch bản kiểm thử Chịu lỗi & Khôi phục (Fault Tolerance & Recovery)

Kịch bản chứng minh: Khi ứng dụng tính toán Spark gặp sự cố hoặc tạm dừng bảo trì, Kafka vẫn tiếp nhận tin nhắn từ Producer và Spark có thể phục hồi chính xác từ Checkpoint khi khởi động lại mà không làm mất dữ liệu.

* **Thao tác 1 — Dừng container Spark:**
  ```powershell
  docker compose stop spark
  ```
  *(Kafka Broker vẫn hoạt động bình thường).*

* **Thao tác 2 — Producer tiếp tục phát sự kiện khi Spark đang dừng:**
  ```powershell
  .\.venv\Scripts\python.exe producer.py --demo --interval 1
  ```
  *Producer thông báo `Sent=... offset=...` thành công, chứng minh dữ liệu đã được lưu trữ bền vững trong commit log của Kafka.*

* **Thao tác 3 — Khởi động lại Spark:**
  ```powershell
  docker compose start spark
  docker compose logs -f --tail 30 spark
  ```
* **Kết quả quan sát:** Spark tải lại state từ `checkpoints/retail-intent-v1/`, đọc tiếp đúng offset chưa xử lý trên Kafka, và cập nhật bảng tính điểm ngay trong batch đầu tiên sau khi khởi động.

---

## 7. Phát luồng dữ liệu thực tế từ file CSV

Sau khi chạy demo kiểm thử, có thể truyền trực tiếp dữ liệu từ tệp `events.csv` kèm điều tiết tốc độ (Pacing):

```powershell
# Bắn 1.000 dòng sự kiện thật, cách nhau 0.1 giây, chuyển timestamp sang thời gian hiện tại
.\.venv\Scripts\python.exe producer.py --replay-clock --interval 0.1 --limit 1000
```

* `--replay-clock`: Chuyển mốc thời gian lịch sử năm 2015 thành thời điểm hiện tại của máy để khớp với cơ chế Watermark 5 phút của Spark.
* `--interval 0.1`: Khoảng nghỉ 0.1 giây giữa các sự kiện (mô phỏng 10 sự kiện/giây).
* `--limit 1000`: Giới hạn số lượng dòng phát (bỏ cờ này nếu muốn phát toàn bộ file).

---

## 8. Kết quả lưu trữ & Giám sát hiệu năng

| Vị trí thư mục | Định dạng dữ liệu | Nội dung & Mục đích |
| :--- | :--- | :--- |
| `Part_B/output/intent/batch_.../` | **Parquet** | Dữ liệu tinh chế có cấu trúc theo từng micro-batch (chứa điểm intent, status, recommended item) |
| `Part_B/output/metrics/batch_...json` | **JSON** | Nhật ký đo lường vi mô: `input_records`, `trigger_duration_ms`, `watermark`, `state_memory_bytes` |
| `Part_B/checkpoints/retail-intent-v1/` | **Metadata / WAL** | Checkpoint lưu vết offset Kafka, commit log và trạng thái phân tán của State Store |

Kiểm tra nhanh kết quả bằng PowerShell:
```powershell
# Xem 3 batch Parquet mới nhất
Get-ChildItem output/intent -Directory | Select-Object -Last 3

# Xem chỉ số hiệu năng của batch gần nhất
Get-ChildItem output/metrics/*.json | Select-Object -Last 1 | Get-Content
```

---

## 9. Checklist nghiệm thu & Xử lý sự cố thường gặp

### 9.1. Checklist nghiệm thu đề tài
- [x] Cụm Kafka KRaft mode hoạt động không phụ thuộc ZooKeeper (`docker compose ps`).
- [x] Topic `retail-rocket-events` được tạo sẵn với $\ge 2$ partitions (`PartitionCount: 2`).
- [x] Producer gửi dữ liệu định dạng JSON chuẩn schema với Partition Key theo `visitorid`.
- [x] PySpark Structured Streaming áp dụng đúng **Window 10 phút** và **Watermark 5 phút**.
- [x] Thuật toán tính đúng điểm Intent Score (`view=1, cart=5, trans=10`) và lọc đúng điều kiện gợi ý (`carts > 0 & trans == 0 & score >= 5`).
- [x] Lưu trữ kết quả ra Parquet, ghi metrics JSON và phục hồi thành công qua Checkpoint khi restart.

### 9.2. Xử lý sự cố thường gặp
* **Lỗi `no configuration file provided`:** Kiểm tra đang đứng đúng tại thư mục `Part_B/` trước khi gõ các lệnh `docker compose`.
* **Lỗi `NoBrokersAvailable` khi chạy `producer.py`:** Đợi container Kafka chuyển sang trạng thái `healthy` (khoảng 15–20 giây sau khi `docker compose up -d`).
* **Lỗi `CSV not found`:** Đảm bảo tệp `events.csv` đã được giải nén và nằm tại `retailrocket/events.csv` hoặc `Part_B/retailrocket/events.csv`.
* **Lỗi cổng `9092` hoặc `4040` bị chiếm:** Tắt các ứng dụng chạy ngầm đang chiếm cổng trước khi bật Docker Compose.

---

## 10. Dừng hệ thống

Sau khi hoàn tất phiên làm việc hoặc demo:
```powershell
docker compose down
```
*(Nếu muốn xóa sạch trạng thái để chạy lại từ đầu: xóa các thư mục `Part_B/checkpoints/` và `Part_B/output/`).*

---

## 11. Danh mục tài liệu tham chiếu

* **Báo cáo kỹ thuật hoàn chỉnh:** [REPORT.md](REPORT.md) (Toàn bộ lý thuyết Part A, trích dẫn học thuật ACM SIGMOD 2018, thiết kế kiến trúc Part B và kịch bản thuyết trình 10–15 phút).
* **Từ điển dữ liệu & Khám phá EDA:** [DATA_DICTIONARY_AND_EDA.md](DATA_DICTIONARY_AND_EDA.md) | Notebook: [DATA_DICTIONARY_AND_EDA.ipynb](DATA_DICTIONARY_AND_EDA.ipynb).
* **Bóc tách yêu cầu & Barem điểm:** [LAM_MIN_YEU_CAU_DE_TAI.md](LAM_MIN_YEU_CAU_DE_TAI.md).
* **Hướng dẫn chi tiết Part A & Part B:** [Part_A/README.md](Part_A/README.md) và [Part_B/README.md](Part_B/README.md).
