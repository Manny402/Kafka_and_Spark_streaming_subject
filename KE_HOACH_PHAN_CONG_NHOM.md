# KẾ HOẠCH PHÂN CÔNG DỰ ÁN & ĐỀ XUẤT CÔNG VIỆC (NHÓM 6 NGƯỜI)
## Đề tài: Real-Time Streaming with Kafka & PySpark Structured Streaming
**Bộ dữ liệu:** Retail Rocket E-Commerce Events Dataset (`events.csv`)  
**Mục tiêu:** Xây dựng hệ thống pipeline xử lý dữ liệu luồng theo thời gian thực, tính toán điểm ý định mua hàng (Intent Score) và kích hoạt gợi ý sản phẩm tức thì trước khi phiên duyệt web kết thúc.

---

## 1. TỔNG QUAN YÊU CẦU ĐỀ BÀI & CƠ CẤU ĐIỂM (100 ĐIỂM)

| Hạng mục | Nội dung chính | Điểm số |
| :--- | :--- | :---: |
| **1. Theoretical Research & Literature** | So sánh Spark DStream vs Structured Streaming; Kiến trúc ETL Streaming; Kafka vs Flume; Tóm tắt 1 paper nghiên cứu tối ưu ETL streaming. | **30đ** |
| **2. Kafka Setup & Event Producer** | Dựng cụm Kafka (KRaft mode, $\ge 2$ partitions); Viết `producer.py` đẩy dữ liệu từ `events.csv` với định dạng JSON và điều tiết tốc độ. | **20đ** |
| **3. PySpark Streaming & Intent Logic** | Viết `streaming_etl.py`: parse JSON, watermark 5 phút + sliding window 10 phút, tính điểm Intent Score (`view`=1, `cart`=5, `trans`=10), lọc và trigger gợi ý. | **25đ** |
| **4. Logging & Fault Tolerance** | Micro-batch logging qua `.foreachBatch()`, Console sink và Parquet/Delta Lake sink kèm `checkpointLocation`. | **15đ** |
| **5. Code Modularity & Live Demo** | Mã nguồn chuẩn mực, tài liệu `REPORT.md`, slide thuyết trình và Live Demo 10–15 phút trên lớp. | **10đ** |

---

## 2. PHÂN CHIA NHIỆM VỤ CHI TIẾT (3 CẶP - 6 THÀNH VIÊN)

Mỗi thành viên đều đảm nhiệm **cả 2 phần: Lý thuyết (Part A) và Lập trình thực hành (Part B)** để đảm bảo công bằng và đánh giá toàn diện.

```
                              ┌───────────────────────────────────┐
                              │     events.csv (Retail Rocket)    │
                              └─────────────────┬─────────────────┘
                                                │
                     ┌──────────────────────────▼──────────────────────────┐
                     │    CẶP 1: Hạ tầng Docker & Kafka Event Producer     │
                     │    (Thành viên 1 & Thành viên 2)                    │
                     │    - docker-compose.yml (Kafka 4.3.1 KRaft)         │
                     │    - producer.py (Pacing, JSON schema)              │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                                                ▼  Kafka Topic: retail-rocket-events (2 partitions)
                     ┌─────────────────────────────────────────────────────┐
                     │    CẶP 2: Spark Streaming ETL Core & Trạng thái     │
                     │    (Nguyễn Phương Đông & Nguyễn Tuấn Phong)         │
                     │    - Part A: Notebook ETL & DStream vs Structured   │
                     │    - streaming_etl.py: Window, Watermark, Intent    │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                                                ▼  Processed Stream Data
                     ┌─────────────────────────────────────────────────────┐
                     │    CẶP 3: Recommender Engine, Logging & Báo cáo     │
                     │    (Thành viên 5 & Thành viên 6)                    │
                     │    - Logic gợi ý sản phẩm khi Intent Score >= 5     │
                     │    - .foreachBatch logging, Parquet Sink + Checkpoint│
                     │    - Tổng hợp REPORT.md, Slide & Kịch bản Demo      │
                     └─────────────────────────────────────────────────────┘
```

---

### 🔷 CẶP 1: Hạ tầng Docker & Kafka Event Producer (Thành viên 1 & 2)
> **Trọng tâm:** Xây dựng môi trường chạy ổn định và đảm bảo dữ liệu sự kiện được đẩy vào Kafka liên tục, đúng định dạng.

* **Phần Lý thuyết (Part A - 10đ):**
  - Nghiên cứu và viết báo cáo: **Real-Time Distributed Ingestion: Kafka vs. Apache Flume** (kiến trúc kênh truyền, so sánh ưu/nhược điểm, bảng ma trận so sánh chi tiết).
  - Trình bày kiến trúc nội bộ của Kafka: Broker, Topic, Partition, Consumer Group, cơ chế KRaft consensus (loại bỏ Zookeeper) và cơ chế kết nối qua thư viện `spark-sql-kafka-0-10`.
* **Phần Thực hành (Part B - 20đ):**
  - **Docker Setup:** Tối ưu hóa file [`docker-compose.yml`](docker-compose.yml), đảm bảo Kafka broker chạy ổn định ở cổng `localhost:9092` (host) và `kafka:19092` (container), tạo sẵn topic `retail-rocket-events` với tối thiểu 2 partitions.
  - **`producer.py`:** Tải file `events.csv` (Retail Rocket trên Kaggle), viết script đọc từng dòng (hoặc stream theo batch nhỏ), giả lập độ trễ thực tế (pacing: ví dụ 50–100 events/giây) và bắn message JSON chuẩn schema vào topic Kafka.

---

### 🔶 CẶP 2: Spark Streaming ETL Core & Stateful Transformation (Nguyễn Phương Đông & Nguyễn Tuấn Phong)
> **Trọng tâm:** Trái tim xử lý dữ liệu luồng, tiếp nhận stream từ Kafka và tính toán trạng thái động theo thời gian thực.

* **Phần Lý thuyết (Part A - 15đ):**
  - So sánh chi tiết **DStream vs. Structured Streaming** (Mô hình RDD micro-batching vs. Unbounded table, xử lý Event-time, Catalyst optimizer).
  - Hoàn thiện phần **ETL Data Architecture in Spark Streaming** và tóm tắt bài báo khoa học ACM SIGMOD 2018 (đã có bản mẫu tại [`Part_A/ETL_Data_Architecture_in_Spark_Streaming.ipynb`](Part_A/ETL_Data_Architecture_in_Spark_Streaming.ipynb)).
* **Phần Thực hành (Part B - 20đ):**
  - Viết phần lõi của script **`streaming_etl.py`**:
    - Kết nối và đọc luồng từ Kafka broker bằng `spark.readStream`.
    - Parse JSON payload với schema rõ ràng (`from_json`), chuyển đổi timestamp thành kiểu `TimestampType`.
    - Áp dụng **Sliding Window 10 phút** và **Watermark 5 phút** (`withWatermark("event_time", "5 minutes")`) để xử lý dữ liệu đến trễ.
    - Cài đặt công thức tính điểm **Intent Score** theo trọng số hành vi: `view` = 1, `addtocart` = 5, `transaction` = 10; tính tổng điểm tích lũy theo từng cặp `(visitorid, itemid)`.

---

### 🟢 CẶP 3: Recommender Engine, Logging/Sink & Báo cáo / Live Demo (Thành viên 5 & 6)
> **Trọng tâm:** Xử lý logic nghiệp vụ gợi ý, lưu trữ chịu lỗi (fault-tolerant sink), tổng hợp tài liệu và điều phối thuyết trình.

* **Phần Lý thuyết (Part A - 10đ):**
  - Nghiên cứu và viết mục: **Recommender Systems with Spark Streaming** (so sánh cơ chế gợi ý Batch vs. Streaming, cách phát hiện ý định tức thời trong phiên để kích hoạt gợi ý trước khi user rời website).
  - Giải thích cơ chế phục hồi lỗi (Fault Tolerance) và ngữ nghĩa **End-to-End Exactly-Once** ở phía Sink (Idempotent Sinks, Checkpointing).
* **Phần Thực hành (Part B - 15đ):**
  - **Recommendation Trigger:** Lọc các visitor có điểm `intent score >= 5` trong window (ví dụ: đã thêm vào giỏ nhưng chưa thanh toán) để sinh danh sách sản phẩm gợi ý tức thì.
  - **Structured Logging & Sinks:**
    - Cài đặt micro-batch logging bằng `.foreachBatch()`: in ra console Batch ID, số lượng records xử lý, thời gian thực thi / độ trễ (latency).
    - Cấu hình lưu trữ dữ liệu dạng **Parquet / Delta Lake** có phân vùng (partition) và bật `checkpointLocation`.
* **Phối hợp & Tổng kết (10đ):**
  - Tổng hợp toàn bộ lý thuyết, hình ảnh, sơ đồ kiến trúc vào file **`REPORT.md`**.
  - Thiết kế Slide thuyết trình (khoảng 15–20 slides ngắn gọn, trực quan).
  - Xây dựng kịch bản **Live Demo 10–15 phút** cho buổi báo cáo trên lớp.

---

## 3. LỘ TRÌNH THỰC HIỆN ĐỀ XUẤT (3 GIAI ĐOẠN)

| Giai đoạn | Thời gian | Mục tiêu & Sản phẩm bàn giao |
| :--- | :---: | :--- |
| **Giai đoạn 1: Chuẩn bị & Lý thuyết** | Ngày 1 – 2 | • Cặp 1: Chạy thử Docker Kafka, viết xong khung `producer.py`.<br>• Cặp 2: Merge nhánh `phong-etl`, hoàn thiện lý thuyết Spark ETL.<br>• Cặp 3: Soạn lý thuyết Recommender Systems và khung sườn `REPORT.md`. |
| **Giai đoạn 2: Lập trình Pipeline** | Ngày 3 – 4 | • Cặp 1: Producer bắn data JSON mượt mà vào Kafka topic `retail-rocket-events`.<br>• Cặp 2: `streaming_etl.py` đọc Kafka, parse schema, window 10m/watermark 5m, tính intent score.<br>• Cặp 3: Tích hợp logic trigger gợi ý, micro-batch logging, ghi file Parquet + Checkpoint. |
| **Giai đoạn 3: Kiểm thử & Live Demo** | Ngày 5 | • Chạy kiểm thử toàn diện toàn bộ pipeline từ đầu đến cuối (End-to-End Test).<br>• Tắt/bật lại container để kiểm tra checkpoint khôi phục dữ liệu.<br>• Hoàn thiện `REPORT.md`, thiết kế slide và tập dượt Demo 10–15 phút. |

---

## 4. QUY TẮC PHỐI HỢP TRÊN GIT (TRÁNH XUNG ĐỘT)

Để tránh tình trạng 6 người ghi đè code của nhau, nhóm thống nhất quy tắc làm việc trên Git như sau:

1. **Nhánh chính (`main`):** Chỉ chứa mã nguồn đã chạy ổn định và đã được kiểm thử.
2. **Các nhánh tính năng (Feature Branches):**
   - Cặp 1: `feature/kafka-producer` (quản lý `docker-compose.yml`, `producer.py`)
   - Cặp 2: `feature/spark-etl-core` (quản lý `streaming_etl.py`, `Part_A/`)
   - Cặp 3: `feature/recommender-sink-docs` (quản lý Sink, logging, `REPORT.md`, slides)
3. **Quy trình gộp mã nguồn (Merge):**
   - Mỗi cặp hoàn thành phần việc của mình thì đẩy lên remote branch và tạo **Pull Request (PR)** để các thành viên khác review trước khi merge vào `main`.

---

## 5. CÁC TÀI LIỆU & FILE BÀN GIAO CUỐI CÙNG

1. `REPORT.md`: Báo cáo kỹ thuật tổng thể (Lý thuyết Part A + Kiến trúc & Hướng dẫn Part B).
2. `docker-compose.yml`: File chạy cụm Kafka KRaft + Spark chỉ bằng 1 câu lệnh.
3. `producer.py`: Script đọc `events.csv` và stream dữ liệu vào Kafka.
4. `streaming_etl.py`: Ứng dụng PySpark Structured Streaming hoàn chỉnh.
5. `Part_A/ETL_Data_Architecture_in_Spark_Streaming.ipynb`: Notebook minh họa kiến trúc ETL.
6. Bộ Slide thuyết trình (PDF / PPTX) phục vụ báo cáo trước lớp.
