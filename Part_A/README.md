# PHẦN A: NỀN TẢNG LÝ THUYẾT & NGHIÊN CỨU HỌC THUẬT (30 ĐIỂM)

> **Mục tiêu:** Tổng hợp toàn bộ nghiên cứu lý thuyết, phân tích kiến trúc chuyên sâu và bài báo khoa học về Spark Streaming, Recommender Systems và Kafka Ingestion theo yêu cầu của Đề bài.

---

## 1. MỤC LỤC & ĐIỀU HƯỚNG NỘI DUNG

Các nội dung nghiên cứu lý thuyết được chia thành 3 chủ đề tương ứng với 3 đề mục lớn trong đề tài:

```
Part_A/
├── README.md                                    # Mục lục & tổng quan Part A
├── A1_Spark_Architecture/                       # Đề mục A.1: Kiến trúc Spark Streaming & ETL
│   ├── A1_1_DStream_vs_Structured_Streaming.ipynb
│   ├── A1_1_DStream_vs_Structured_Streaming.html
│   └── A1_2_ETL_Data_Architecture.ipynb
├── A2_Recommender_Systems/                      # Đề mục A.2: Hệ thống gợi ý & Live Intent Scoring
│   └── A2_Recommender_Systems_Spark_Streaming.ipynb
└── A3_Ingestion_Kafka_vs_Flume/                 # Đề mục A.3: Thu nhận phân tán Kafka vs Flume
    └── A3_Kafka_vs_Flume.ipynb
```

---

## 2. TÓM TẮT CÁC CHỦ ĐỀ NGHIÊN CỨU

### 🔷 Chủ đề A.1: Kiến trúc Spark Streaming & ETL Data Architecture
* **Thư mục:** [`A1_Spark_Architecture/`](A1_Spark_Architecture/)
* **Nội dung chính:**
  1. **DStream vs. Structured Streaming:**
     * File: [`A1_1_DStream_vs_Structured_Streaming.ipynb`](A1_Spark_Architecture/A1_1_DStream_vs_Structured_Streaming.ipynb) ([Bản HTML](A1_Spark_Architecture/A1_1_DStream_vs_Structured_Streaming.html)).
     * So sánh mô hình RDD micro-batching cổ điển với mô hình Bảng vô hạn (Unbounded Table).
     * Phân tích cơ chế xử lý Event-Time, Watermarking và bộ tối ưu hóa Catalyst Optimizer + Tungsten Engine.
  2. **ETL Data Architecture in Spark Streaming:**
     * File: [`A1_2_ETL_Data_Architecture.ipynb`](A1_Spark_Architecture/A1_2_ETL_Data_Architecture.ipynb).
     * Phân tích vai trò của Continuous ETL trong kỷ nguyên Big Data.
     * Trình bày cơ chế đảm bảo chất lượng luồng: Schema Enforcement, Stateful Transformations, Fault Tolerance qua Write-Ahead Logs (WAL) và Checkpointing.
     * Ngữ nghĩa tin cậy đầu cuối: End-to-End Exactly-Once Semantics.
     * **Trích dẫn nghiên cứu khoa học:** Tóm tắt paper gốc về Structured Streaming công bố tại ACM SIGMOD 2018 (*"Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark"*).

---

### 🔶 Chủ đề A.2: Hệ thống gợi ý với Spark Streaming (Recommender Systems)
* **Thư mục:** [`A2_Recommender_Systems/`](A2_Recommender_Systems/)
* **File nghiên cứu:** [`A2_Recommender_Systems_Spark_Streaming.ipynb`](A2_Recommender_Systems/A2_Recommender_Systems_Spark_Streaming.ipynb)
* **Nội dung chính:**
  * So sánh cơ chế gợi ý thời gian thực (Streaming Recommendations) so với gợi ý theo lô (Batch Recommendations).
  * Cách tiếp nhận luồng clickstream (`view`, `addtocart`) trong PySpark Structured Streaming.
  * Thuật toán tính điểm ý định mua sắm tức thì (**Live Intent Score**) trong cửa sổ trượt (Sliding Window) để kích hoạt gợi ý sản phẩm trước khi người dùng thoát phiên.

---

### 🟢 Chủ đề A.3: Thu nhận phân tán: Apache Kafka vs. Apache Flume
* **Thư mục:** [`A3_Ingestion_Kafka_vs_Flume/`](A3_Ingestion_Kafka_vs_Flume/)
* **File nghiên cứu:** [`A3_Kafka_vs_Flume.ipynb`](A3_Ingestion_Kafka_vs_Flume/A3_Kafka_vs_Flume.ipynb)
* **Nội dung chính:**
  * **Apache Kafka Deep-Dive:** Định nghĩa và cơ chế hoạt động của Broker, Topic, Partition, Consumer Group, và kiến trúc đồng thuận **KRaft** (loại bỏ Zookeeper).
  * Ưu và nhược điểm của Kafka khi làm message broker cho Big Data.
  * Cơ chế kết nối với PySpark thông qua `spark-sql-kafka-0-10`.
  * **Bảng ma trận so sánh chi tiết Kafka vs. Apache Flume:** Kiến trúc, cơ chế Push vs. Pull, khả năng mở rộng, độ trễ, và ngữ cảnh ứng dụng phù hợp.
