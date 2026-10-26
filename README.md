# Retail Rocket — Kafka & PySpark Structured Streaming

Theo dõi ý định mua theo cửa sổ trượt và gợi ý khách quay lại sản phẩm trong giỏ chưa mua.

Dự án được phân chia thành 2 phần độc lập tương ứng với cấu trúc yêu cầu của đề tài:
* [Part_A/](Part_A/README.md): **Nền tảng lý thuyết & Nghiên cứu học thuật (30 điểm)** — Kiến trúc Spark Streaming, Recommender Systems, Kafka vs. Flume và tóm tắt paper.
* [Part_B/](Part_B/README.md): **Hiện thực thực hành & Live Pipeline (70 điểm)** — Mã nguồn `producer.py`, `streaming_etl.py`, `docker-compose.yml`, `requirements.txt`.
* [LAM_MIN_YEU_CAU_DE_TAI.md](LAM_MIN_YEU_CAU_DE_TAI.md): Bản làm mịn và bóc tách toàn bộ tiêu chí kỹ thuật, nghiệp vụ và barem điểm 100đ từ đề bài.
* [KE_HOACH_PHAN_CONG_NHOM.md](KE_HOACH_PHAN_CONG_NHOM.md): Kế hoạch phân công nhiệm vụ chi tiết cho 6 thành viên trong nhóm.

**Chạy lần đầu:** làm theo [phần 1](#1-chuẩn-bị), sau đó [demo ở phần 2](#2-demo-trực-tiếp). Mọi thao tác chạy kỹ thuật được thực hiện trong thư mục **`Part_B/`**.

---

## 0. Cấu trúc thư mục dự án

```text
Kafka_and_Spark_streaming_subject/
│
├── Part_A/                                # PHẦN A: NỀN TẢNG LÝ THUYẾT & HỌC THUẬT (30 ĐIỂM)
│   ├── README.md                          # Tổng quan và mục lục Part A
│   ├── A1_Spark_Architecture/             # Đề mục A.1: Kiến trúc Spark Streaming & ETL
│   │   ├── A1_1_DStream_vs_Structured_Streaming.ipynb
│   │   ├── A1_1_DStream_vs_Structured_Streaming.html
│   │   └── A1_2_ETL_Data_Architecture.ipynb
│   ├── A2_Recommender_Systems/            # Đề mục A.2: Hệ thống gợi ý & Live Intent Scoring
│   │   └── A2_Recommender_Systems_Spark_Streaming.ipynb
│   └── A3_Ingestion_Kafka_vs_Flume/       # Đề mục A.3: Thu nhận phân tán Kafka vs. Flume
│       └── A3_Kafka_vs_Flume.ipynb
│
├── Part_B/                                # PHẦN B: HIỆN THỰC THỰC HÀNH & PIPELINE (70 ĐIỂM)
│   ├── README.md                          # Hướng dẫn chạy chi tiết Part B
│   ├── docker-compose.yml                 # Dựng cụm Kafka KRaft & Spark Structured Streaming
│   ├── producer.py                        # Bộ phát sinh sự kiện từ events.csv vào Kafka
│   ├── streaming_etl.py                   # PySpark Streaming ETL (Window 10m, Watermark 5m, Recommender)
│   ├── requirements.txt                   # Thư viện Python cho producer (kafka-python)
│   ├── output/                            # Kết quả Parquet và metric latency JSON (sinh khi chạy)
│   └── checkpoints/                       # Lưu trữ checkpoint phục hồi lỗi (sinh khi chạy)
│
├── retailrocket/                          # Thư mục chứa tập tin dữ liệu từ Kaggle
│   └── events.csv                         # (Tải về và đặt tại đây)
│
├── LAM_MIN_YEU_CAU_DE_TAI.md              # Bóc tách và làm mịn toàn bộ yêu cầu đề tài
├── KE_HOACH_PHAN_CONG_NHOM.md             # Kế hoạch và phân chia nhiệm vụ nhóm
└── README.md                              # Tài liệu hướng dẫn chung dự án
```

---

## 1. Chuẩn bị

### 1.1. Chuẩn bị và kiểm tra Docker Desktop

Giả định bạn đã cài Docker Desktop trên Windows. Mở ứng dụng và chờ engine hoạt động; dự án dùng Linux containers trong Docker Desktop.

| Cần có | Cách chuẩn bị |
| --- | --- |
| Docker Desktop đã cài | Mở ứng dụng và chờ engine sẵn sàng |
| Python | Cài [Python](https://www.python.org/downloads/), chọn nhánh **3.13** để dùng cùng nhánh đã kiểm thử producer. Bảo đảm gọi được `python` hoặc `py` từ PowerShell. |
| Git hoặc file ZIP của repository | Dùng Git để clone; hoặc dùng Download ZIP |
| Internet | Dùng khi tải dataset, image Docker, thư viện Python và Spark Kafka connector lần đầu |

Mở **PowerShell** và chạy:

```powershell
docker --version
docker compose version
docker info
```

### 1.2. Mở thư mục thực hành Part_B

Sau khi clone hoặc mở repository, di chuyển vào thư mục **`Part_B`**:

```powershell
cd Part_B
```

**Tất cả các lệnh chạy Docker và Python Producer bên dưới đều chạy tại thư mục `Part_B/`.**

### 1.3. Tải và đặt dataset

1. Mở [Retail Rocket E-Commerce Dataset trên Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset), tải dữ liệu.
2. Giải nén lấy file `events.csv`.
3. Đặt `events.csv` vào thư mục `retailrocket/` ở thư mục gốc của dự án (hoặc trong `Part_B/retailrocket/`).

Kiểm tra từ thư mục `Part_B/`:

```powershell
Test-Path ../retailrocket/events.csv
```

Kết quả cần là `True`. Nếu file nằm ở `retailrocket/retailrocket/events.csv`, hãy chuyển lên đúng một cấp như sơ đồ.

### 1.4. Tạo môi trường Python và cài thư viện

Chạy trong PowerShell tại thư mục dự án:

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip show kafka-python
.\.venv\Scripts\python.exe producer.py --help
```

Nếu máy chỉ nhận lệnh `py`, thay hai dòng đầu bằng `py --version` và `py -m venv .venv`. Các dòng dùng `.venv\Scripts\python.exe` giữ nguyên.

**Cần thấy:** `kafka-python` phiên bản `3.0.11` và danh sách tùy chọn của producer. Tạo `.venv` và gọi Python bằng đường dẫn trực tiếp giúp mỗi terminal dùng đúng thư viện; không cần kích hoạt môi trường hoặc đổi PowerShell execution policy. Tham khảo [Python venv](https://docs.python.org/3.13/library/venv.html).

Nếu bạn đã dùng Conda `bigdata` từ trước, vẫn có thể dùng môi trường đó sau khi cài `requirements.txt`; các lệnh bên dưới chọn `.venv` để người tải dự án lần đầu có thể chạy giống nhau.

### 1.5. Khởi động Kafka và Spark

Tại thư mục dự án, chạy:

```powershell
docker compose config --quiet
docker compose up -d
docker compose ps -a
docker compose logs --tail 50 spark
```

| Lệnh | Đang thực hiện điều gì? |
| --- | --- |
| `config --quiet` | Kiểm tra Compose đọc được cấu hình; không có output khi cấu hình hợp lệ |
| `up -d` | Bật Kafka, chạy `kafka-init` tạo topic, rồi chạy Spark ETL trong nền |
| `ps -a` | Xem các service đã chạy hoặc đã kết thúc |
| `logs --tail 50 spark` | Xem Spark có tải được connector và khởi chạy query hay không |

**Chưa gửi sự kiện cho tới khi các trạng thái sau đã đạt:**

| Service / log | Kết quả mong đợi |
| --- | --- |
| `kafka` | `Up ... (healthy)` |
| `kafka-init` | `Exited (0)` — đã hoàn tất việc chuẩn bị topic |
| `spark` | `Up ...` và có log `"kind": "query_started"` |

Lần đầu có thể mất vài phút để tải image/connector. `up -d` kết thúc chưa có nghĩa query Spark đã sẵn sàng; tiếp tục xem log nếu còn đang tải package. Nếu không thấy dòng khởi động trong 50 dòng cuối, dùng `docker compose logs spark` để xem toàn bộ. Không cần mở PySpark shell hay chạy thêm một bản `streaming_etl.py` bằng tay.

Dependency trong Compose bảo đảm Kafka healthy trước `kafka-init`, rồi đợi init thành công trước Spark. Tham khảo [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/).

| Thành phần | Phiên bản / địa chỉ |
| --- | --- |
| Kafka | `apache/kafka:4.3.1`, một broker kiêm controller KRaft |
| Spark | `apache/spark:4.2.0-python3`, `local[2]` |
| Connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` |
| Kafka từ Python trên máy | `127.0.0.1:9092` |
| Kafka từ container | `kafka:19092` |
| Spark UI khi ETL chạy | http://localhost:4040 |

## 2. Demo trực tiếp

Mục tiêu demo là theo dõi **hai khách đang truy cập cửa hàng**: A thêm giỏ rồi mua, B thêm giỏ nhưng chưa mua. Ta cần chứng minh hệ thống nhận được hành vi, tạo gợi ý đúng lúc và cập nhật khi khách đã mua.

Mở các terminal tại cùng thư mục dự án. Terminal A xem kết quả Spark; Terminal B phát sự kiện; Terminal C xem JSON trong Kafka nếu cần. Tất cả lệnh `docker compose` dùng cùng file `docker-compose.yml` tại đây.

### Bước 1 — Kiểm tra nơi tiếp nhận hành vi đã sẵn sàng

Sau khi chạy phần chuẩn bị, kiểm tra topic:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic retail-rocket-events
```

**Đang làm gì trong bài toán:** kiểm tra “hàng đợi sự kiện” mà mọi lượt xem, thêm giỏ và mua hàng sẽ đi vào. Lệnh này đọc thông tin topic, chưa tạo hành vi mua sắm.

**File/thành phần tham gia:** topic được service `kafka-init` trong `docker-compose.yml` tạo khi khởi động.

**Cần thấy:** `PartitionCount: 2` hoặc lớn hơn, cùng các dòng partition. Offset của mỗi partition là một dãy riêng. Hai partitions cho phép chia dữ liệu đầu vào cho các tác vụ đọc của Spark.

### Bước 2 — Mở màn hình theo dõi quyết định của hệ thống

Ở **Terminal A**:

```powershell
docker compose logs -f --tail 30 spark
```

**Đang làm gì trong bài toán:** mở màn hình quan sát bộ xử lý hành vi: khách nào đang xem, khách nào cần được gợi ý quay lại giỏ, khách nào đã mua. Lệnh chỉ theo dõi log; ứng dụng `streaming_etl.py` đã được Compose chạy trước đó.

**Cần thấy:** khi query mới khởi động có `"kind": "query_started"`. Sau khi nhận sự kiện sẽ có bảng `visitorid`, `itemid`, `intent_score`, `status`, `recommended_itemid` và log batch.

Nếu Spark đã chạy từ trước, dòng `query_started` có thể nằm ngoài 30 dòng cuối. Có thể tìm lại bằng lệnh dưới đây ở terminal khác; không cần đợi một dòng khởi động mới:

```powershell
docker compose logs spark | Select-String "query_started"
```

### Bước 3 — Mô phỏng khách thao tác trên cửa hàng

Mở **Terminal B** tại cùng thư mục dự án. Dùng Python của `.venv` đã tạo ở bước 1.4.

```powershell
.\.venv\Scripts\python.exe producer.py --demo --interval 5
```

**Đang làm gì trong bài toán:** thay cho website thật, `producer.py` phát các hành vi của hai khách vào Kafka. Script lấy item từ `retailrocket/events.csv`, tạo visitor giả mới mỗi lần chạy và dùng timestamp hiện tại. `--interval 5` tạo khoảng nghỉ 5 giây giữa các lần phát để dễ theo dõi; cả demo mất khoảng 25 giây, cộng thời gian kết nối/gửi.

**Luồng đang chạy:** `events.csv` → `producer.py` → JSON → topic Kafka. `streaming_etl.py` ở container đọc topic độc lập và xử lý theo micro-batch.

| Thứ tự gửi | Hành vi mô phỏng | Ý nghĩa trong bài toán | Kết quả mong đợi trong window liên quan |
| --- | --- | --- | --- |
| 1 | A: `view` item X | A quan tâm nhưng mới chỉ xem | Điểm 1, `browsing`, chưa gợi ý |
| 2 | A: `addtocart` item X | A có ý định mua cao, chưa thấy giao dịch | Điểm 6 nếu window có cả view/cart; `recommend`, gợi ý X |
| 3 | B: `view` item Y | Một khách khác xem một sản phẩm khác | Điểm 1, `browsing` |
| 4 | B: `addtocart` item Y | B có ý định mua nhưng chưa hoàn tất | Điểm 6 nếu window có cả view/cart; `recommend`, gợi ý Y |
| 5 | A: `transaction` item X | A đã mua X, không cần nhắc mua tiếp trong window đó | `purchased`, item gợi ý chuyển thành `NULL` |

Ở Terminal B, `Sent=... partition=... offset=...` chứng minh Kafka đã xác nhận nhận message. Dòng này chưa chứng minh Spark đã tính điểm; cần đối chiếu kết quả ở Terminal A.

### Bước 4 — Đọc kết quả gợi ý và giải thích quyết định

Quay lại **Terminal A**, theo dõi bảng mà `write_batch()` trong `streaming_etl.py` in ra:

| Cột/giá trị | Cách đọc trong bài toán |
| --- | --- |
| `visitorid` / `itemid` | Khách nào, sản phẩm nào đang được xét |
| `intent_score` | Mức tương tác: số lượt xem + 5 × số lượt thêm giỏ |
| `status=browsing` | Chưa có điều kiện gợi ý; ví dụ mới xem |
| `status=recommend` | Có thêm giỏ và chưa thấy mua trong window; gợi ý quay lại item đó |
| `recommended_itemid` | Item được gợi ý; trong baseline này là chính item đang trong giỏ |
| `status=purchased` và item gợi ý `NULL` | Đã thấy giao dịch; cập nhật để tắt gợi ý của window tương ứng |
| `batch_id` / `window_start` | Bản cập nhật thuộc lần xử lý nào và cửa sổ thời gian nào |

**Kết luận cần chứng minh:** A từng được gợi ý rồi chuyển sang đã mua; B có gợi ý khi chưa mua. Hệ thống đưa ra quyết định từ hành vi đã quan sát, chưa thể biết B có mua trong tương lai hay không.

Một số điểm để đọc đúng kết quả:

- Spark gom nhiều sự kiện vào một micro-batch, nên không nhất thiết có một bảng cho mỗi dòng `Sent`. Nếu cart và transaction cùng batch, bạn có thể thấy ngay `purchased` mà không thấy bảng trung gian `recommend` của A.
- Window dài 5 phút, trượt mỗi phút, nên một sự kiện có thể góp vào 5 hàng window. Nếu sự kiện ở hai phía của một mốc phút, có window chỉ chứa cart và đạt điểm 5, có window chứa cả view/cart và đạt điểm 6.
- Transaction chỉ tắt gợi ý của các window chứa transaction đó. Một dòng lịch sử ở window khác không phải quyết định mới nhất cho toàn bộ khách hàng.
- Bảng in các hàng **vừa cập nhật**, tối đa 20 hàng; B không xuất hiện ở batch mua hàng của A không có nghĩa là gợi ý B đã bị tắt.
- Đây là mô phỏng hành vi, không phải lịch sử mua hàng thật. Baseline gợi ý quay lại giỏ, chưa tạo gợi ý sản phẩm liên quan bằng mô hình ML.

### Bước 5 — Xem dữ liệu trước khi Spark biến đổi

Ở **Terminal C** (tùy chọn):

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic retail-rocket-events --from-beginning
```

**Đang làm gì trong bài toán:** quan sát nguyên liệu đầu vào — các JSON hành vi mà website giả lập đã gửi. Công cụ console consumer đi kèm Kafka image, không phải file Python khác trong dự án.

**Cần thấy:** JSON có đủ `timestamp`, `visitorid`, `event`, `itemid`, `transactionid`. Đối chiếu visitor/item với bảng Spark ở bước 4 để theo dấu một hành vi từ đầu vào đến quyết định.

`--from-beginning` đọc cả dữ liệu cũ còn giữ trong topic, vì vậy có thể thấy nhiều hơn 5 message. Consumer này đọc độc lập với Spark; việc xem JSON không lấy mất dữ liệu của Spark.

### Bước 6 — Kiểm tra hệ thống đã lưu kết quả và ghi metrics

Ở terminal trống hoặc dừng theo dõi log bằng `Ctrl+C`, chạy:

```powershell
Get-ChildItem output/intent -Directory | Sort-Object Name | Select-Object -Last 3
Get-ChildItem output/metrics/*.json | Sort-Object Name | Select-Object -Last 1 | Get-Content
Get-ChildItem checkpoints/retail-intent-v1
```

**Đang làm gì trong bài toán:** kiểm tra quyết định đã được lưu để xem lại, đo được tốc độ xử lý và có trạng thái để tiếp tục nếu bộ xử lý dừng.

| Bằng chứng | Ý nghĩa |
| --- | --- |
| Thư mục `output/intent/batch_.../` | `write_batch()` lưu kết quả window/điểm/gợi ý thành Parquet; lệnh trên chỉ liệt kê thư mục, chưa đọc nội dung Parquet |
| `input_records` trong metrics | Số message Kafka mà batch đọc; không phải số khách hay số dòng window |
| `trigger_duration_ms` | Thời gian xử lý batch, đơn vị mili giây; không phải tuổi của dữ liệu lịch sử |
| `watermark`, `state_rows`, `late_rows_dropped` | Tiến độ event time, lượng state còn giữ và số hàng bị loại do watermark; hàng bị loại có thể là hàng window sau khi một event được mở rộng |
| `sources` | Offset đầu/cuối và thông tin Kafka lag để xem Spark đã đọc tới đâu |
| `checkpoints/retail-intent-v1/` | Spark ghi vị trí đọc, commit và state phục vụ recovery; không phải nơi lưu báo cáo hay file CSV |

Metrics mới nhất có thể là batch không có dữ liệu, nên `input_records=0` là bình thường. Xem batch trước nếu muốn đối chiếu batch vừa nhận hành vi. Log `active_recommendation_rows` cũng chỉ đếm các hàng active trong phần cập nhật của batch, không phải tổng khách còn giỏ trên toàn hệ thống.

### Bước 7 — Thử tình huống bộ xử lý tạm ngừng

Thực hiện theo [mục 4.1](#41-demo-khôi-phục-khi-spark-tạm-dừng). Mục tiêu là chứng minh hành vi vẫn được Kafka tiếp nhận và Spark đọc tiếp khi hoạt động trở lại.

`Ctrl+C` ở lệnh xem log hoặc console consumer chỉ dừng màn hình theo dõi đó. Muốn mô phỏng bộ xử lý ngừng hoạt động cần dùng `docker compose stop spark`.

**Demo cơ bản thành công khi:** producer báo đã gửi 5 message; Spark in các hàng `recommend` cho khách thêm giỏ chưa mua và `purchased` cho khách A; có Parquet, metrics JSON và checkpoint. Xem `visitorid` ở output producer để nhận diện đúng hai khách của lần demo mới nhất.

## 3. Phát dữ liệu CSV

Sau demo ngắn, có thể thử 1.000 dòng thật từ CSV với timestamp được đổi thành thời điểm phát để mô phỏng clickstream hiện tại.

```powershell
.\.venv\Scripts\python.exe producer.py --limit 1000 --interval 0.1 --replay-clock
```

`--replay-clock` giữ nguyên visitor, item, event và transaction ID. Bỏ `--limit` để đọc hết file; dùng `Ctrl+C` ở terminal producer nếu muốn dừng phát sớm. Khoảng nghỉ `0.1` là 0,1 giây; 1.000 dòng cần khoảng 100 giây cộng thời gian gửi/xác nhận.

Nếu muốn giữ **timestamp gốc**, bỏ `--replay-clock`. Lưu ý CSV lịch sử không được sắp tăng dần hoàn toàn theo timestamp. Với watermark 2 phút, phát nguyên thứ tự file có thể khiến sự kiện bị coi là quá trễ. Đặc biệt, sau demo dùng thời gian hiện tại, timestamp năm 2015 sẽ đứng sau watermark của query đó. Để phân tích lịch sử cần một bộ topic/checkpoint/output riêng và chiến lược thứ tự thời gian phù hợp; không dùng chế độ đó để kiểm tra demo hiện tại.

## 4. Kết quả và khôi phục

| Đường dẫn trên máy | Nội dung |
| --- | --- |
| `output/intent/batch_000.../` | Các hàng window được cập nhật, lưu Parquet; gồm điểm, status và item gợi ý |
| `output/metrics/batch_000....json` | Input records, duration, watermark, state, offsets và Kafka lag |
| `checkpoints/retail-intent-v1/` | Offset, commit và aggregate state để khôi phục |
| Docker volume `kafka-events` của Compose | Dữ liệu Kafka bền qua việc tạo lại container |

Lệnh PowerShell xem metrics nằm ở bước 6 của phần 2.

### 4.1. Demo khôi phục khi Spark tạm dừng

**Tình huống nghiệp vụ:** dịch vụ tính ý định tạm dừng để bảo trì nhưng khách vẫn xem hàng, thêm giỏ và mua. Kafka cần giữ các hành vi đó để Spark xử lý khi hoạt động trở lại.

Chạy từng thao tác theo thứ tự, có thể dùng Terminal B sau khi producer demo trước đã kết thúc.

**Thao tác 1 — dừng bộ xử lý hành vi:**

```powershell
docker compose stop spark
```

Chỉ container Spark dừng; Kafka vẫn chạy. `streaming_etl.py` không tạo quyết định mới trong lúc này. Các batch đã commit vẫn có kết quả và checkpoint trên máy.

**Thao tác 2 — khách tiếp tục thao tác khi Spark đang dừng:**

```powershell
.\.venv\Scripts\python.exe producer.py --demo --interval 1
```

Producer phát 5 hành vi và vẫn nhận xác nhận partition/offset từ Kafka. Đây là bằng chứng luồng tiếp nhận sự kiện hoạt động độc lập với Spark. Trong lúc đó chưa có kết quả tính điểm mới từ Spark.

**Thao tác 3 — cho bộ xử lý hoạt động trở lại:**

```powershell
docker compose start spark
docker compose logs -f --tail 30 spark
```

Compose chạy lại `streaming_etl.py`. Query đọc checkpoint để khôi phục các bộ đếm và tiếp tục từ offset cũ, rồi nhận các message đã tích lại trong Kafka.

**Cần quan sát:**

- `query_started` có `query_id` giống trước khi dừng, `run_id` mới.
- `batch_id` tiếp tục tăng; source offset tiếp tục từ vị trí đã lưu.
- Batch mới xử lý các message được phát khi Spark dừng. Nếu không có nguồn phát nào khác, tổng input records của các batch đọc phần dữ liệu mới là 5.
- Hai khách mô phỏng mới có kết quả tương ứng: A đã mua, B còn gợi ý. Do cả 5 sự kiện đã nằm trong Kafka trước khi khởi động lại, chúng có thể cùng một batch; lúc này không cần thấy trạng thái trung gian của A.

**Điều demo chứng minh:** producer có thể tiếp tục gửi khi Spark dừng; Kafka giữ dữ liệu; Spark dùng checkpoint để khôi phục và xử lý tiếp. Không xóa checkpoint, output hoặc volume giữa ba thao tác.

### 4.2. Cách hiểu kết quả lưu và quản lý phiên chạy

Không dùng chung checkpoint cho hai query chạy đồng thời. Nếu thay schema/state/topic, cần bộ checkpoint **và output mới**.

Parquet chứa lịch sử **các bản cập nhật**, không phải một bảng snapshot duy nhất. Khi đọc lịch sử, lấy batch lớn nhất theo `(window_start, window_end, visitorid, itemid)` trước khi lọc `recommendation_active`. Các cửa sổ cũ có thể lưu gợi ý trước giao dịch; không diễn giải chúng thành gợi ý hiện tại. Window hết hạn cần được loại khỏi màn hình phục vụ trực tiếp; bản demo dùng console kết quả cập nhật.

Dừng producer bằng `Ctrl+C` nếu nó còn đang phát, rồi dừng môi trường:

```powershell
docker compose stop
```

Khi muốn demo lần nữa, mở Docker và chạy tại thư mục dự án:

```powershell
docker compose up -d
```

Sau đó làm lại phần 2; không cần clone, tải dataset hay cài thư viện lại nếu những file đó vẫn còn. Demo tạo visitor mới mỗi lần chạy. Giữ volume Kafka, thư mục output và checkpoints để tiếp tục phiên; không xóa chúng trong khi thử recovery.

## 5. Checklist xác nhận demo thành công

Có thể kiểm chứng trực tiếp bằng các lệnh và kết quả ở phần 2, không cần tải thêm bộ test hay tài liệu khác.

| Cần chứng minh | Quan sát ở đâu? | Kết quả mong đợi |
| --- | --- | --- |
| Hạ tầng chạy được | `docker compose ps -a` | Kafka healthy, init exit 0, Spark đang chạy |
| Topic có khả năng chia luồng đọc | Lệnh describe topic ở bước 1 | Ít nhất 2 partitions |
| Producer gửi JSON vào Kafka | Terminal B; có thể đối chiếu console consumer ở Terminal C | Demo gửi thành công 5 message; JSON đủ 5 trường |
| Tính ý định và gợi ý | Bảng ở Terminal A | Cart chưa mua có `recommend`; có giao dịch thì window tương ứng chuyển `purchased` |
| Lưu kết quả | `output/intent/batch_.../` | Có file Parquet sau batch có kết quả |
| Theo dõi từng batch | `output/metrics/batch_....json` | Có `input_records`, `trigger_duration_ms`, watermark và offsets |
| Có checkpoint | `checkpoints/retail-intent-v1/` | Có metadata/offsets/commits/state được Spark sinh |
| Đọc tiếp sau khi dừng | Thực hiện demo recovery ở mục 4.1 | Message gửi lúc Spark dừng được xử lý khi khởi động lại; query ID giữ nguyên |

Nhận diện hai visitor mới theo log producer của lần chạy hiện tại, vì topic/output có thể chứa dữ liệu các lần demo cũ. Số message không nhất thiết bằng số hàng hiển thị: Spark tổng hợp theo window và chỉ in tối đa 20 hàng cập nhật mỗi batch.

Cấu hình đã được chạy trên Windows với Docker Desktop/WSL2 và Python 3.13.

## 6. Vai trò các file và thư mục

**Các file có sẵn trên repository:**

| File | Vai trò |
| --- | --- |
| `README.md` | Hướng dẫn cài môi trường, chạy demo, đọc kết quả và xử lý lỗi |
| [docker-compose.yml](docker-compose.yml) | Khai báo Kafka KRaft, service chuẩn bị topic và Spark chạy ETL |
| [requirements.txt](requirements.txt) | Kafka client cần cài cho producer trên máy |
| [producer.py](producer.py) | Đọc CSV hoặc phát demo, gửi hành vi khách hàng vào Kafka |
| [streaming_etl.py](streaming_etl.py) | Parse, window, watermark, tính điểm/gợi ý, ghi kết quả, metrics và checkpoint |

**Các thư mục được tạo khi làm theo hướng dẫn:**

| Thư mục | Hình thành khi nào? |
| --- | --- |
| `retailrocket/` | Bạn tạo và chép `events.csv` từ Kaggle ở bước 1.3 |
| `.venv/` | Python tạo môi trường và thư viện ở bước 1.4 |
| `output/` | Spark ghi kết quả Parquet và metrics khi chạy |
| `checkpoints/` | Spark ghi tiến độ và state khi chạy |

Các thư mục này không phải mã nguồn bổ sung cần tìm trên repo. Riêng dữ liệu Kafka được Docker giữ trong named volume `kafka-events` của dự án Compose.

## 7. Xử lý lỗi thường gặp

| Dấu hiệu | Cách xử lý |
| --- | --- |
| `docker` hoặc `docker compose` không được nhận diện | Mở Docker Desktop, sau đó mở PowerShell mới và chạy lại các lệnh kiểm tra ở bước 1.1. |
| `docker info` báo không kết nối được engine hoặc lỗi pipe | Mở Docker Desktop và chờ engine hoạt động. Kiểm tra Docker đang dùng Linux containers. Chỉ chạy tiếp khi `docker info` có phần Server. |
| `no configuration file provided` | Terminal đang sai thư mục; chuyển tới nơi chứa `docker-compose.yml`. Mỗi terminal A/B/C đều phải mở tại đây. |
| `python` không được nhận diện trên Windows | Thử `py --version`, dùng `py -m venv .venv`. Nếu cả hai đều không có, cài Python rồi mở lại terminal. |
| Không tìm thấy `.venv\Scripts\python.exe` | Tạo `.venv` ở bước 1.4 và kiểm tra PowerShell đang đứng đúng thư mục dự án. |
| `ModuleNotFoundError: kafka` hoặc lỗi import serializer | Cài lại `requirements.txt` bằng chính Python `.venv` đang chạy producer. Kiểm tra `pip show kafka-python` ra `3.0.11`; dùng đường dẫn Python đầy đủ ở bước 1.4. |
| `CSV not found` | Kiểm tra file đã giải nén và có đúng đường dẫn `retailrocket/events.csv`; tránh đặt thêm một thư mục `retailrocket` lồng bên trong. |
| `NoBrokersAvailable`, gửi message timeout | Kiểm tra Docker đang chạy, Kafka healthy và topic có sẵn theo bước 1.5/phần 2. Producer trên máy dùng `127.0.0.1:9092`, Spark trong container dùng `kafka:19092`. |
| Cổng `9092` hoặc `4040` đã được sử dụng | Dừng dịch vụ cũ đang chiếm cổng của bạn rồi chạy lại Compose. Với cấu hình hiện tại, các địa chỉ trong code và Compose phải khớp nhau. |
| `kafka-init` kết thúc với `Exited (0)` | Bình thường: service này chỉ chuẩn bị topic rồi kết thúc. Nếu exit code khác 0, xem `docker compose logs kafka-init`. |
| Spark `Exited` hoặc không có `query_started` | Xem `docker compose logs --tail 100 spark`. Nếu lỗi tải package/image, kiểm tra mạng/proxy và thử lại `docker compose up -d`. Nếu Docker báo thiếu bộ nhớ, tăng tài nguyên dành cho Docker rồi chạy lại. |
| Producer có `Sent=...` nhưng chưa thấy bảng kết quả | Chờ Spark hoàn tất batch và xem log; `Sent` là xác nhận từ Kafka. Nếu đang phát timestamp gốc sau demo hiện tại, dùng `--replay-clock` như phần 3. |
| Không thấy A ở trạng thái `recommend`, chỉ thấy `purchased` | Cart và transaction có thể cùng batch. Quan sát khách B chưa mua; nếu cần quan sát từng bước rõ hơn, chạy demo mới với `--interval 10` và chờ các batch hoàn tất. |
| Metrics mới nhất có `input_records=0` | Có thể là batch không có sự kiện mới; xem các file batch trước. |

Nếu đã sửa nguyên nhân nhưng Spark vẫn đang dừng, chạy `docker compose up -d` rồi xem lại log. Không xóa checkpoint hoặc volume Kafka chỉ để thử lại lỗi kết nối.
