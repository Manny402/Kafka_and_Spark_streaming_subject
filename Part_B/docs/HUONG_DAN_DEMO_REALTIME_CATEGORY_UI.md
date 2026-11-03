# Hướng dẫn demo đúng giao diện Retail Rocket · Demo Control

Cập nhật 03/11/2026. Đối chiếu trực tiếp cấu hình Gradio tạo bởi [gradio_ui.py](../gradio_ui.py), callback trong [demo_controller.py](../demo_controller.py) và các bước trong [demo_scenarios.py](../demo_scenarios.py). Bản đối chiếu có **4 tab, 15 nút**; không dùng tên nút của các bản UI cũ.

## 1. Mở đúng bản UI

Mở Docker Desktop ở chế độ Linux containers. Từ **gốc dự án**, chạy:

```powershell
conda activate bigdata
cd Part_B
python gradio_ui.py
```

Nếu đã đứng trong `Part_B`, bỏ dòng `cd Part_B`. Nếu chưa cài dependency cho môi trường Python đang dùng, chạy `python -m pip install -r requirements.txt` một lần tại Part_B trước khi mở UI. Dataset cần đủ bốn CSV trong `retailrocket/` gốc repo.

Trình duyệt tự mở tại **http://127.0.0.1:7860**. Tiêu đề lớn phải là **Retail Rocket · Demo Control**. Cách khác là nhấp đúp `Part_B/run_demo.cmd`: launcher dùng `.venv` nếu có, nếu không dùng Python trong PATH; tái sử dụng bản Demo Control đang chạy và ghi log vào `Part_B/output/control-panel/launcher.log`.

Nếu trang đang mở có tên tab/nút khác danh mục ở mục 2, kiểm tra tiến trình UI đang chạy có phải bản code vừa cập nhật hay không. Sửa file Python hoặc nhấn F5 không nạp lại code vào tiến trình cũ: đóng tiến trình UI cũ rồi chạy lại một lần. Nếu UI mới nhất đã chạy, chỉ mở lại URL hoặc dùng launcher, tránh chạy thêm server trùng cổng.

## 2. Bản đồ chính xác các nút và tab

### 2.1. Thanh điều khiển phía trên — hiện ở mọi tab

Hai nút hàng đầu:

| Tên nút đúng trên UI | Cách dùng |
|---|---|
| **Chuẩn bị hệ thống** | Kiểm tra Docker/dataset, khởi động Kafka/catalog/Spark nếu cần và chờ các query sẵn sàng. Dùng ở nguồn demo chính, khi không có tác vụ đang chạy. |
| **Hủy thao tác** | Yêu cầu hủy tác vụ chạy nền. Nếu đang đứng giữa các bước kiểm chứng mà không có tác vụ chạy, nút này đưa về demo chính. Recovery sẽ mở lại Spark nếu chính kịch bản đã dừng nó. |

Bên dưới là trường **Chế độ phiên mới**, có hai lựa chọn **Tự động** và **Từng bước**; mặc định **Từng bước**. Tiếp theo là năm nút:

| Tên nút đúng trên UI | Cách dùng / điều kiện |
|---|---|
| **Phiên mới** | Tạo visitor mới, đặt lại bảng/bộ đếm của phiên. Cần hệ thống sẵn sàng, không chạy tác vụ và không đang ở kịch bản kiểm chứng. |
| **Tạm dừng** | Ngừng phát, giữ visitor và vị trí kịch bản/CSV. Cần phiên đang chạy và nguồn demo chính sẵn sàng. |
| **Tiếp tục** | Phát tiếp phiên tạm dừng; không tạo visitor mới. Chỉ mở khi phiên ở trạng thái tạm dừng. |
| **Kết thúc phiên** | Đóng phiên phát hiện tại, giữ dữ liệu/checkpoint. Khi đang kiểm chứng phải dùng Trở về demo chính trước. |
| **Bước tiếp theo** | Gửi một interaction của kịch bản nghiệp vụ. Chỉ mở khi runtime đang chạy ở chế độ Từng bước và kết quả bước trước đã được Spark công bố. |

**Đổi Chế độ phiên mới chưa đổi phiên đang chạy.** Chọn chế độ rồi bấm **Phiên mới** để áp dụng. Sau replay CSV, chọn Từng bước rồi bấm Phiên mới nếu muốn quay lại kịch bản U1/U2.

### 2.2. Bốn tab và các nút bên trong

| Tab đúng trên UI | Điều khiển / kết quả có trong tab |
|---|---|
| **Intent & Recommendation** | Checkbox **Xem thêm lịch sử các phiên**, **Theo dõi interaction mới nhất**; bảng **Interaction đã được Kafka ACK**; dropdown **Visitor**, **Window đang xem**; bảng **Intent do Spark tính**, **Top-K category**. Tab này không có nút bắt đầu riêng: dùng Phiên mới/Bước tiếp theo ở thanh trên. |
| **Kafka & CSV** | Hai ô **Số dòng CSV**, **Interval CSV (giây)**; nút **Phát CSV**, **Kiểm tra Kafka topic / partitions**; khung **Metadata Kafka thật**. |
| **Recovery & Watermark** | Bốn nút **Bắt đầu Recovery**, **Bắt đầu Watermark**, **Bước kiểm chứng tiếp theo**, **Trở về demo chính**; dòng trạng thái kịch bản; khung JSON **Kết quả đối chiếu trước/sau**. |
| **Logging & Storage** | Bảng **Micro-batch metrics thật**; nút **Đọc log Spark**, **Đọc lại Parquet**; khung log và hai khung JSON Parquet. |

Dưới các tab còn có hai vùng thu gọn:

- **Tiến độ và nhật ký điều khiển** → ô **Thao tác gần nhất**: tiến độ công việc, ACK, thông báo hoàn tất/lỗi.
- **Chi tiết kỹ thuật: JSON, query ID, run ID, nguồn dữ liệu** → hai khung **Nguồn dữ liệu đang xem**, **Trạng thái hệ thống và tác vụ**.

**Phân biệt hai nút bước:** `Bước tiếp theo` ở thanh trên gửi interaction nghiệp vụ; `Bước kiểm chứng tiếp theo` chỉ nằm trong tab Recovery & Watermark và điều khiển scenario. Các bước bên dưới luôn ghi đầy đủ tên nút, không dùng tên nút “Next” giả định.

## 3. Chuẩn bị hệ thống và bắt đầu trình bày

1. Bấm **Chuẩn bị hệ thống** ở thanh trên.
2. Theo dõi dòng trạng thái và **Tiến độ và nhật ký điều khiển**. Lần đầu có thể mất vài phút để đọc metadata/tải connector.
3. Chờ **Hệ thống sẵn sàng**; Kafka healthy, catalog sẵn sàng, Spark running. Nút Phiên mới được mở.
4. Nêu cấu hình: **window 10 phút, slide 5 phút, watermark 5 phút; threshold 5, top-K 3, policy extended; trọng số view/cart/transaction = 1/5/10**.

Không cần nhập lệnh Docker trong buổi demo. Một tác vụ chạy nền tại một thời điểm; bấm lặp không làm tác vụ hoàn tất nhanh hơn. Hai tab trình duyệt dùng chung producer và bộ điều khiển.

## 4. Demo nghiệp vụ trong Intent & Recommendation

### 4.1. Heavy browsing và nhánh cart bằng từng bước

1. Mở tab **Intent & Recommendation**.
2. Ở thanh trên chọn **Chế độ phiên mới → Từng bước**, rồi bấm **Phiên mới**.
3. Giữ checkbox **Theo dõi interaction mới nhất** được bật. Trong **Window đang xem**, giữ tùy chọn **Window của phiên / kịch bản**.
4. Bấm **Bước tiếp theo** từng lần, đợi nút mở lại và dòng “Đã có kết quả Spark cho lần gửi gần nhất”.

| Lần bấm Bước tiếp theo kể từ Phiên mới | Interaction | Kết quả trong Intent do Spark tính / Top-K category |
|---|---|---|
| 1–4 | U1 xem cùng item A | Views/score tăng 1 → 4; carts 0; chưa gợi ý. |
| 5 | U1 xem A lần thứ năm | Views 5, carts 0, score **5**, trạng thái recommend; lý do hiển thị **Quan tâm qua nhiều lượt xem**; xuất hiện top-3. |
| 6 | U1 mua A | Score **15**, purchased, top-K rỗng nếu không còn seed khác. |
| 7 | U2 xem B | Score **1**, browsing. |
| 8 | U2 thêm B vào giỏ | Score **6**, recommend; lý do **Đã thêm giỏ, chưa thấy thanh toán**. |
| 9 | U2 mua B | Score **16**, purchased, top-K được thu hồi. |

Checkbox theo dõi mới nhất tự chọn visitor vừa gửi event. Nếu tự chọn **Visitor**, UI tắt theo dõi; muốn tự chuyển sang U2 thì bật lại checkbox. **U1/U2 là vai trò trong kịch bản**, không phải tên lựa chọn literal trong dropdown: dropdown hiển thị ID số thực được tạo cho mỗi phiên.

Năm view phải cùng visitor/item/window. Trong một dòng Top-K category, chỉ vào **Item gợi ý**, **Category**, **Similarity lớn nhất**, **Score**, **Seed đóng góp**, **Nguồn**. Score là tổng `intent(seed) × similarity(seed_category, candidate_category)`; cột similarity chỉ là giá trị lớn nhất trong các seed đóng góp. Seed và các item visitor đã tương tác trong window bị loại khỏi candidate.

Lời dẫn: “View thứ năm làm score đạt 5 dù carts vẫn bằng 0. Sau mua, score tăng lên 15 nhưng gợi ý biến mất vì điều kiện chưa mua không còn đúng.” Chưa thấy transaction không đồng nghĩa đã bỏ giỏ hàng.

### 4.2. Phát tự động, nghỉ và chạy lại

- **Phát đều:** chọn Tự động rồi bấm Phiên mới. Event đầu sau khoảng 5 giây, các event tiếp theo nhắm nhịp 5 giây; batch Spark có thể gom nhiều event.
- **Nghỉ rồi giữ nguyên phiên:** Tạm dừng → Tiếp tục. Visitor và vị trí phát được giữ; auto chờ một interval trước lần gửi tiếp.
- **Chạy lại từ đầu:** Phiên mới. Không cần restart UI hay xóa checkpoint.
- **Đóng phiên:** Kết thúc phiên. Tiếp tục không mở lại một phiên đã kết thúc.

F5 chỉ tải lại trang, không hủy producer. Checkbox **Xem thêm lịch sử các phiên** mở thêm intent/recommendation đã lưu; nó không biến bảng interaction hiện tại thành toàn bộ lịch sử topic.

## 5. Demo Kafka và CSV

1. Mở tab **Kafka & CSV**.
2. Bấm **Kiểm tra Kafka topic / partitions**. Xem **Metadata Kafka thật**, mở `partitions` để chỉ hai partition.
3. Đặt **Số dòng CSV = 20**, **Interval CSV (giây) = 0.1**, bấm **Phát CSV**.
4. Chuyển tab **Intent & Recommendation** để xem bảng ACK. Không có bảng interaction riêng trong tab Kafka & CSV.
5. Mở vùng **Chi tiết kỹ thuật: JSON, query ID, run ID, nguồn dữ liệu** để xem event gần nhất ở **Nguồn dữ liệu đang xem**.

Replay dùng visitor/item/action từ `events.csv`, timestamp là thời gian phát. **Phát CSV kết thúc phiên trước**, nên muốn trở lại câu chuyện U1/U2 phải chọn chế độ rồi bấm Phiên mới. Lượt 20 dòng/0,1 giây kết thúc nhanh; nếu muốn thao tác Tạm dừng/Tiếp tục trên CSV, chọn 100 dòng/0,5 giây.

## 6. Demo Recovery — đúng thứ tự nút và nơi xem kết quả

1. Mở tab **Recovery & Watermark**.
2. Bấm **Bắt đầu Recovery**. Chương trình tạo phiên kiểm chứng mới, gửi sẵn bốn view và chờ score 4 được commit. Không tự bấm Bước tiếp theo bốn lần nữa.
3. Chờ trạng thái **Recovery · bước 1 · ready**. Sau đó dùng nút **Bước kiểm chứng tiếp theo trong chính tab này**:

| Lần bấm Bước kiểm chứng tiếp theo | Trạng thái phải chờ | Việc vừa thực hiện / bằng chứng |
|---|---|---|
| 1 | Recovery · bước 2 · ready | Spark đã dừng; Kafka còn hoạt động; score đã commit là 4. |
| 2 | Recovery · bước 3 · ready | View thứ năm được Kafka ACK khi Spark đang dừng; chưa có score 5. |
| 3 | Recovery · bước 4 · ready | Spark chạy lại cùng checkpoint; score **4 → 5**, carts 0, có top-K; Query ID giữ nguyên, Run ID đổi. |
| 4 | Recovery · bước 5 · complete | Transaction làm score **15** và thu hồi top-K. |

**Nơi xem bằng chứng:**

- Trong tab **Recovery & Watermark**, mở JSON **Kết quả đối chiếu trước/sau → observations**: score, query_id, previous_run_id, run_id, ACK/offset, recommendation.
- Muốn xem bảng score và top-K, **chuyển sang tab Intent & Recommendation**. Giữ Window của phiên / kịch bản. Các bảng đó không nằm trong tab Recovery & Watermark.
- Muốn xem metrics/log, chuyển sang tab **Logging & Storage**. Đổi tab không kết thúc scenario.
- Muốn chạy bước tiếp theo, **quay lại tab Recovery & Watermark** và bấm Bước kiểm chứng tiếp theo.

Trong lúc Spark bị dừng có chủ đích, thanh trạng thái có thể báo chưa sẵn sàng. Tiếp tục bằng nút kiểm chứng trong tab; không bấm Chuẩn bị hệ thống để mở Spark sớm. Các nút Phiên mới/Bước tiếp theo nghiệp vụ bị khóa khi scenario còn tồn tại.

Khi hoàn tất, tại tab **Recovery & Watermark** bấm **Trở về demo chính**. Trạng thái complete chưa tự thoát scenario. Recovery đã thay phiên live trước bằng phiên kiểm chứng mới; dùng Phiên mới nếu muốn kể lại câu chuyện nghiệp vụ. Hoàn tất trong window còn chứa chuỗi sự kiện; nếu chờ quá lâu, hủy và tạo kịch bản mới.

## 7. Demo Watermark — cùng trang, nguồn dữ liệu riêng

1. Nếu còn ở Recovery, tại tab **Recovery & Watermark** bấm **Trở về demo chính**, đợi xong.
2. Trong chính tab này bấm **Bắt đầu Watermark**. Chương trình tạm dừng live, tạo topic/container/output/checkpoint riêng và chuyển nguồn đang xem.
3. Chờ **Watermark · bước 0 · ready**. Nút bắt đầu mới chuẩn bị môi trường, chưa gửi bước đầu.
4. Bấm **Bước kiểm chứng tiếp theo** bốn lần, chờ từng trạng thái:

| Lần bấm | Trạng thái phải chờ | Kết quả mong đợi |
|---|---|---|
| 1 | Watermark · bước 1 · ready | Event mục tiêu tại T+1 phút và event đồng hồ T+3 phút được commit; score mục tiêu 1. |
| 2 | Watermark · bước 2 · ready | Event mục tiêu T+1 phút 30 giây đến sau event T+3 phút vẫn được tính; score **1 → 2**. |
| 3 | Watermark · bước 3 · ready | Event đồng hồ T+18 phút làm watermark tiến tới ít nhất T+13 phút; vượt hai window cũ kết thúc T+5/T+10 phút; state cũ được dọn. |
| 4 | Watermark · bước 4 · complete | Event mục tiêu T+1 phút bị loại khỏi các window đã hết hạn; late rows tăng, score vẫn **2**, không lên 3. |

`T` là mốc quá khứ do chương trình chọn, không phải thời gian hiện tại của đồng hồ UI.

**Nơi xem kết quả:**

- Tab **Recovery & Watermark**: JSON Kết quả đối chiếu trước/sau, các observations về event_time, max_event_time, watermark, old_window_end, state_rows_removed và late_rows.
- Tab **Intent & Recommendation**: visitor mục tiêu được scenario chọn; giữ tùy chọn **Window của phiên / kịch bản** trong Window đang xem để thấy score lịch sử 1 → 2. Không dùng Window mới nhất theo đồng hồ cho dữ liệu quá khứ này.
- Tab **Logging & Storage**: metrics và Parquet của nguồn kiểm chứng hiện tại. Thanh trên có **Nguồn đang xem: Watermark / kiểm chứng riêng**.

Watermark dọn state Spark, không xóa snapshot lịch sử; late rows có thể tính theo window chồng lấn, không tương đương số Kafka event duy nhất. Không diễn giải mọi event cũ hơn max event-time 5 phút đều chắc chắn bị loại khỏi mọi output.

Kết thúc: quay lại tab **Recovery & Watermark**, bấm **Trở về demo chính**. Nút này dừng container riêng và trả nguồn về **Demo chính**; không cần mở trang cổng 7861. Topic/checkpoint/artifact cũ vẫn được giữ. Tiếp tục chỉ dùng nếu phiên live đang tạm dừng; nếu đã kết thúc/hoàn tất thì tạo Phiên mới.

## 8. Log, Parquet, hủy và kết thúc

### 8.1. Đọc kết quả lưu trữ

Mở tab **Logging & Storage**:

| Điều khiển / khung | Cần làm / giải thích |
|---|---|
| **Micro-batch metrics thật** | Xem Query, Batch, Input records, Trigger ms, Watermark UTC, State rows, Late rows, State bytes. |
| **Đọc log Spark** | Bấm rồi xem ô **Log Spark / native ConsoleSink** ngay bên dưới. Đợi tác vụ xong; không phải log tự refresh liên tục. |
| **Đọc lại Parquet** | Bấm rồi xem thông báo đường dẫn và hai khung **Intent đọc trực tiếp từ Parquet**, **Recommendation đọc trực tiếp từ Parquet**. |

Các kết quả theo **nguồn đang xem**, có thể là live hoặc Watermark riêng. Muốn đọc live phải Trở về demo chính trước. Nếu chưa có batch commit, thao tác đọc Parquet báo chưa có dữ liệu thay vì tạo kết quả giả.

Input records thuộc từng query/batch; không cộng clean và intent thành số event duy nhất. Trigger ms là thời gian thực thi batch, không phải độ trễ end-to-end riêng của từng interaction. Kafka ACK và Spark commit là hai mốc khác nhau.

### 8.2. Hủy đúng thao tác

- Tác vụ đang chạy: bấm **Hủy thao tác** ở hàng trên. Chờ lệnh hiện tại kết thúc và bộ điều khiển xử lý rollback; không bấm dồn.
- Đang đứng giữa hai bước scenario: **Hủy thao tác** đưa về demo chính; nếu Recovery đang giữ Spark ở trạng thái dừng thì mở lại Spark.
- Muốn ngừng producer tự động/CSV: dùng **Tạm dừng** hoặc **Kết thúc phiên**. Hủy thao tác là nút cho tác vụ điều khiển/scenario, không thay cho nút dừng phiên phát.

### 8.3. Kết thúc buổi demo

Nếu còn scenario, mở tab Recovery & Watermark → **Trở về demo chính**. Sau đó bấm **Kết thúc phiên** ở thanh trên. Đóng trang không tự tắt server UI/Kafka/Spark. Nếu muốn tắt hẳn UI được mở bằng lệnh Python, Ctrl+C trong terminal đã khởi động nó; thao tác này thực hiện sau buổi trình diễn.

## 9. Vì sao nút bị mờ hoặc không có nút đang tìm?

| Hiện tượng | Nguyên nhân / thao tác đúng |
|---|---|
| Không tìm thấy Bắt đầu / Dừng của UI cũ | Bản này dùng **Phiên mới**, **Tạm dừng**, **Tiếp tục**, **Kết thúc phiên**. |
| Không tìm thấy nút Dừng Spark / Khởi động Spark độc lập | Các thao tác này nằm trong các bước Recovery, được chạy bằng **Bước kiểm chứng tiếp theo**. |
| Phiên mới, Phát CSV, Bắt đầu Recovery/Watermark bị mờ | Hệ thống chưa ready, có tác vụ đang chạy hoặc scenario chưa được thoát. Xem trạng thái; dùng Trở về demo chính khi cần. |
| Bước tiếp theo bị mờ dù chọn Từng bước | Chế độ chỉ áp dụng khi tạo Phiên mới; hoặc phiên đang tạm dừng/đã kết thúc, Spark chưa commit bước trước, hoặc đang ở scenario. |
| Tiếp tục bị mờ | Runtime không ở trạng thái tạm dừng. Muốn chạy lại phiên đã kết thúc, bấm Phiên mới. |
| Bước kiểm chứng tiếp theo bị mờ | Chưa bắt đầu scenario, tác vụ đang chạy, hoặc scenario đã complete/interrupted. Mở đúng tab và đọc trạng thái kịch bản. |
| Trở về demo chính bị mờ | Không có scenario hoặc tác vụ vẫn đang thực hiện. |
| Bấm kiểm tra topic/đọc log nhưng được báo đang bận | Các nút này vẫn có thể nhận click; bộ điều khiển yêu cầu đợi tác vụ hiện tại. Xem Tiến độ và nhật ký điều khiển. |
| Không thấy bảng intent trong tab Recovery & Watermark | Bảng đó ở **Intent & Recommendation**; đổi tab để xem, rồi quay về tab kiểm chứng để chạy bước tiếp. |
| Không thấy U2 | Bật Theo dõi interaction mới nhất hoặc chọn ID số của U2 trong Visitor. |
| Spark báo chưa sẵn sàng tại Recovery bước 2/3 | Trạng thái dừng có chủ đích; dùng Bước kiểm chứng tiếp theo, không chuẩn bị lại hệ thống giữa kịch bản. |

Lỗi Docker/dataset/query được ghi trên UI và trong nhật ký. Nếu checkpoint malformed hoặc metadata mismatch, giữ dữ liệu lỗi để truy vết; không xóa checkpoint đang cần chứng minh recovery.

## 10. Thứ tự trình bày và bằng chứng rubric

| Phần | Thao tác / kết quả trọng tâm | Rubric liên quan |
|---|---|---|
| Chuẩn bị và ingestion | Chuẩn bị hệ thống → Kafka & CSV; topic 2 partition, JSON và ACK/offset thật. | Kafka & Producer — 20 điểm |
| Intent/ranking | Phiên mới Từng bước; 4 → 5 views; top-K category; transaction clear; U2 nhánh cart. | PySpark & Intent — 25 điểm |
| Recovery | Bắt đầu Recovery → bốn lần Bước kiểm chứng tiếp theo; score 4 → 5 cùng visitor/item/window, query ID giữ nguyên/run ID đổi. | Logging & Fault Tolerance — 15 điểm |
| Watermark | Trở về demo chính → Bắt đầu Watermark → bốn bước; score 1 → 2, state removal, quá trễ giữ score 2. | PySpark & Intent — 25 điểm |
| Logging/storage | Trở về demo chính → Logging & Storage; records/latency, ConsoleSink và Parquet read-back. | Logging & Fault Tolerance — 15 điểm |

Dự kiến tổng 10–15 phút sau khi môi trường đã chuẩn bị; điều chỉnh theo tốc độ máy. UI hỗ trợ live demo trong nhóm Code Modularity & Live Demo — 10 điểm; phần code/tài liệu vẫn cần đối chiếu riêng.

**Tình trạng kiểm tra của tài liệu:** đã lấy tên 15 nút và 4 tab trực tiếp từ `build_app().config`, đối chiếu điều kiện mở/khóa trong `render()` và callback. Khi cập nhật tài liệu, không có server UI trả lời tại các cổng 7860–7863; lượt này không chạy lại kịch bản demo. Nếu màn hình bạn đang mở khác danh mục mục 2, cần xác định đúng tiến trình/bản UI thay vì tìm nút không tồn tại trong code này.

[Proof kiểm chứng trước đó](../../docs/proof/control-panel-20261102T174623Z/manifest.json) lưu 25 host tests, 18 kiểm tra Chrome và 3 kiểm tra lifecycle của bản triển khai đã kiểm chứng. Kết quả chạy mới lưu tại `Part_B/output/control-ui-verification/`, `control-lifecycle-verification/`; scenario lưu `demo/result-<tag>.json` trong output tương ứng. Proof cũ không được coi là bằng chứng đã kiểm thử lại ở lượt chỉnh tài liệu này.
