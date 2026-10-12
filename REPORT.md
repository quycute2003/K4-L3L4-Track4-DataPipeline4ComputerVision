# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phạm Xuân Quý |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602745 |
| GitHub | quycute2003 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02134` |

Quy ước chọn trip cho MSSV có chữ: giữ nguyên `LAB_STUDENT_ID=2A202602745` khi chia dataset; riêng bài tập sập lấy toàn bộ chữ số `2202602745`, tính `2202602745 % 26 = 5` trong danh sách trip đã sắp xếp, được `trip-02134`. Lệnh `int(MSSV)` nguyên bản của GUIDE không hỗ trợ chữ `A`.

## Kết quả cuối

- `dataset_version` của bạn: `2d395955ed58`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `714 / 154 / 151`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` ISO UTC, `speed_mps` (m/s), `heading_deg`, `lat/lon`, `gas/brake` theo tỷ lệ; nguồn B dùng `timestamp_ms` epoch ms, `spd_kmh` (km/h), `hdg`, `latitude/longitude`, `throttle_pct/brake_pct` theo %. Đồng hồ A không lệch, B nhanh hơn video 1.500 ms.
Lần chạy Bronze nhận 3.021 dòng A, 3.234 dòng B, 26 trip và 2 video. Nạp A lần hai báo `already present`; bảng `bronze.raw_telemetry_a` vẫn chỉ có commit version 0, gồm 3.021 dòng.
Tách bảng theo nguồn giữ nguyên khác biệt schema để có thể kiểm tra lại nguồn gốc. Giữ các cột nghiệp vụ dạng chữ bảo toàn ô trống và giá trị lỗi; bước Silver mới chịu trách nhiệm ép kiểu, đổi đơn vị và phân loại. Các cột metadata do pipeline thêm có thể có kiểu riêng.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Silver đổi B về cùng tên cột với A, chia tốc độ km/h cho 3,6, bàn đạp % cho 100 và đổi epoch ms sang timestamp UTC. B được trừ đúng 1.500 ms; A giữ timestamp ISO UTC.
Sau xử lý, A sạch 2.968 dòng, B sạch 3.181 dòng; tốc độ tối đa tương ứng 71 và 100 km/h. Telemetry B bắt đầu `2025-10-08 14:30:00.009+00:00`, chỉ cách video bắt đầu 9 ms, chứng minh đồng hồ đã được hiệu chỉnh.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng rejects |
|---|---:|---:|---:|---:|
| A | 20 | 30 | 3 | 53 |
| B | 20 | 30 | 3 | 53 |

`2.968 + 53 = 3.021` và `3.181 + 53 = 3.234`: không mất dòng nhận về. Giữ dòng lỗi kèm lý do trong quarantine giúp kiểm toán, tìm nguyên nhân và xử lý lại khi có cách sửa, thay vì xoá mất bằng chứng.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` dùng `warn` vì mất tín hiệu cảm biến là lỗi đã biết: cần báo để quan sát và kiểm tra đồng bộ nhưng chưa phải lý do loại toàn bộ drive. `in_range` dùng `error` vì tốc độ ngoài 0–70 m/s không được phép đi vào dataset.
Trong `break_gate.json`, nới luật lên 1.000 m/s làm test `in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng; kết quả `PASS=5 WARN=1 ERROR=1 SKIP=0`.
Sau khôi phục, kết quả `PASS=6 WARN=1 ERROR=0 SKIP=0`. Luật biến đổi có thể bị nới sai nhưng test độc lập vẫn chặn xuất bản downstream; cảnh báo không đồng nghĩa pipeline thất bại.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm chạy lại `trip-02134` hai lần: tổng bảng trước/sau đều 1.246 dòng, dấu vân tay đều `6c7480dfde7e`; trip có 60 khung. Biến thể append hai lần ghi 120 dòng cho cùng trip, tức 2× (`idempotency.json`).
Delta ghi file mới rồi commit log nguyên tử cho phần dữ liệu khớp `trip_id`: lần đọc thấy snapshot trước hoặc sau commit, thay vì khoảng trống giữa xoá và ghi. Bảng phân vùng vật lý theo `drive_id`; predicate giới hạn phạm vi thay thế vào đúng trip.
Khi lỗi xảy ra trước commit, snapshot đã commit vẫn đọc được và có thể chạy lại. Điều này bảo vệ giao dịch của bảng; ảnh JPG là object riêng nên tính tái lập còn phụ thuộc nguồn video và quá trình trích ảnh.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có brightness < 12; khung tối có 12 ≤ brightness < 40. A/B có 6/4 khung đen và 4/61 khung tối được gắn cờ; khung đen bị loại, còn tối không bị loại chỉ vì tối, nhưng vẫn có thể bị loại bởi cờ mờ, trùng hoặc mất đồng bộ. Giữ tình huống đường hầm/bóng râm là hợp lý để dataset phản ánh điều kiện thực.
Theo `threshold.json`, bảng trên cho thấy ngưỡng 6 tăng số khung trùng lên 363/244, dễ bỏ nhiều mẫu khác nhau thật; ngưỡng 0 chỉ bắt 61/8, để lọt nhiều mẫu gần giống.
Chọn ngưỡng 2 (149/53) theo cân bằng này. Xe đi chậm hoặc dừng tạo nhiều ảnh gần nhau, nên cần cân nhắc tốc độ và tần suất lấy mẫu khi hiệu chỉnh; pipeline hiện dùng một ngưỡng cố định, không tự đổi theo tốc độ.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
