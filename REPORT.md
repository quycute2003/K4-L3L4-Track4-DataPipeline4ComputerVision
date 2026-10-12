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

Trong `leakage.json`, 1.019 khung còn lại thuộc 26 trip. Chia theo frame làm 26/26 trip nằm trong nhiều split, đồng thời cả 26 trip có frame ở cả train và test; chia theo trip của Gold có 0 trip bị chia cắt.
Khung liền nhau chứa cùng cảnh, góc nhìn và điều kiện nên rất tương tự. Nếu chúng nằm ở hai phía train/test, mô hình có thể ghi nhớ cảnh đã thấy thay vì tổng quát sang chuyến mới; điểm test khi đó có thể cao giả tạo. Thí nghiệm này đo mức chồng lấn trip, không trực tiếp đo độ chính xác mô hình.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Hai lần xuất dataset liên tiếp đều cho `2d395955ed58`, với 714 train / 154 val / 151 test. Thử MSSV `2A202602746` trong môi trường của riêng lệnh Docker làm version đổi thành `a11d315b867b` và số khung thành 674/169/176; khôi phục MSSV thật trả về version và số liệu ban đầu (`versioning.json`).
Mã hiện tại băm danh sách đã sắp xếp `frame_id|split` bằng SHA256, lấy 12 ký tự: cùng tập thành viên và split cho cùng ID, không bị ảnh hưởng bởi thời điểm chạy. MSSV được nối vào `md5(trip_id || MSSV)` để xếp trip nên thay MSSV có thể thay split.
Giới hạn cần phân biệt: đây là băm thành viên/split, không phải băm toàn bộ pixel hoặc telemetry. Để tái tạo nội dung đầy đủ còn phải dùng checksum nguồn, lineage, luật xử lý và môi trường chạy.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest hiện tại của `2d395955ed58` ghi các phiên bản Delta: `bronze.raw_telemetry_a`=0, `bronze.raw_telemetry_b`=0, `bronze.trips_raw`=0, `bronze.video_files`=1, `silver.frames`=27, `silver.telemetry`=0, `silver.frames_flagged`=0, `silver.frame_telemetry`=0. Bản manifest được lưu tại `report/evidence/dataset_manifest.json`.
Video A có SHA256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; B có SHA256 `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Dataset được tạo từ các nguồn này, telemetry/trips Bronze, khung và telemetry Silver, rồi áp dụng luật loại đen/mờ/trùng/mất đồng bộ và split theo trip.
Để tái tạo sau 3 tháng: cần giữ snapshot Delta/file nguồn tương ứng, checksum CSV trong `data/MANIFEST.json`, MSSV `2A202602745`, cấu hình dbt/ngưỡng, commit mã và phiên bản thư viện. Delta version chỉ hữu ích khi snapshot cũ còn được lưu; riêng manifest không chứa toàn bộ dữ liệu nguồn.

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
