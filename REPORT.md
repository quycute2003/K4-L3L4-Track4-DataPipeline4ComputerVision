# REPORT — Lab Day 9

> Báo cáo vận hành pipeline với MSSV `2A202602745`; số liệu và ảnh được lấy từ các lượt chạy trên máy của bài làm.

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
- [Kết quả verify](report/verify.txt): đã lưu đầu ra `scripts/lab.sh verify` qua Git Bash trên Windows, đạt **27/27 checks passed**.
- Ảnh: [DAG từ lake trống](report/screenshots/airflow_graph.png), [log task sập](report/screenshots/airflow_crash_log.png), [preview Clear](report/screenshots/airflow_clear_preview.png), [DAG phục hồi](report/screenshots/airflow_recovered_graph.png), [MinIO](report/screenshots/minio_lake.png), [Metabase](report/screenshots/metabase_dashboard.png), [câu hỏi riêng](report/screenshots/metabase_own_question.png).

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

Manifest của lượt chạy cuối `2d395955ed58` ghi các phiên bản Delta: `bronze.raw_telemetry_a`=0, `bronze.raw_telemetry_b`=0, `bronze.trips_raw`=0, `bronze.video_files`=1, `silver.frames`=25, `silver.telemetry`=0, `silver.frames_flagged`=0, `silver.frame_telemetry`=0. Bản manifest được lưu tại `report/evidence/dataset_manifest.json`.
Video A có SHA256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; B có SHA256 `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Dataset được tạo từ các nguồn này, telemetry/trips Bronze, khung và telemetry Silver, rồi áp dụng luật loại đen/mờ/trùng/mất đồng bộ và split theo trip.
Để tái tạo sau 3 tháng: cần giữ snapshot Delta/file nguồn tương ứng, checksum CSV trong `data/MANIFEST.json`, MSSV `2A202602745`, cấu hình dbt/ngưỡng, commit mã và phiên bản thư viện (đã lưu trong `runtime_versions.json`). Delta version chỉ hữu ích khi snapshot cũ còn được lưu; riêng manifest không chứa toàn bộ dữ liệu nguồn.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Theo quy ước chữ số đã ghi ở đầu báo cáo, trip của bài tập là `trip-02134`, map index 5. Sau reset và trigger với `LAB_FAIL_ONCE=trip-02134`, có đúng 1 mapped task đỏ, 7 task downstream `upstream_failed` và 29 task thành công; log có `simulated crash while extracting trip-02134`.
Trong giao diện Airflow chọn instance 5 và Clear task; chế độ mặc định có Downstream, preview liệt kê 8 task. Chỉ 1/26 task trích ảnh chạy lại (try 2); 25 instance đã thành công giữ nguyên thời điểm chạy. Bảy task downstream trước đó chưa được thực thi nay chạy tiếp, rồi DAG đạt 37/37 success.
Sau phục hồi, bảng frames vẫn có 1.246 dòng, fingerprint `6c7480dfde7e`, dataset vẫn `2d395955ed58`, split 714/154/151, không nhân đôi. Bằng chứng: `airflow_crash_log.png`, `airflow_clear_preview.png`, `airflow_recovered_graph.png` và `airflow_crash.json`/`airflow_recovery.json`.
Cần phân biệt chọn một task đỏ với phạm vi Clear: UI có thêm 7 downstream đang bị chặn; không clear 25 task trích ảnh xanh. Đã bỏ `LAB_FAIL_ONCE` khỏi cấu hình sau bài tập.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Đây là dự đoán từ cấu hình/mã lab, chưa phải benchmark 500 xe/ngày. Ba nút thắt cụ thể:

1. **Decode video và I/O:** `extract_trip` tải cả video cho từng trip, rồi FFmpeg giải mã và xử lý ảnh trên CPU. Nhiều trip của cùng drive tải lại cùng file. Tách worker decode theo drive/clip, dùng cache video tại worker và phân phối job theo cụm máy; cân nhắc tăng tốc giải mã sau khi đo throughput.
2. **ELT dùng một file DuckDB:** dbt hiện chạy trên `/opt/work/lakehouse.duckdb`; chế độ nhúng read-write tập trung vào một tiến trình ghi nên tăng worker cùng ghi file này không giải quyết được scale. Chuyển phần join/biến đổi lớn sang Spark + Delta trên object storage, giữ dbt cho SQL và quality gate phù hợp với engine mới. Xem [DuckDB concurrency](https://www.duckdb.org/docs/current/connect/concurrency) và [Delta cho engine phân tán](https://docs.delta.io/delta-kernel/).
3. **Airflow một máy và mapping:** `parallelism=2`, `silver_frames` chỉ chạy 1 instance cùng lúc, và `max_map_length` đọc trực tiếp từ Airflow là 1.024. Nếu mỗi xe có 13 trip như lab thì 500 xe tạo 6.500 mapped instance, vượt giới hạn hiện tại. Chia DAG/mapping theo drive hoặc batch, dùng KubernetesExecutor/CeleryExecutor với worker tách khỏi scheduler, đồng thời bỏ phụ thuộc vào file DuckDB dùng chung. Xem [Airflow executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html).

Các thay đổi này tách storage/compute và đưa ELT sang xử lý phân tán để đáp ứng yêu cầu mở rộng được đề nhắc cùng slide 12, 22, 23; repo không kèm các slide để kiểm tra nội dung chi tiết.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi:** trip nào có tỷ lệ khung bị loại cao nhất, và điều kiện ánh sáng/tốc độ của trip đó là gì? Đã thêm câu hỏi `Trip nào mất nhiều khung nhất? — 2A202602745` vào [dashboard cục bộ](http://localhost:3000/dashboard/2), card ID 46.

```sql
select drive_id, trip_id,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as dropped_pct,
       frames, frames_kept, frames - frames_kept as frames_dropped,
       lighting, conditions, avg_speed_kmh, split
from marts.mart_trip_summary
order by dropped_pct desc, frames desc, trip_id
```

Kết quả: `trip-04622` (drive A) bị loại 34/60 khung, tức 56,7%, cao nhất trong 26 trip; metadata Day/Clouds, tốc độ trung bình 67,1 km/h. `trip-03904` tiếp theo là 11/22 = 50%. Tỷ lệ này giúp ưu tiên xem video/trip có vấn đề; truy vấn đếm khung bị loại một lần theo Gold, không cộng các cờ có thể chồng lấn. Chưa thể kết luận nguyên nhân chỉ từ tốc độ và ánh sáng; cần xem các cờ và ảnh từng trip. Số liệu đầy đủ lưu trong `metabase_question.json`.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều đáng chú ý từ lần chạy là khung tối không bị loại chỉ vì tối: drive B có 61 khung được gắn cờ, trong khi khung đen chỉ có 4. Chia theo frame khiến cả 26 trip rò rỉ giữa train/test dù tỷ lệ split trông hợp lý.
Ngưỡng dHash 2 vẫn cần kiểm tra bằng ảnh thật trên điều kiện xe đi chậm/đường hầm; thí nghiệm đếm khung chưa chứng minh mẫu bị loại là dư thừa hoàn toàn.
Một lỗi chưa được kiểm tra đầy đủ là GPS ngoài vùng hợp lệ hoặc nhảy vị trí phi thực tế: test hiện chủ yếu kiểm tra tốc độ, trùng, timestamp và chất lượng ảnh.
Ngoài ra `dataset_version` chỉ băm thành viên và split, nên đổi nội dung ảnh mà giữ frame ID/split có thể không đổi ID dataset; checksum nguồn và lineage là phần cần thiết để truy vết.
