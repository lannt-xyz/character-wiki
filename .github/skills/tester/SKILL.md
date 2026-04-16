---
name: tester
description: "**WORKFLOW SKILL** — Hướng dẫn chi tiết quy trình QA & Testing. Dùng để phân tích side-effect, viết unit test cho logic phức tạp, chạy giả lập (mock/dummy) xác nhận full flow, và dọn dẹp môi trường. Trigger: test flow, kiểm thử, phân tích side-effect, viết unit test logic."
argument-hint: "Mô tả file hoặc quy trình cần test"
---

# Workflow Kiểm thử & QA (Tester)

## Khi nào sử dụng (When to Use)
- Khi một tính năng backend/frontend vừa được hoàn thành và cần kiểm thử độ ổn định.
- Khi có sự thay đổi code (code change) có nguy cơ gây lỗi cho các module khác.
- Khi cần viết Unit Test cho các thuật toán khó (risk engine, machine learning, logic tính toán).

## Quy trình Thực thi (Procedure)

Bạn phải thực hiện tuần tự các bước sau để đảm bảo chất lượng hệ thống:

### Bước 1: Phân tích Tác động (Side-effect Analysis)
1. Liệt kê các file vừa bị thay đổi.
2. Dùng công cụ `search` để tìm tất cả các file/module khác đang gọi (import/invoke) tới những đoạn code bị thay đổi đó.
3. Xác định luồng dữ liệu (full flow) bị ảnh hưởng để khoanh vùng những gì cần test.

### Bước 2: Viết Unit Test cho Logic Cốt lõi
1. BỎ QUA việc tạo test cho các luồng CRUD cơ bản hoặc giao diện tĩnh không chứa logic.
2. TẬP TRUNG viết test (dùng `pytest`) cho các hàm phức tạp. Lưu file test vào thư mục `tests/`.
3. Chạy `pytest` qua terminal để đảm bảo test passed.

### Bước 3: Kiểm thử Full Flow bằng Code Tạm (Dummy/Mock)
1. Nếu cần giả lập dữ liệu hoặc luồng chạy từ đầu tới cuối, hãy tạo một script tạm (ví dụ `test_temp_flow.py`).
2. Chạy script đó để confirm kết quả khớp với Acceptance Criteria.

### Bước 4: Dọn dẹp (Clean up)
1. **BẮT BUỘC:** Sau khi confirm luồng chạy thành công, phải xóa sạch các file code tạm, file sinh ra do test (những file không nằm trong source code chính thức hoặc source code test chuẩn).
2. Kiểm tra lại `git status` (nếu cần) để chắc chắn không commit rác.

### Bước 5: Cập nhật Kế hoạch (Update Plan)
1. Đọc file Master Plan markdown trong thư mục `plans/`.
2. Cập nhật tiến độ: Đánh dấu hoàn tất việc kiểm thử, ghi chú lại các side-effect đã phân tích và lưu lại file.
