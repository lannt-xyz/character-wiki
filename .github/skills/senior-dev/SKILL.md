---
name: senior-dev
description: "**WORKFLOW SKILL** — Senior developer workflow. Use when: implementing a new feature, refactoring a module, fixing a complex bug, or handling any task that requires structured planning and verification. Triggers: 'plan this', 'implement feature', 'refactor', 'build', 'design', 'architect', 'new task', 'yêu cầu', 'làm task', 'lên kế hoạch', 'triển khai'."
argument-hint: "Mô tả yêu cầu công việc cần thực hiện"
---

# Senior Developer Workflow

## When to Use
- Nhận yêu cầu tính năng mới, refactor, hoặc sửa bug phức tạp
- Task liên quan đến nhiều file hoặc nhiều layer (frontend / backend / DB)
- Cần kế hoạch rõ ràng trước khi code
- Cần xác nhận kết quả sau khi code

---

## Procedure

### Step 1 — Làm rõ yêu cầu

Trước khi làm bất cứ điều gì, kiểm tra xem yêu cầu có điểm nào chưa rõ không:

- Đọc kỹ yêu cầu và tìm các **ambiguous points** (điểm mơ hồ)
- Nếu có điểm chưa rõ: hỏi người dùng trước, liệt kê từng điểm dưới dạng câu hỏi đánh số
- Chỉ tiếp tục Step 2 khi tất cả điểm đã được làm rõ
- Nếu yêu cầu rõ ràng hoàn toàn: ghi nhận và chuyển ngay sang Step 2

**Checklist làm rõ:**
- Scope: Task ảnh hưởng đến những module/layer nào?
- Behavior: Expected input/output là gì?
- Edge cases: Có trường hợp đặc biệt nào cần xử lý không?
- Dependencies: Có integration point nào với hệ thống bên ngoài không?
- Constraints: Có ràng buộc về performance, backward compatibility không?

---

### Step 2 — Lên kế hoạch thực thi

Sau khi đã làm rõ yêu cầu, tạo file kế hoạch:

**Quy tắc đặt tên file plan:**
- Tên mô tả ngắn gọn nội dung, dùng kebab-case
- Nếu file `plans/<tên>.md` đã tồn tại, tăng postfix số: `<tên>-2.md`, `<tên>-3.md`, ...
- Ví dụ: `plans/enhance-report.md` → `plans/enhance-report-2.md`

**Cấu trúc file plan bắt buộc:**

```markdown
# [Tên Task]

## Status: In Progress

## Mục tiêu
[Mô tả ngắn gọn mục tiêu và phạm vi]

## Roadmap

### Backend (nếu có thay đổi backend)
- [ ] Item 1
- [ ] Item 2

### Frontend (nếu có thay đổi frontend/dashboard)
- [ ] Item 1
- [ ] Item 2

### Database (nếu có thay đổi schema/migration)
- [ ] Item 1

### Testing & Verification
- [ ] Unit test cho các hàm logic tính toán
- [ ] Integration/E2E verification

## Acceptance Criteria
- [ ] Tiêu chí 1
- [ ] Tiêu chí 2

## Notes
[Ghi chú về quyết định kỹ thuật quan trọng]
```

**Nguyên tắc plan:**
- Nếu task chạm đến cả frontend lẫn backend: tách thành 2 hạng mục riêng trong plan
- Mỗi item trong plan phải là một action cụ thể, có thể verify được
- Ước lượng độ phức tạp để quyết định thứ tự triển khai

Sau khi tạo plan: **trình bày tóm tắt plan cho người dùng và xin xác nhận** trước khi triển khai.

> **QUAN TRỌNG:** Sau khi tạo plan, **DỪNG LẠI** và chờ người dùng review.
> - Nếu người dùng yêu cầu chỉnh sửa: cập nhật plan và trình bày lại, tiếp tục chờ
> - Chỉ chuyển sang Step 3 khi người dùng **explicitly approved** (ví dụ: "ok", "approved", "làm đi", "go ahead")
> - Không tự ý bắt đầu implement khi chưa có xác nhận

---

### Step 3 — Triển khai theo plan

**Trước khi bắt đầu code, tạo branch mới:**

1. Xác định tên branch theo định dạng: `feature/<tên-task-ngắn-gọn>` hoặc `fix/<tên-bug>` (kebab-case)
2. Lưu lại tên branch hiện tại (base branch) để tạo PR sau
3. Tạo và checkout sang branch mới:
   ```bash
   git checkout -b feature/<tên-task>
   ```
4. Ghi tên branch mới vào phần **Notes** trong file plan

Implement từng item trong plan, theo thứ tự. Sau khi hoàn thành mỗi item:

- Đánh dấu `[x]` trong file plan: `- [x] Item đã hoàn thành`
- Nếu item là nhiều file: cập nhật plan ngay sau khi file cuối cùng của item đó xong
- Không bỏ qua việc cập nhật plan — đây là record tiến độ

**Nguyên tắc triển khai:**
- Implement đúng scope của plan, không thêm feature ngoài yêu cầu
- Giữ changes tối thiểu và focused
- Tuân thủ coding conventions đang có trong codebase
- Dọn dẹp imports thừa sau khi sửa file
- Log các sự kiện quan trọng (errors, critical events)

---

### Step 4 — Xác nhận kết quả

Sau khi implement xong, verify theo loại thay đổi:

**Đối với hàm logic/tính toán:**
- Viết hoặc chạy unit test để verify output đúng
- Ưu tiên dùng unittest/pytest với các test case điển hình + edge cases
- Ví dụ: `pytest tests/test_<module>.py -v`

**Đối với luồng hoạt động/integration:**
- Tạo một script nhỏ để chạy thử luồng end-to-end, hoặc
- Chạy trực tiếp trong console để confirm behavior
- Ví dụ: `python -c "from app.xxx import yyy; print(yyy())"`

**Đối với API/endpoint:**
- Chạy server và test với curl hoặc một script nhỏ

**Sau khi verify:**
- Cập nhật status trong file plan: `## Status: Done`
- Thêm comment summary vào cuối plan:
  ```markdown
  ## Implementation Summary
  [Tóm tắt những gì đã làm, quyết định kỹ thuật quan trọng, known limitations]
  ```
- Cập nhật acceptance criteria trong plan: đánh dấu `[x]` các tiêu chí đã đạt

**Tạo Pull Request:**

1. Commit toàn bộ thay đổi (bao gồm file plan đã cập nhật):
   ```bash
   git add -A
   git commit -m "<type>: <mô tả ngắn gọn>"
   ```
2. Push branch lên remote:
   ```bash
   git push -u origin <tên-branch-mới>
   ```
3. Tạo Pull Request vào **base branch** (branch đang đứng trước khi tạo branch mới):
   - Title: `[<type>] <mô tả ngắn gọn>`
   - Body: tóm tắt những thay đổi, link đến file plan, và các acceptance criteria đã đạt
   - Dùng GitHub CLI nếu có: `gh pr create --base <base-branch> --title "..." --body "..."`
   - Hoặc dùng MCP GitHub tool để tạo PR
4. Thông báo link PR cho người dùng

---

## Quality Checklist (before finishing)

Trước khi kết thúc task, tự review:

- [ ] Không có unused imports / variables / functions
- [ ] Không có hardcoded secrets hoặc magic numbers
- [ ] Error handling hợp lý — không nuốt exception, không dư thừa
- [ ] Logic đúng, không có off-by-one hay race condition rõ ràng
- [ ] Style nhất quán với code xung quanh
- [ ] Plan file đã được cập nhật đầy đủ
- [ ] Tests pass (nếu có viết test)
