---
name: develop-new-feature
description: "**WORKFLOW SKILL** — End-to-end feature development orchestrator. Chạy toàn bộ vòng đời phát triển: lập plan → review plan → code theo plan (senior-dev) → review code → test. Triggers: 'develop feature', 'làm tính năng', 'new feature', 'phát triển tính năng', 'full dev flow', 'build feature', 'implement and test', 'làm từ đầu', 'end-to-end', 'full cycle', 'develop-new-feature'."
argument-hint: "Mô tả tính năng cần phát triển (hoặc chỉ định file plan đã có sẵn)"
---

# Develop New Feature — Full Cycle Workflow

## Mục đích

Orchestrate toàn bộ vòng đời phát triển một tính năng mới theo thứ tự:

```
[Plan] → [Review Plan] → [Code] → [Review Code] → [Test]
```

Mỗi bước gọi đúng skill/workflow chuyên biệt. Kết quả từ bước trước là đầu vào cho bước sau.

---

## When to Use

- Khi cần phát triển một tính năng hoàn chỉnh từ đầu đến cuối
- Khi muốn đảm bảo chất lượng qua từng giai đoạn (plan → review → code → review → test)
- Khi cần một checklist quy trình nhất quán cho mọi tính năng
- Khi làm task phức tạp ảnh hưởng nhiều layer (frontend/backend/DB)

---

## Procedure

### PHASE 1 — Lập Kế Hoạch (Plan)

**Điều kiện:** Luôn chạy, trừ khi người dùng đã cung cấp file plan sẵn có.

**Nếu chưa có plan:**

1. Đọc yêu cầu tính năng từ argument hoặc conversation
2. Làm rõ các điểm mơ hồ — hỏi người dùng nếu cần (xem checklist bên dưới)
3. Gọi workflow **`senior-dev`** (Steps 1–2: Làm rõ yêu cầu + Lên kế hoạch)
4. Tạo file plan tại `plans/<tên-feature>.md` theo cấu trúc chuẩn

**Checklist làm rõ yêu cầu:**
- Scope: Ảnh hưởng đến module/layer nào?
- Behavior: Input/output mong đợi là gì?
- Edge cases: Trường hợp đặc biệt nào cần xử lý?
- Dependencies: Có integration point bên ngoài không?
- Constraints: Ràng buộc performance, backward compatibility?

**Nếu đã có plan (người dùng chỉ định file plan):**
- Đọc file plan đó
- Xác nhận với người dùng: "Sếp đã có plan tại `<path>`, em sẽ bỏ qua bước tạo plan và review plan, nhảy thẳng sang code. Confirm không?"
- Chuyển thẳng sang **PHASE 3**

---

### PHASE 2 — Review Kế Hoạch (Plan Review)

**Điều kiện:** Chỉ chạy nếu plan vừa được tạo ở Phase 1 (không chạy nếu plan đã được chỉ định sẵn).

1. Gọi skill **`plan-reviewer`** với file plan vừa tạo
2. Plan reviewer sẽ kiểm tra:
   - Tính khả thi kỹ thuật
   - Thiếu sót, rủi ro, hoặc yêu cầu chưa rõ
   - Phân tách task đúng layer (frontend/backend/DB)
   - Acceptance criteria có đủ và đo được không
3. Nếu plan reviewer báo cáo vấn đề nghiêm trọng:
   - Cập nhật file plan theo phản hồi
   - Xác nhận lại với người dùng nếu cần
4. Sau khi plan đã được approve (hoặc không có vấn đề): chuyển sang Phase 3

---

### PHASE 3 — Implement Code (Senior Dev)

**Điều kiện:** Luôn chạy sau khi có plan đã được approve.

1. Gọi workflow **`senior-dev`** (Steps 3+: Implement theo plan)
2. Senior dev sẽ:
   - Explore codebase để hiểu context
   - Implement từng item trong Roadmap theo thứ tự
   - Tick checkbox từng item sau khi hoàn thành
   - Cập nhật file plan khi hoàn tất tất cả items
3. Sau khi implement xong: chuyển sang Phase 4

---

### PHASE 4 — Review Code (Code Reviewer)

**Điều kiện:** Luôn chạy sau Phase 3.

1. Gọi skill **`code-reviewer`** với scope là các file vừa thay đổi
2. Code reviewer sẽ:
   - Kiểm tra correctness, security, Python quality, project conventions
   - Kiểm tra performance, maintainability, error handling
   - Tạo danh sách issue có cấu trúc
   - Fix tất cả issues theo severity
3. Sau khi không còn issue nào: chuyển sang Phase 5

---

### PHASE 5 — Test (Tester)

**Điều kiện:** Luôn chạy sau Phase 4.

1. Gọi skill **`tester`** cho các module/file đã thay đổi
2. Tester sẽ:
   - Phân tích side-effect của thay đổi
   - Viết unit test cho logic phức tạp
   - Chạy test và xác nhận full flow
   - Dọn dẹp môi trường test
3. Báo cáo kết quả test cuối cùng

---

## Decision Flowchart

```
User Input
    │
    ▼
Plan đã có sẵn? ──YES──► Đọc plan ──────────────────────────────────────┐
    │                                                                      │
   NO                                                                      │
    │                                                                      │
    ▼                                                                      │
PHASE 1: Tạo Plan (senior-dev workflow)                                   │
    │                                                                      │
    ▼                                                                      │
PHASE 2: Review Plan (plan-reviewer skill)                                │
    │                                                                      │
    ▼◄─────────────────────────────────────────────────────────────────────┘
PHASE 3: Implement Code (senior-dev skill)
    │
    ▼
PHASE 4: Review Code (code-reviewer skill)
    │
    ▼
PHASE 5: Test (tester skill)
    │
    ▼
DONE — Feature hoàn chỉnh, đã review và test
```

---

## Completion Criteria

Feature được coi là hoàn thành khi:
- [ ] File plan tồn tại với tất cả items đã được tick
- [ ] Status trong plan file: `Done`
- [ ] Code review không còn issue nào ở severity `HIGH` hoặc `CRITICAL`
- [ ] Tất cả test pass
- [ ] Không có unused imports, dead code, hay debug statements còn sót

---

## Notes

- Mỗi phase là một checkpoint — người dùng có thể review và confirm trước khi qua phase tiếp theo
- Nếu một phase thất bại hoặc phát hiện vấn đề nghiêm trọng, quay lại phase trước để sửa
- Không skip phase nếu không có lý do rõ ràng
- Luôn dùng tiếng Việt trong toàn bộ workflow (trừ code)
