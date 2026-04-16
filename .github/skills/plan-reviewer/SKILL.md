---
name: plan-reviewer
description: "**WORKFLOW SKILL** — Plan reviewer. Use when: reviewing a technical plan before implementation; checking if a plan is feasible; identifying risks, missing pieces, or unclear requirements; validating roadmap structure; ensuring tasks are properly split by layer (frontend/backend/DB). Triggers: 'review plan', 'review lại plan', 'kiểm tra plan', 'plan có ổn không', 'đánh giá plan', 'plan feasibility', 'plan review', 'check plan', 'xem plan', 'có vấn đề gì không'."
argument-hint: "Đường dẫn đến file plan cần review (ví dụ: plans/my-feature.md)"
---

# Plan Reviewer

## Purpose

Review một technical plan trước khi bắt đầu implementation để đảm bảo:
1. Plan khả thi về mặt kỹ thuật
2. Không có rủi ro tiềm ẩn bị bỏ qua
3. Roadmap đủ cụ thể và phân tầng rõ ràng
4. Các task phân chia đúng theo layer (Frontend / Backend / DB / Testing)

---

## When to Use

- Trước khi bắt đầu implement một plan mới
- Khi muốn sanity-check một plan do AI tạo ra
- Khi plan cần được approve trước khi giao cho team
- Người dùng nói: "review plan", "plan có ổn không", "kiểm tra plan", v.v.

---

## Workflow (4 Phases)

### Phase 1 — Đọc và Hiểu Plan

1. **Xác định file plan cần review**:
   - Nếu người dùng chỉ định file path → đọc file đó
   - Nếu không chỉ định → hỏi hoặc tìm file `.md` mới nhất trong `plans/`

2. **Đọc toàn bộ nội dung plan**, ghi nhận:
   - Mục tiêu / phạm vi
   - Cấu trúc roadmap
   - Danh sách tasks/steps
   - Acceptance Criteria (nếu có)
   - Dependencies / constraints (nếu có)

3. **Scan codebase liên quan** (nếu cần) để hiểu context kỹ thuật:
   - Các module bị ảnh hưởng
   - DB schema hiện tại
   - API endpoints liên quan

---

### Phase 2 — Phân Tích và Phát Hiện Vấn Đề

Dùng [checklist đầy đủ](./references/checklist.md) để đánh giá plan theo các chiều:

#### [FEASIBILITY] — Tính khả thi
- Mục tiêu có rõ ràng và đo được không?
- Có assumption nào chưa được kiểm chứng?
- Có phụ thuộc vào hệ thống bên ngoài chưa sẵn sàng không (API, data, infra)?
- Effort có được ước tính sơ bộ không?

#### [STRUCTURE] — Cấu trúc Roadmap
- Plan có roadmap cụ thể không (không chỉ mô tả chung chung)?
- Các tasks có phân chia rõ theo layer không:
  - `Backend` — API, service, business logic
  - `Frontend` — UI components, routing, state
  - `Database` — schema, migration, index
  - `Testing & Verification` — unit tests, integration, E2E
  - `DevOps/Infra` (nếu cần) — deployment, config, env vars
- Thứ tự tasks có hợp lý không (không có circular dependency)?
- Tasks có đủ nhỏ để có thể estimate và track không?

#### [RISKS] — Rủi ro
- Có thay đổi schema/migration nào không? (rủi ro data loss, downtime)
- Có breaking change đối với API hiện tại không?
- Có thay đổi nào ảnh hưởng đến shared modules/utilities không?
- Có yêu cầu rollback plan không?
- Có side effect với pipeline/cronjob đang chạy không?

#### [COMPLETENESS] — Tính đầy đủ
- Có Acceptance Criteria không? Có đo được không?
- Có bỏ sót edge case nào rõ ràng không?
- Có mention đến error handling / logging không?
- Có bao gồm security considerations không (auth, input validation)?
- Có chỉ định ai làm gì không (nếu plan cho team)?

#### [CLARITY] — Sự rõ ràng
- Có thuật ngữ/từ viết tắt nào không được giải thích không?
- Có task nào mơ hồ, không có definition of done không?
- Có mâu thuẫn nội bộ trong plan không?

---

### Phase 3 — Tổng hợp và Đặt Câu Hỏi

Sau khi phân tích, tổng hợp kết quả theo cấu trúc sau:

```
## Kết quả Review Plan: <Tên Plan>

### Verdict: ✅ Khả thi / ⚠️ Khả thi có điều kiện / ❌ Cần rework

### Điểm mạnh
- [Những gì plan làm tốt]

### Vấn đề phát hiện

#### [CRITICAL] <Tiêu đề ngắn>
- **Vấn đề**: <mô tả rõ>
- **Rủi ro**: <hậu quả nếu không xử lý>
- **Đề xuất**: <cách khắc phục>

#### [HIGH] <Tiêu đề ngắn>
...

#### [MEDIUM] <Tiêu đề ngắn>
...

#### [LOW / SUGGESTION] <Tiêu đề ngắn>
...

### Câu hỏi cần làm rõ
1. [Câu hỏi 1 — liên quan đến điểm chưa rõ hoặc assumption]
2. [Câu hỏi 2]
...

### Đề xuất bổ sung
- [Thứ gì nên thêm vào plan để hoàn thiện hơn]
```

**Severity levels:**
- `CRITICAL` — Plan sẽ thất bại hoặc gây hỏng hóc nghiêm trọng nếu không xử lý
- `HIGH` — Rủi ro lớn, cần giải quyết trước khi implement
- `MEDIUM` — Nên xử lý, có thể defer nhưng phải acknowledge
- `LOW / SUGGESTION` — Cải thiện chất lượng, không bắt buộc

---

### Phase 4 — Đề Xuất Cập Nhật Plan (Tùy chọn)

Nếu người dùng muốn cập nhật plan dựa trên feedback:

1. Hỏi người dùng: muốn cập nhật trực tiếp vào file plan không?
2. Nếu có, apply các thay đổi sau:
   - Cấu trúc lại roadmap theo các layer còn thiếu
   - Thêm section còn thiếu (Risks, Rollback, Acceptance Criteria)
   - Tách tasks quá to thành sub-tasks
   - Cập nhật Status nếu cần
3. Không xóa hay thay đổi nội dung gốc nếu không được yêu cầu rõ ràng

---

## Cấu trúc Plan Chuẩn (Reference)

Plan được coi là **đầy đủ** khi có đủ các section sau:

```markdown
# [Tên Task]

## Status: Draft / In Progress / Done

## Mục tiêu
[Mô tả ngắn gọn, có thể đo được]

## Roadmap

### Backend (nếu có)
- [ ] Task B1
- [ ] Task B2

### Frontend (nếu có)
- [ ] Task F1

### Database (nếu có)
- [ ] Migration / schema change
- [ ] Index strategy

### Testing & Verification
- [ ] Unit test
- [ ] Integration test / E2E

### DevOps / Infra (nếu có)
- [ ] Deployment step
- [ ] Env var / config change

## Acceptance Criteria
- [ ] Criterion 1 — có thể kiểm tra được
- [ ] Criterion 2

## Risks & Mitigations
| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| ...  | Low/Med/High | Low/Med/High | ... |

## Rollback Plan (nếu cần)
[Cách rollback nếu deploy thất bại]

## Notes
[Quyết định kỹ thuật, trade-offs]
```
