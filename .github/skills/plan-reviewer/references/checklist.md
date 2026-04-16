# Plan Review Checklist

Dùng checklist này trong Phase 2 để đánh giá toàn diện một plan.

---

## [FEASIBILITY] Tính khả thi

- [ ] Mục tiêu được phát biểu rõ ràng, không mơ hồ
- [ ] Có thể đo được kết quả sau khi hoàn thành
- [ ] Không có assumption ngầm chưa được kiểm chứng (ví dụ: "API bên thứ 3 đã sẵn sàng")
- [ ] Phụ thuộc bên ngoài (third-party API, infra, team khác) đã được xác nhận
- [ ] Scope không bị "scope creep" — không ôm đồm quá nhiều trong một plan
- [ ] Resource (người, thời gian, công cụ) có khả thi không?

---

## [STRUCTURE] Cấu trúc Roadmap

- [ ] Có roadmap (không phải chỉ mô tả chung chung)
- [ ] Tasks phân chia đúng theo layer:
  - [ ] **Backend** — service, API, business logic, worker
  - [ ] **Frontend** — UI, routing, state management, API integration
  - [ ] **Database** — model change, migration, index, seed data
  - [ ] **Testing & Verification** — unit, integration, E2E
  - [ ] **DevOps/Infra** — deployment, config, env, Docker, CI/CD (nếu liên quan)
- [ ] Thứ tự tasks không có circular dependency (A cần B, B cần A)
- [ ] Tasks đủ nhỏ và cụ thể để track (không có task mơ hồ như "làm phần backend")
- [ ] Các tasks không chồng chéo layer một cách không cần thiết

---

## [RISKS] Rủi ro

### Database Risks
- [ ] Có migration không? Có rollback migration không?
- [ ] Có index thay đổi — ảnh hưởng query performance?
- [ ] Có rename column/table — cần backward compatible?
- [ ] Có xóa dữ liệu hoặc column không?

### API / Integration Risks
- [ ] Có breaking change trên API endpoint hiện tại không?
- [ ] Có thay đổi response format — client cũ bị break?
- [ ] Authentication/Authorization thay đổi không?

### System Risks
- [ ] Có ảnh hưởng đến pipeline/cron job đang chạy không?
- [ ] Có shared utility/module nào bị thay đổi — ảnh hưởng module khác?
- [ ] Có thể gây downtime không? Có zero-downtime strategy không?
- [ ] Session/cache invalidation có được xử lý không?

### Security Risks
- [ ] Có endpoint mới — có auth/authz không?
- [ ] Dữ liệu nhạy cảm có bị expose không?
- [ ] Input validation ở đâu?

---

## [COMPLETENESS] Tính đầy đủ

- [ ] Có **Acceptance Criteria** không? Có thể kiểm tra được không?
- [ ] Có section **Risks** không?
- [ ] Có **Rollback Plan** cho các thay đổi nguy hiểm không?
- [ ] Error handling được nhắc đến không?
- [ ] Logging/monitoring được nhắc đến không?
- [ ] Edge cases rõ ràng có được xử lý không?
- [ ] Performance implications được đề cập không (với data-heavy ops)?
- [ ] Testing strategy được chỉ định không?

---

## [CLARITY] Sự rõ ràng

- [ ] Không có thuật ngữ/viết tắt không được giải thích
- [ ] Mỗi task có "Definition of Done" rõ ràng
- [ ] Không có mâu thuẫn nội bộ (task A nói A, task B nói ~A)
- [ ] Status của plan được ghi rõ (Draft / In Progress / Done)
- [ ] Notes section có giải thích trade-offs và quyết định kỹ thuật quan trọng

---

## Scoring Guide (tham khảo)

| Số vấn đề CRITICAL | Verdict |
|---|---|
| 0 | ✅ Khả thi |
| 1–2 | ⚠️ Khả thi có điều kiện — cần resolve trước khi code |
| 3+ | ❌ Cần rework plan trước |
