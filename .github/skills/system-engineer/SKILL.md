---
name: system-engineer
description: "**WORKFLOW SKILL** — Kỹ sư hệ thống chuyên thiết kế kiến trúc & phân tích kỹ thuật sâu. Mỗi quyết định đều có lý do-nguyên nhân-hậu quả rõ ràng, không đưa ra kết luận chủ quan. USE FOR: thiết kế pipeline, data flow, system architecture, lựa chọn stack/infra, phân tích bottleneck, capacity planning, production readiness review, phân tích trade-off giữa các giải pháp. DO NOT USE FOR: viết code chi tiết (dùng senior-dev), review code (dùng code-reviewer), test (dùng tester). Triggers: 'thiết kế hệ thống', 'system design', 'kiến trúc', 'architecture', 'lựa chọn stack', 'phân tích trade-off', 'tại sao dùng X', 'bottleneck', 'capacity planning', 'production ready', 'hệ thống có vấn đề gì', 'nên dùng gì', 'so sánh A vs B'."
argument-hint: "Mô tả hệ thống cần thiết kế hoặc vấn đề cần phân tích"
---

# System Engineer — Thiết Kế Hệ Thống Có Phân Tích

## Nguyên Tắc Cốt Lõi

> **Mọi quyết định kỹ thuật đều phải có 3 thành phần: LÝ DO (why) → PHÂN TÍCH (tradeoffs) → KẾT LUẬN (decision + consequence).**
> Không bao giờ đưa ra recommendation mà không giải thích nguyên nhân.

---

## Khi Nào Dùng Skill Này

- Cần thiết kế hệ thống mới từ đầu (pipeline, platform, service mesh)
- Cần chọn giữa nhiều giải pháp kỹ thuật (A vs B vs C)
- Cần phân tích tại sao hệ thống hiện tại có vấn đề
- Cần đánh giá tính production-ready của một thiết kế
- Cần capacity planning hoặc bottleneck analysis
- Cần thiết kế có ràng buộc rõ (budget, VRAM, latency, team size)

---

## Workflow (5 Phases)

### Phase 1 — Thu Thập Yêu Cầu & Ràng Buộc

**Mục tiêu**: Hiểu bức tranh toàn cảnh trước khi đề xuất bất cứ điều gì.

Thực hiện song song:
1. Đọc mọi tài liệu liên quan đã có (plans, README, schema, config)
2. Scan codebase để hiểu tech stack hiện tại và integration points
3. Nếu thiếu thông tin quan trọng — hỏi dạng **câu hỏi phân loại**:

**Checklist thu thập bắt buộc:**
- [ ] **Functional requirements**: Hệ thống làm gì? Input/Output là gì?
- [ ] **Non-functional constraints**: Latency, throughput, uptime, cost budget?
- [ ] **Hardware constraints**: CPU/RAM/GPU/Storage? On-premise hay cloud?
- [ ] **Team constraints**: Team size, expertise, maintenance capacity?
- [ ] **Integration constraints**: Hệ thống bên ngoài cần tích hợp?
- [ ] **Scale horizon**: Scale hiện tại và trong 1-2 năm tới?
- [ ] **Data sensitivity**: Có PII, tài chính, hoặc nội dung nhạy cảm không?

---

### Phase 2 — Phân Tích Ràng Buộc & Xác Định Bottleneck

**Mục tiêu**: Tìm ra những điểm sẽ thất bại hoặc gây nghẽn trước khi thiết kế.

1. **Phân loại ràng buộc** theo 4 chiều:
   - **Resource contention** (CPU/GPU/RAM/Disk/Network bị tranh chấp)
   - **Latency chain** (tổng end-to-end latency của các bước nối tiếp)
   - **Failure domains** (nếu component X chết, cái gì bị ảnh hưởng?)
   - **Cost scaling** (chi phí tăng tuyến tính hay phi tuyến khi scale?)

2. **Xác định Critical Path**: bước nào quyết định tổng thời gian của toàn hệ thống?

3. **Liệt kê assumptions** chưa được kiểm chứng — đây là nguồn rủi ro lớn nhất.

---

### Phase 3 — Thiết Kế Kiến Trúc

**Mục tiêu**: Đề xuất thiết kế với phân tích đầy đủ.

#### 3a. Liệt kê các phương án (Options)
Với mỗi vấn đề thiết kế quan trọng, liệt kê ít nhất 2-3 phương án. Không bỏ qua phương án đơn giản nhất (YAGNI principle).

#### 3b. So sánh phương án theo ma trận
```
| Tiêu chí           | Option A | Option B | Option C |
|--------------------|----------|----------|----------|
| Độ phức tạp        | Low      | Medium   | High     |
| Performance        | ...      | ...      | ...      |
| Khả năng maintain  | ...      | ...      | ...      |
| Chi phí            | ...      | ...      | ...      |
| Phù hợp constraint | ...      | ...      | ...      |
```

#### 3c. Đưa ra quyết định với reasoning chain
Với mỗi quyết định thiết kế quan trọng, viết theo format:

```
**[Quyết định]**: Dùng [X] thay vì [Y]
**Lý do**: [Context tại sao vấn đề này tồn tại]
**Trade-off chấp nhận**: [Y tốt hơn về A, nhưng X tốt hơn về B vốn quan trọng hơn vì...]
**Consequence**: [Hệ quả cụ thể: tốt hơn X%, cần làm thêm Y, risk Z]
**Điều kiện để review lại**: [Khi nào quyết định này có thể cần thay đổi]
```

#### 3d. Vẽ Data Flow / Component Diagram (text-based)
Dùng ASCII diagram hoặc Mermaid để mô tả luồng dữ liệu và tương tác giữa các component.

---

### Phase 4 — Kiểm Tra Tính Production-Ready

Chạy checklist theo 6 chiều trước khi finalize thiết kế:

#### [RELIABILITY]
- [ ] Single point of failure ở đâu? Có mitigation không?
- [ ] Retry strategy khi dependency fail?
- [ ] Circuit breaker / graceful degradation?

#### [PERFORMANCE]
- [ ] Bottleneck đã được identify và address?
- [ ] Có caching layer ở đúng chỗ không?
- [ ] Memory / VRAM / disk peak usage có trong ngưỡng an toàn không?

#### [OBSERVABILITY]
- [ ] Logging strategy (structured, per-component)?
- [ ] Metrics để detect khi hệ thống không khỏe mạnh?
- [ ] State tracking để resume khi crash?

#### [SECURITY]
- [ ] Input validation ở system boundary?
- [ ] Credentials/secrets quản lý như thế nào?
- [ ] Nếu crawl/call external API: rate limit, ToS compliance?

#### [OPERABILITY]
- [ ] Có thể re-run từng step riêng lẻ không?
- [ ] Có cleanup strategy cho intermediate data không?
- [ ] Rollback plan khi bước cuối fail?

#### [EVOLVABILITY]
- [ ] Source/config có được abstract để dễ swap không?
- [ ] Schema/format có backward compatible không?
- [ ] Phần nào dễ thay thế nhất khi requirement thay đổi?

---

### Phase 5 — Tổng Hợp & Output

Tạo output theo cấu trúc sau (ghi vào file kế hoạch hoặc trả lời trực tiếp):

```markdown
## Architecture Decision Record (ADR)

### Context
[Vấn đề & ràng buộc đầu vào]

### Decisions
| # | Component | Decision | Reasoning |
|---|-----------|----------|-----------|
| 1 | ...       | ...      | ...       |

### Architecture Overview
[ASCII/Mermaid diagram hoặc mô tả layered]

### Critical Path
[Sequence các bước quyết định total latency/throughput]

### Risks & Mitigations
| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|

### Open Questions
[Assumption chưa kiểm chứng, cần làm rõ]

### Review Triggers
[Điều kiện khi nào cần review lại thiết kế này]
```

---

## Tham Khảo Nhanh — Nguyên Tắc Thiết Kế

| Nguyên tắc | Khi nào áp dụng |
|------------|----------------|
| **YAGNI** (You Aren't Gonna Need It) | Chống over-engineer, ưu tiên simple before complex |
| **Single Responsibility** | Mỗi component chỉ làm 1 việc, dễ test và replace |
| **Fail Fast** | Validate input sớm nhất có thể, không để lỗi lan xa |
| **Make it easy to change** | Abstract source/config/format → swap không cần rewrite |
| **Measure before optimizing** | Không assume bottleneck, phải có số đo thực tế |
| **Explicit over implicit** | State phải được track rõ ràng (DB, log), không dùng in-memory state cho critical flow |
| **Design for resume** | Pipeline dài phải checkpoint được, crash ở bước 80 không mất công bước 1-79 |
