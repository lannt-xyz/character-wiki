---
name: code-reviewer
description: "**WORKFLOW SKILL** — Agent-generated code reviewer. Use for: reviewing code created by AI agents; auditing PRs or new implementations; post-implementation quality gate; finding bugs, security issues, style violations, dead code, missing error handling; producing structured issue list then fixing all issues. Triggers: 'review code', 'review this', 'code review', 'kiểm tra code', 'review lại', 'audit', 'check quality', 'post-review', 'fix issues'."
argument-hint: "Optional: specify file path(s) or scope to review (default: last agent-generated changes)"
---

# Code Reviewer

## Purpose

Systematically review code produced by AI agents or developers, produce a **structured issue list**, then fix all identified issues in order of severity.

---

## When to Use

- After an agent implements a feature or bug fix
- Before committing agent-generated code
- When performing a quality gate check on changed files
- When the user says "review this", "kiểm tra code", "review lại", etc.

---

## Workflow (5 Phases)

### Phase 1 — Identify Scope

1. **Ask the user how to scope the diff** (unless already specified in the argument):

   > "Sếp muốn review theo cách nào?"
   > - **A) So sánh với branch** — e.g. `main`, `develop` → sẽ dùng `git diff <branch>`
   > - **B) So sánh với commit cụ thể** — e.g. `HEAD~1`, `abc1234` → sẽ dùng `git diff <commit>`
   > - **C) Staged changes** — những gì đã `git add` nhưng chưa commit → `git diff --cached`
   > - **D) Working tree** — tất cả thay đổi chưa staged → `git diff`
   > - **E) Chỉ định file thủ công** — sếp tự liệt kê file/folder cần xem

2. Run the appropriate `git diff` command to get the list of changed files, then confirm the file list with the user before proceeding.

3. Read each file fully before reviewing.

4. Note the file's role in the codebase (model, controller, utility, etc.)

### Phase 2 — Static Analysis & Issue Detection

Go through the [full review checklist](./references/checklist.md) for each file. Check every category:

- **Correctness**: logic errors, off-by-one, wrong conditions, unhandled edge cases
- **Security**: OWASP Top 10, injection, exposed secrets, auth bypass
- **Python quality**: imports, type hints on public APIs, deprecated patterns, naming
- **Project conventions**: session pattern, prompt handling, async rules (see checklist)
- **Performance**: N+1 queries, unnecessary loops, missing caching on expensive I/O
- **Maintainability**: dead code, unused imports/variables, overly complex logic
- **Error handling**: silent exceptions, missing logs, unhandled external I/O failures
- **Tests**: missing coverage for critical paths (suggest, don't create unless asked)

### Phase 3 — Log Issues

After scanning, compile all issues into a structured list **before making any changes**:

```
## Issues Found

### [CRITICAL] <Short Title>
- File: `path/to/file.py`, Line: XX
- Problem: <clear description of what is wrong>
- Fix: <what needs to change>

### [HIGH] <Short Title>
- File: `path/to/file.py`, Line: XX
- Problem: ...
- Fix: ...

### [MEDIUM] <Short Title>
...

### [LOW / STYLE] <Short Title>
...
```

Severity levels:
| Level | Meaning |
|-------|---------|
| `CRITICAL` | Bug that causes crash, data loss, or security breach |
| `HIGH` | Logic error, broken feature, or security weakness |
| `MEDIUM` | Bad pattern, missing error handling, performance issue |
| `LOW` | Style, naming, dead code, unnecessary complexity |

> Present the issue list to the user. Pause briefly so they can add/remove items if desired.

### Phase 4 — Fix Issues

Fix in order: `CRITICAL → HIGH → MEDIUM → LOW`.

Rules during fixing:
- Make **minimal, targeted changes** — do not refactor surrounding code
- Remove unused imports/variables introduced by the fix
- Do not add docstrings or comments unless the logic is non-obvious
- Do not change code that is not related to any listed issue
- After each fix, mentally re-check: did this introduce new issues?

### Phase 5 — Verification

After all fixes are applied:
1. Re-read each changed file
2. Confirm every issue from Phase 3 is resolved
3. Check no new issues were introduced
4. Run `get_errors` tool to verify no compile/lint errors
5. Report final summary:

```
## Review Summary
- Files reviewed: N
- Issues found: N (critical: X, high: X, medium: X, low: X)
- Issues fixed: N
- Remaining (deferred): N (reason)
```

---

## Hard Rules (Never Violate)

1. **Read before touching** — Never edit a file you haven't fully read
2. **No scope creep** — Only fix what was listed; do not improve unrelated code
3. **No silent swallowing** — Never add `except: pass` or suppress exceptions without logging
4. **Security first** — CRITICAL and HIGH security issues must be fixed, no deferral
5. **Log meaningful context** — Errors involving external I/O must log relevant state
6. **No fake fixes** — Do not mark an issue as fixed without actually changing code
7. **Correctness > style** — Fix logic bugs before cleaning up style violations
8. **Preserve existing tests** — Do not break test expectations when fixing issues

---

## Project-Specific Rules (vn-stock-analytics)

- Session pattern: always `next(get_session())` + `try/finally session.close()` — never `with get_session()`
- Use `session.exec()`, never `session.execute()`
- Use `model_validate()` not `from_orm()` (deprecated)
- Secrets must come from env/config — never hardcoded
- External I/O (LLM, crawlers) must use existing retry/backoff from `app/utils/`
- Caching: respect `@cached_data` on expensive calls; do not bypass without reason
- Async: do not mix sync I/O into async flows; follow existing `asyncio.TaskGroup` patterns
- ML pipeline: preserve chronological order; no data leakage into train set
- Prompt changes: if output structure changes → update parser logic too

---

## Reference Files

- [Full Review Checklist](./references/checklist.md) — Detailed per-category checklist
