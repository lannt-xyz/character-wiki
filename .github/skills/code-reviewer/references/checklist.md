# Full Review Checklist

Reference for Phase 2 of the code-reviewer skill. Check all applicable items for each file.

---

## 1. Correctness

- [ ] All conditions (`if`, `while`, comparisons) are logically correct
- [ ] Off-by-one errors in loops, slices, pagination
- [ ] Return values are always valid; no accidental `None` returns on non-None return type
- [ ] Boolean logic is not inverted (`not x or y` vs `not (x or y)`)
- [ ] Edge cases handled: empty list, zero, None, empty string, single-element
- [ ] Mutating arguments that should not be mutated (esp. default mutable args `def f(x=[])`)
- [ ] Correct variable used (not stale or from a neighboring scope)
- [ ] Database queries filter on the right fields; no wrong joins
- [ ] Correct operator: `=` vs `==`, `is` vs `==` for None checks
- [ ] Async functions are awaited; no missing `await`

---

## 2. Security (OWASP Top 10)

- [ ] **Injection**: No SQL string concatenation — use parameterized queries / ORM
- [ ] **XSS**: User input is escaped before being rendered in HTML templates
- [ ] **Command Injection**: No `os.system(user_input)`, `subprocess` with shell=True and user data
- [ ] **Broken Auth**: Auth checks present before accessing protected resources
- [ ] **Sensitive Data Exposure**: No secrets, API keys, passwords in source code or logs
- [ ] **SSRF**: URLs fetched from external input are validated against an allowlist
- [ ] **Insecure Deserialization**: No `pickle.loads` or `eval` on untrusted data
- [ ] **Path Traversal**: File paths from user input are sanitized (`pathlib`, `os.path.abspath`)
- [ ] **Dependency issues**: No import of removed/unofficial packages
- [ ] **Logging secrets**: Passwords, tokens not logged even accidentally

---

## 3. Python Quality

- [ ] No wildcard imports (`from x import *`)
- [ ] No unused imports (remove after any code change)
- [ ] No unused local variables (prefix with `_` if intentional)
- [ ] No shadowing of built-ins (`id`, `list`, `type`, `input`, `filter`, etc.)
- [ ] f-strings preferred over `.format()` or `%` formatting
- [ ] `isinstance()` used instead of `type(x) ==`
- [ ] `pathlib.Path` used instead of string path manipulation where appropriate
- [ ] Generator expressions used for large sequences (not list comprehensions)
- [ ] No bare `except:` — always catch specific exceptions
- [ ] Deprecated APIs replaced: `from_orm` → `model_validate`, etc.
- [ ] Type hints on function signatures for public/shared functions
- [ ] Dataclasses or Pydantic models instead of plain dicts for structured data

---

## 4. Error Handling & Logging

- [ ] Exceptions from external I/O (DB, LLM, HTTP) are caught and logged
- [ ] Log messages contain enough context: which stock ticker, which step, which input
- [ ] No silent `except Exception: pass` without at minimum a `logger.warning`
- [ ] Retries don't stack on top of existing retry logic in `app/utils/`
- [ ] `finally` block used when resource cleanup is necessary (DB sessions, file handles)
- [ ] Error propagation is intentional — not accidentally swallowed mid-stack

---

## 5. Performance

- [ ] No N+1 queries (SQLAlchemy: use `joinedload` / batch queries)
- [ ] Expensive function calls inside loops that could be hoisted outside
- [ ] `@cached_data` applied where appropriate for expensive I/O
- [ ] Unnecessary re-computation of the same value in a loop
- [ ] Large DataFrames: operations are vectorized (NumPy/Pandas), not row-by-row Python loops
- [ ] No blocking sync I/O inside async functions (use `asyncio.to_thread` or async libs)

---

## 6. Maintainability

- [ ] No dead code (unreachable branches, commented-out blocks)
- [ ] Function/method does one thing — no god functions
- [ ] Magic numbers extracted to named constants
- [ ] Deeply nested code (> 3 levels) refactored if clearly complex
- [ ] Duplicated logic that already exists as a helper elsewhere
- [ ] Variable and function names are self-explanatory

---

## 7. Project Conventions (vn-stock-analytics)

- [ ] Session: `next(get_session())` → `try/finally session.close()`
- [ ] ORM: `session.exec(select(...))` not `session.execute(...)`
- [ ] Pydantic: `model_validate()` not `from_orm()`
- [ ] Config: values from `app/config.py` / env — no hardcoded credentials
- [ ] Prompt: if template changed → downstream parser updated
- [ ] Async: `asyncio.TaskGroup` pattern, no sync I/O mixed in
- [ ] Crawler: implements `BaseCrawler`, returns proper types from `app/model/models.py`
- [ ] ML: chronological order preserved; no future data in training set

---

## 8. Testing Readiness (suggest only, don't create unless asked)

- [ ] Critical-path functions are unit-testable (no hidden global state)
- [ ] Side effects (DB, HTTP, LLM) are injectable/mockable
- [ ] Pure computation functions return deterministic results
- [ ] Edge cases (empty input, maximal input) would be exercised by obvious test cases
