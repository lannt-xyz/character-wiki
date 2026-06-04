# Agent Rules

## Workflow

### For ambiguous requests

1. Clarify requirements.
2. List assumptions.
3. Wait for confirmation.

Do not implement anything before confirmation.

---

### For implementation requests

Step 1 — Analysis

Before making changes, list:

* files to modify
* functions/classes to modify
* expected change scope
* potential impact

Do not edit code during analysis.

---

Step 2 — Plan

Create a short implementation plan.

For each planned change include:

* target file
* target symbol
* reason for change

Wait for approval.

---

Step 3 — Implementation

Apply only approved changes.

Do not expand scope without approval.

---

Step 4 — Verification

Review the final diff and verify:

* no unrelated code changed
* imports remain valid
* typing remains valid
* tests remain valid

---

Step 5 — Summary

List:

* modified files
* modified functions/classes
* completed changes

Never skip approval steps.

---

## Code Review Graph

Use graph tools first only when:

* exploring unfamiliar code
* architecture analysis
* impact analysis
* code review

Skip graph tools for:

* typo fixes
* formatting changes
* small localized edits
* single-function changes

Use graph tools to gather information.

Do not use graph tools repeatedly when the required information is already available.

---

## Scope Declaration

Before editing, explicitly state:

Files to change:

* ...

Functions/classes to change:

* ...

Functions/classes NOT being changed:

* ...

If scope is unclear:
stop and ask.

---

## Read Before Edit

Before modifying code:

1. Read the target file.
2. Read directly related dependencies.
3. Understand call sites when relevant.
4. Verify the target code exists.

Never edit based on assumptions.

---

## Editing Rules

When editing:

1. Modify the smallest possible region.
2. Preserve architecture.
3. Preserve formatting.
4. Preserve comments.
5. Avoid unrelated changes.
6. Prefer patch-style edits over regeneration.

Never rewrite an entire file unless explicitly requested.

Never rewrite a file to modify a single function.

If more than 30% of a file would change:

* Stop.
* Explain why.
* Ask for confirmation.

---

## Diff Discipline

Before editing:

Identify the exact symbol to change.

Example:

* file: reviewer.py
* function: _llm_fix_shots

After editing:

List exactly what changed.

Example:

Changed:

* _llm_fix_shots

Unchanged:

* _review_script
* _validate_shots
* _build_prompt

Never modify code outside the requested scope.

---

## Edit Failure Handling

If an edit operation fails:

1. Re-read the file.
2. Re-locate the target code.
3. Retry with a smaller patch.

Maximum retry count: 2

After two failed attempts:

* Stop.
* Explain the failure.
* Ask for guidance.

Do not continue guessing.

---

## Editing Safety

Source code may only be modified through editor tools.

Never create temporary scripts solely to modify source code.

Never create files such as:

* fix.py
* patch.py
* rewrite.py
* migrate.py

for the purpose of editing existing source files.

Do not use shell commands to rewrite files.

Do not use Python scripts to rewrite files.

Do not bypass editor limitations.

Use editor tools only.

---

## Tool Discipline

Use only the tools required for the current task.

Avoid:

* repeated failed tool calls
* unnecessary graph queries
* unnecessary file reads

If the same tool fails twice:

stop and ask.

---

## Verification

After editing:

1. Review the diff.
2. Remove dead code.
3. Check imports.
4. Check typing.
5. Check tests.

Verify that no unrelated code changed.

Verify that only approved symbols were modified.

---

## Python Standards

* Package manager: uv
* HTTP: httpx
* Config: pydantic-settings
* Validation: Pydantic models
* Logging: loguru
* Retry: tenacity

External calls must use retry.

Do not use raw dictionaries for LLM outputs.

Parse structured outputs with Pydantic models.

---

Do not apply these rules to unrelated code.
