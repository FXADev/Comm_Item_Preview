---
trigger: always_on
---


## 🔧 Parameters

- `project_context_file`: *default* = `ai_project_context.md`
- `worklog_path`: *default* = `worklog.md`
- `worklog_entry_count`: *default* = 2

---

## 🎯 **Trigger**

- On every **new chat session**
- When the AI agent reports missing or insufficient project context

---

## 🪜 **Required Steps**

1. **Load Project Context**
   - Read and parse the `project_context_file`.  
   - If missing, **log a warning** and prompt the user to provide this file.

2. **Scan Recent Worklog Entries**
   - Locate the file(s) at `worklog_path`.  
   - Read the most recent `worklog_entry_count` entries, sorted by timestamp if available.
   - If missing, log a warning and prompt the user to provide a worklog or recent status update.

3. **Build/Update Agent Context**
   - Merge insights from both sources into an internal summary.
   - If the context hasn’t changed since the last session, note that and avoid redundant prompts.

4. **Confirm and Summarize**
   - Output a brief summary of the context loaded and key recent activities:
     > "Loaded project context from `ai_project_context.md`. Recent worklog: [summary]."
   - **Ask the user** if priorities or project status have shifted.

---

## ❗ **Fallback Behavior**

- If either file is missing, politely prompt the user for:
  - Project overview (if no context file)
  - Most recent progress or issues (if no worklog)
- Continue only after context is sufficient.
