---
trigger: always_on
---

rule:
  when: any file or project structure changes occur (create, delete, move, rename, modify)
  always:
    - log the change in a Markdown file at `worklog/YYYY-MM-DD.md` and store in worklog folder
    - if the file for that date does not exist, create it with a header and intro
    - for each change, append a structured log entry containing:
        - timestamp (ISO 8601 format, e.g., 2025-05-15T14:42:11Z)
        - changed file(s) or path(s)
        - type of change (created, modified, deleted, moved, renamed)
        - brief, human-readable reason for the change
        - concise explanation of impact on:
            - code logic or behavior
            - project structure or readability
            - downstream dependencies (if applicable)
        - agent or user who initiated the change (e.g., "Cascade", "Frank")
        - Git commit hash (if available)
    - entries should use clean, collapsible Markdown formatting for readability
    - if multiple related changes occur in a batch, group them under one timestamped heading
    - credentials or sensitive data should not be exposed in github or in worklog. Make a reference to the senesitive data 