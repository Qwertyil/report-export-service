# Editing workflow

When modifying existing code in this repository, follow these rules:

- Explain the intended approach before making edits.
- State important assumptions before editing.
- List every file that will be modified before making changes.
- If more than one file will change, provide a short plan first.
- In unfamiliar or other people's code, explain the relevant data flow, entry points, and affected components before editing.

# Scope control

- Prefer minimal, local changes over broad rewrites.
- Do not expand scope beyond the user's request unless explicitly approved.
- Ask before making broad refactors, architectural changes, or cross-cutting edits.
- Preserve existing style, naming, and patterns unless explicitly asked to refactor.

# Protected areas

Do not modify any of the following unless explicitly instructed:

- configuration files
- infrastructure files
- CI/CD workflows
- dependency versions
- lockfiles
- database schemas or migrations
- build or deployment settings
- environment setup

# After edits

- Summarize exactly what changed in each modified file.
- Explicitly call out risky, breaking, or non-obvious behavior changes.
- Mention assumptions that still affect the result.