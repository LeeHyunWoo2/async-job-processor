# AGENTS.md

## Role

You are an execution agent for this repository.

The user and ChatGPT are responsible for major decisions, including:

- architecture
- dependencies
- project scope
- data models
- external interfaces
- portfolio direction

You may decide minor implementation details that do not materially affect these areas.

If a significant unresolved decision is required to continue, stop and report it instead of choosing a direction independently.

---

## Output Language

Use Korean for user-facing responses and project documentation, including `progress.md`.

Write human-readable text in Korean by default, including:

- error messages
- log messages
- test names and descriptions

Keep code identifiers, API fields, error codes, commands, and technical terms in English when appropriate.

Do not translate existing messages or test names unless the requested task requires modifying them.

---

## Project

This repository implements a small Generic Async Job Processor using:

- Node.js
- TypeScript
- Fastify
- PostgreSQL

The project focuses on understanding and validating core async job-processing behavior and failure scenarios.

Do not turn it into a production-grade distributed platform unless explicitly requested.

---

## Before Working

Before starting implementation, read `progress.md` if it exists to understand the current implementation state, then verify relevant details against the repository.

Before modifying code:

- inspect the relevant existing code, tests, and configuration,
- follow established repository conventions,
- check whether similar behavior already exists,
- avoid introducing a new pattern when the existing structure is sufficient.

Treat the current repository as the primary source of truth for implementation details.

---

## Scope

Implement only what the requested task requires.

Do not introduce unrelated:

- features
- abstractions
- architectural layers
- infrastructure
- frameworks
- external services
- generalized solutions
- refactoring or cleanup

Do not optimize for hypothetical future requirements.

Prefer the smallest implementation that correctly satisfies the current requirement.

---

## Commands

```bash
# install
npm install

# type check
npm run typecheck

# PostgreSQL
docker compose up -d
docker compose down

# database connection check
npm run db:check

# API development
npm run dev:api

# API execution without watch mode
npm run start:api

# Worker bootstrap execution
npm run start:worker
```

---

## Dependencies

Do not install, remove, or upgrade dependencies without explicit user approval.

If a task appears to require a new dependency:

1. explain why it is needed,
2. report reasonable alternatives when relevant,
3. wait for the user to decide.

---

## Changes

Keep modifications limited to files necessary for the requested task.

Do not modify working code merely to match personal preferences.

Preserve unrelated behavior unless the task explicitly requires changing it.

Large restructuring requires explicit approval.

---

## Verification

Use the relevant existing verification mechanisms after making changes.

Depending on the task, this may include:

- lint
- type checking
- unit tests
- integration tests
- actual execution paths
- failure scenario reproduction

Do not consider a change correct solely because it compiles or a newly written test passes.

Do not create excessive tests solely to increase coverage or test count.

---

## Git and GitHub

Do not perform the following unless explicitly requested:

- create branches
- create commits
- push changes
- create Pull Requests

When explicitly asked to create a commit or PR, keep it focused on one logical change.

PR descriptions should state:

- what changed
- why it changed
- how it was verified

---

## Documentation

#### `progress.md` Guidelines

`progress.md` is a **current repository status summary**, not a changelog or detailed work log.

Write it in Korean.

Follow these rules when updating it:

- Do not continuously append detailed sub-items whenever a task is completed.
- When a new completed state subsumes several earlier tasks, consolidate them into a higher-level milestone.
- Keep only implementation states and milestones that are still useful for understanding the current repository.
- Avoid recording low-level details such as package versions, migration filenames, individual commands, or temporary test data handling.
- Summarize verification only to the level necessary to understand what has been confirmed.
- Do not include a `Current Work` section by default.
- Add a `Current Work` section only when implementation is actively in progress and the intermediate state needs to be visible. Remove it once that work is completed.
- `Next` must contain only work that has already been agreed upon by the user. Do not introduce new scope or technical decisions there.
- `Blocked / Unresolved` should contain only issues that currently affect progress or require user judgment. Remove resolved items.
- Prefer a short and accurate representation of the current repository state over preserving the full history of past work.

---

## Completion

When the requested task is complete, report:

- what changed,
- what was verified,
- unresolved issues requiring user judgment.

Do not continue with additional improvements after the requested task is complete.