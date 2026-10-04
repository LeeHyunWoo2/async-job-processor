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

Use Korean for user-facing responses.

Write `progress.md` in Korean.

Keep code, identifiers, commands, logs, and technical terms in their natural form when appropriate.

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

### `progress.md`

```text
./progress.md
```

Update `progress.md` only when your work materially changes the actual implementation state.

Write it in Korean.

It may contain:

- completed work
- current work
- already-established next work
- blockers
- unresolved issues

Do not use `progress.md` to introduce new technical decisions, project goals, or scope.

Code review, investigation, or discussion alone does not require updating it unless the implementation state changed.

### Other project documents

Treat project context, goals, decisions, interview notes, and `AGENTS.md` as read-only unless explicitly instructed to modify them.

Do not rewrite project direction based on your own assessment.

---

## Completion

When the requested task is complete, report:

- what changed,
- what was verified,
- unresolved issues requiring user judgment.

Do not continue with additional improvements after the requested task is complete.