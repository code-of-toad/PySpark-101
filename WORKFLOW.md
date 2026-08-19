# PySpark 101 — Learning Workflow

## Purpose

This repository is a structured, hands-on record of learning **PySpark for data engineering**.

The objective is not to collect notes or memorize APIs. The repository should progressively demonstrate the ability to:

- write correct PySpark transformations;
- reason about data grain and schemas;
- use Spark SQL and the DataFrame API;
- understand distributed execution;
- inspect and optimize query plans;
- diagnose Spark performance;
- test data pipelines;
- build reliable incremental workflows;
- run Spark in cloud environments;
- integrate Spark with warehouses;
- design and operate production-style data pipelines.

The repository should serve as **evidence of growing PySpark/data-engineering capability**, not merely as a tutorial archive.

---

# 1. Canonical Project Files

Use the following files as the persistent source of truth for the learning project.

```text
PySpark-101/
├── README.md
├── CURRICULUM.md
├── ROADMAP.md
├── WORKFLOW.md
└── ...
```

## `CURRICULUM.md`

The canonical learning curriculum.

It defines:

- the phases;
- learning objectives;
- topics;
- mastery requirements;
- the final capstone standard.

Treat it as stable unless the curriculum is deliberately revised.

## `ROADMAP.md`

The canonical progress tracker.

Example:

```markdown
# Progress

- [ ] Phase 1 — DataFrame Fundamentals, Schemas & I/O
- [ ] Phase 2 — Joins, Aggregations, Windows & Data Modeling
- [ ] Phase 3 — Spark SQL
- [ ] Phase 4 — Spark's Execution Model
- [ ] Phase 5 — Catalyst, Query Plans & AQE
- [ ] Phase 6 — Storage, Partitioning & Shuffle Engineering
- [ ] Phase 7 — PySpark Performance Engineering
- [ ] Phase 8 — Spark UI, Monitoring & Debugging
- [ ] Phase 9 — PySpark Application Architecture
- [ ] Phase 10 — Data Quality & Schema Enforcement
- [ ] Phase 11 — Testing PySpark
- [ ] Phase 12 — Incremental Processing & Idempotency
- [ ] Phase 13 — Cloud Spark
- [ ] Phase 14 — Warehouse Integration & Analytical Serving
- [ ] Phase 15 — Batch Pipeline Architecture & Orchestration
- [ ] Phase 16 — Structured Streaming Fundamentals
- [ ] Phase 17 — Modern Table Formats & Production Engineering
- [ ] Phase 18 — Capstone: Production-Style PySpark Data Platform
```

## `WORKFLOW.md`

This file.

It defines **how the curriculum should be worked through** and how ChatGPT, Git, exercises, tests, documentation, and phase handoffs should be used.

---

# 2. Recommended Repository Structure

Build the repository incrementally.

Do **not** create hundreds of empty placeholder files in advance.

Create a phase directory when that phase begins.

```text
PySpark-101/
│
├── README.md
├── CURRICULUM.md
├── ROADMAP.md
├── WORKFLOW.md
├── .gitignore
├── requirements.txt
│
├── docs/
│   ├── setup.md
│   ├── spark_mental_model.md
│   ├── common_patterns.md
│   ├── performance_playbook.md
│   └── glossary.md
│
├── phases/
│   ├── phase_01_dataframe_fundamentals/
│   │   ├── README.md
│   │   ├── exercises/
│   │   ├── solutions/
│   │   └── notes.md
│   │
│   ├── phase_02_joins_aggregations_windows/
│   │   ├── README.md
│   │   ├── exercises/
│   │   ├── solutions/
│   │   └── notes.md
│   │
│   └── ...
│
├── src/
│   └── pyspark_learning/
│
├── tests/
│
├── data/
│   ├── raw/
│   ├── curated/
│   └── rejected/
│
├── sql/
│
├── notebooks/
│
└── capstone/
    ├── README.md
    ├── docs/
    ├── src/
    ├── tests/
    ├── sql/
    └── config/
```

The exact structure may evolve as the project grows. Avoid unnecessary abstraction or empty folders that do not yet serve a purpose.

---

# 3. One Dedicated ChatGPT Chat Per Phase

Use a distinct ChatGPT chat for every curriculum phase.

Example:

```text
Phase 1 chat
Phase 2 chat
Phase 3 chat
...
Phase 18 chat
```

Each phase chat should:

- focus primarily on that phase;
- build on committed repository work from earlier phases;
- avoid redoing completed material unnecessarily;
- end with an explicit mastery gate;
- generate a starter prompt for the next phase.

The repository, `CURRICULUM.md`, `ROADMAP.md`, and this file preserve continuity between chats.

---

# 4. Standard Phase Learning Lifecycle

Every phase should follow approximately this sequence:

```text
Concept
   ↓
Mental model
   ↓
Guided example
   ↓
Hands-on exercise
   ↓
Independent problem
   ↓
Applied mini-task
   ↓
Mastery questions
   ↓
Mastery gate
   ↓
Commit
   ↓
Next phase
```

A phase is **not complete merely because the material was explained**.

Completion requires evidence that the concepts can be:

- explained;
- implemented;
- reasoned about;
- debugged;
- applied to realistic data-engineering problems.

---

# 5. Standard Phase Chat Starter

Start each dedicated phase chat with the following template.

Replace the placeholders with the appropriate phase number and title.

For Phase 1, replace the statement about completed phases with:

> **This is the first phase. No curriculum phases have been completed yet.**

---

## Phase Chat Starter Template

```markdown
# Phase [N] — [PHASE TITLE]

Continue my **PySpark for Data Engineering** learning curriculum from **Phase [N] — [PHASE TITLE]**.

Treat:

- `CURRICULUM.md` as the **canonical learning curriculum**;
- `ROADMAP.md` as the **canonical progress tracker**;
- `WORKFLOW.md` as the **canonical learning workflow**;
- the existing GitHub repository as the **source of truth for all work already completed**.

**Phases 1–[N-1] are complete.** Do not redo earlier phases unless the current phase specifically requires building on or reviewing them.

## How I want this phase taught

Teach this phase from the perspective of a **data engineer using PySpark professionally**, not as an abstract Spark course.

For this phase:

1. First explain what I am going to learn, why it matters in data engineering, and how it connects to previous phases.
2. Break the phase into logical, manageable sections rather than teaching everything at once.
3. For each new concept:
   - explain the mental model concisely;
   - show a small practical example where useful;
   - give me hands-on work to perform;
   - have me reason about the result rather than merely copy code.
4. Prefer using and extending the repository's existing datasets, code, and pipeline instead of introducing unrelated toy examples.
5. When Spark execution behavior is relevant, ask me to **predict what Spark will do before we run it**, then compare my prediction against evidence such as:
   - output;
   - schemas;
   - partition counts;
   - `explain()` plans;
   - Spark UI information;
   - tests.
6. Emphasize:
   - DataFrame and Spark SQL proficiency;
   - schema correctness;
   - data grain;
   - joins and transformations;
   - distributed execution;
   - data quality;
   - testing;
   - performance;
   - idempotency and reliability;
   - maintainable data-engineering practices,

   wherever they are relevant to this phase.
7. Do not spend significant time on RDD programming, obscure configuration, or advanced internals unless `CURRICULUM.md` specifically requires them here.
8. Do not give me the complete solution to an exercise before I have had a reasonable opportunity to attempt it, unless I explicitly ask for the solution.
9. When I show you code, output, errors, screenshots, query plans, or Spark UI information, use that evidence to guide the next step rather than assuming what happened.
10. Keep implementation decisions consistent with work already committed to the repository.

## Repository expectations

Any code worth preserving should ultimately live in an appropriate repository file rather than only existing as conversational or notebook code.

As we work, help me maintain good separation between:

- source code;
- exercises;
- tests;
- SQL;
- configuration;
- documentation;
- generated data.

Do not create unnecessary files or abstractions merely for the sake of structure.

For every file we create or modify, explain briefly:

- what it is for;
- why it belongs where it does;
- how it relates to the current phase.

## Mastery requirement

Do **not** consider Phase [N] complete merely because we have covered the material.

Before completion, test whether I can independently:

- explain the important concepts;
- implement the required PySpark behavior;
- reason about what Spark is doing;
- diagnose relevant mistakes or edge cases;
- apply the concepts to a realistic data-engineering problem.

Use the mastery requirements in `CURRICULUM.md` as the minimum standard.

## At the end of the phase

Once the mastery gate has been passed, provide:

1. concise answers to the phase's mastery questions;
2. a summary of what I can now do that I could not do before this phase;
3. the repository files added or materially changed;
4. a concise Git commit message for the completed phase;
5. the appropriate `ROADMAP.md` progress update;
6. a ready-to-copy prompt starter for the **next phase's dedicated ChatGPT chat**.

Begin by reviewing **Phase [N] in `CURRICULUM.md`**, `WORKFLOW.md`, and the relevant current repository state. Then give me the roadmap for this phase before we start the first section.
```

---

# 6. Reuse an Evolving Dataset

Prefer one coherent domain across many phases rather than unrelated toy datasets.

A retail domain works well:

```text
customers
products
stores
orders
order_items
inventory_snapshots
```

Example progression:

```text
Phase 1
Read, type, clean, and write source datasets
        ↓
Phase 2
Join, aggregate, window, and model them
        ↓
Phase 3
Reimplement transformations with Spark SQL
        ↓
Phase 4
Predict jobs, stages, tasks, and shuffles
        ↓
Phase 5
Inspect query plans
        ↓
Phase 6
Study storage and partition behavior
        ↓
Phase 7
Optimize the pipeline
        ↓
...
        ↓
Phase 18
Rebuild a production-style system independently
```

The dataset and pipeline should become progressively more sophisticated as the curriculum advances.

---

# 7. Exercises and Solutions Must Be Separate

Keep exercises and completed/reference solutions distinct.

Example:

```text
phase_02_joins_aggregations_windows/
├── exercises/
│   ├── joins.py
│   ├── aggregations.py
│   └── windows.py
│
└── solutions/
    ├── joins_solution.py
    ├── aggregations_solution.py
    └── windows_solution.py
```

Recommended workflow:

1. Receive the problem.
2. Attempt it independently.
3. Run the implementation.
4. Inspect the result.
5. Debug mistakes.
6. Compare against expected behavior.
7. Review a solution only after a reasonable attempt.

ChatGPT should not immediately reveal full solutions unless explicitly requested.

---

# 8. Prefer Problems Over Passive Lessons

Learning should increasingly be driven by implementation.

Example progression for joins:

```text
Level 1
Perform a basic customer/order join.

Level 2
Preserve customers with no orders.

Level 3
Use an anti join to find invalid foreign keys.

Level 4
Prevent a many-to-many join from multiplying revenue.

Level 5
Given several DataFrames with different grains,
design the transformation independently.
```

The difficulty should increase throughout a phase.

---

# 9. Prediction Before Execution

Whenever Spark behavior is relevant, use:

```text
Prediction
    ↓
Experiment
    ↓
Observation
    ↓
Explanation
```

Before executing an operation, predict things such as:

- Will this trigger an action?
- Will this produce a shuffle?
- Is the transformation narrow or wide?
- Will partition counts change?
- Where might stages split?
- Which join strategy might Spark choose?
- What should appear in `explain()`?
- What should be visible in the Spark UI?

Then execute the code and reconcile the prediction with evidence.

This feedback loop is central to building a correct Spark mental model.

---

# 10. Start Testing Early

Do not wait until the dedicated testing phase before writing any tests.

As soon as meaningful transformation logic exists, begin accumulating tests.

Examples:

```python
def test_completed_orders_only(...):
    ...

def test_sales_grain_is_order_item(...):
    ...

def test_customer_join_does_not_multiply_rows(...):
    ...

def test_net_sales_calculation(...):
    ...
```

Progress from asking:

> Does the Spark code run?

to asking:

> Does the pipeline produce correct data?

The dedicated testing phase should deepen and systematize this practice.

---

# 11. Keep Notebooks Secondary

Notebooks may be used for:

- experimentation;
- exploration;
- quick demonstrations;
- inspecting Spark behavior.

But anything worth preserving as part of the project should ultimately become structured repository code.

Recommended lifecycle:

```text
Notebook
   ↓
Experiment
   ↓
Understand
   ↓
Refactor
   ↓
src/
```

Avoid turning the repository into a collection of large, disconnected notebooks.

---

# 12. Generated Data Should Usually Not Be Committed

Git should primarily preserve:

- source code;
- schemas;
- tests;
- SQL;
- documentation;
- configuration templates;
- small deterministic fixtures.

Avoid committing large generated datasets or Parquet outputs.

A `.gitignore` may include patterns such as:

```gitignore
.venv/
__pycache__/
.pytest_cache/

data/raw/*
data/curated/*
data/rejected/*

*.parquet
```

If small CSV files are intentionally used as deterministic test fixtures, store them explicitly under something like:

```text
tests/fixtures/
```

and allow those files to be committed.

Do not use a blanket `*.csv` ignore if CSV fixtures are part of the repository.

---

# 13. Git History Is Part of the Learning Record

Use meaningful commits as learning milestones.

Examples:

```text
phase 01: add explicit schemas for retail sources
phase 01: implement null and type cleaning exercises
phase 01: add parquet read and write exercises
phase 01: complete mastery exercises
```

Later:

```text
phase 06: demonstrate repartition vs coalesce
phase 06: validate parquet partition pruning
phase 06: document shuffle behavior
```

Prefer:

- small coherent commits;
- descriptive commit messages;
- a clean `main` history.

A separate branch for every lesson is unnecessary unless experimentation specifically benefits from branching.

---

# 14. Mastery Gates

Do not advance automatically.

Each phase should conclude with a mixture of:

- coding exercises;
- conceptual questions;
- debugging questions;
- prediction questions;
- design questions;
- an applied mini-task.

Examples for Spark execution:

- What triggers this job?
- Which transformations are narrow?
- Which are wide?
- Where is a shuffle likely?
- Why might a stage boundary appear here?

Examples for performance:

- Which stage is slow?
- Is skew present?
- Is the join strategy appropriate?
- Is the pipeline reading unnecessary data?
- Would repartitioning help or hurt?
- What evidence supports the diagnosis?

A mastery gate is passed only when the phase can be applied independently with reasonable confidence.

---

# 15. Living Cross-Phase Documentation

Maintain a small number of documents that evolve throughout the curriculum.

Recommended:

```text
docs/
├── spark_mental_model.md
├── common_patterns.md
├── performance_playbook.md
└── glossary.md
```

## `spark_mental_model.md`

Continuously refine understanding of:

- driver;
- executors;
- partitions;
- jobs;
- stages;
- tasks;
- shuffles;
- Catalyst;
- AQE.

## `common_patterns.md`

Record reusable data-engineering patterns such as:

- latest row per key;
- deduplication;
- anti-join validation;
- conditional aggregation;
- grain-safe joins;
- incremental loading.

## `performance_playbook.md`

Eventually organize common Spark symptoms into a diagnostic reference.

Example:

```text
Symptom:
One task is much slower than the others

Investigate:
Partition sizes and key distribution

Possible cause:
Data skew / hot key

Possible responses:
Repartitioning, salting, AQE skew handling,
or redesigning the transformation
```

## `glossary.md`

Maintain short definitions of important Spark/data-engineering terminology encountered during the curriculum.

---

# 16. The Capstone Should Grow Out of Earlier Work

Do not treat Phase 18 as a completely unrelated project.

Earlier phases should progressively create skills, code, data, tests, and design patterns that contribute to the final capstone.

Think:

```text
Phase 1 ──┐
Phase 2 ──┤
Phase 3 ──┤
...       ├──→ Capstone
Phase 17 ─┘
```

rather than:

```text
Phases 1–17
      ↓
Completely unrelated Phase 18 project
```

Phase 18 should test whether the entire system can now be designed and built independently.

---

# 17. End-of-Phase Handoff

At the end of each phase, preserve enough information that the next chat can continue without relying on conversational memory.

Record:

- what was implemented;
- important design decisions;
- observed Spark behavior;
- unresolved questions;
- files created or changed;
- mastery results;
- `ROADMAP.md` status.

Then generate the next phase's starter prompt.

The handoff chain should look like:

```text
Phase 1 chat
    ↓
Phase 2 starter
    ↓
Phase 2 chat
    ↓
Phase 3 starter
    ↓
...
    ↓
Phase 18
```

The repository remains the persistent source of truth.

---

# 18. Repository Quality Standard

The repository should not ultimately resemble:

```text
18 folders of tutorials and copied examples
```

It should show:

```text
Basic DataFrame exercises
        ↓
Increasingly sophisticated transformations
        ↓
Tested reusable PySpark components
        ↓
Distributed execution experiments
        ↓
Query-plan analysis
        ↓
Performance investigations
        ↓
Incremental and cloud pipelines
        ↓
Production-style capstone
```

A reader browsing the repository and its Git history should be able to observe a clear progression from PySpark fundamentals to professional data-engineering practice.

---

# 19. Guiding Principles

Throughout the project:

1. **Build more than you read.**
2. **Predict Spark behavior before execution whenever possible.**
3. **Use evidence instead of guesses.**
4. **Treat data grain as a first-class concern.**
5. **Test business correctness, not only code execution.**
6. **Keep transformations separate from infrastructure concerns.**
7. **Prefer reusable DataFrame/Spark SQL patterns over one-off scripts.**
8. **Understand the cause of performance problems before tuning configuration.**
9. **Make reruns and incremental processing safe.**
10. **Keep the repository—not any single ChatGPT conversation—as the permanent project record.**
11. **Do not advance simply because material was explained; advance after mastery is demonstrated.**
12. **Use the final capstone to integrate the full curriculum rather than introduce a disconnected project.**
