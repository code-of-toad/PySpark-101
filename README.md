# PySpark 101

A hands-on learning repository for mastering **PySpark from a data-engineering perspective**.

The project is built around a structured 18-phase curriculum that progresses from DataFrame fundamentals to distributed execution, performance engineering, testing, incremental processing, cloud Spark, warehouse integration, streaming, production engineering, and a final production-style capstone.

The goal is not to collect tutorials or memorize PySpark syntax. The goal is to develop and preserve evidence that I can **design, implement, test, debug, optimize, and explain real PySpark data pipelines**.

---

## Table of Contents

- [Learning Philosophy](#learning-philosophy)
- [Canonical Project Documents](#canonical-project-documents)
- [Curriculum](#curriculum)
- [Learning Workflow](#learning-workflow)
- [Repository Structure](#repository-structure)
- [Data Strategy](#data-strategy)
- [Current Status](#current-status)
- [Final Mastery Target](#final-mastery-target)

---

## Learning Philosophy

The project follows this progression:

```text
Write PySpark
    ↓
Build correct pipelines
    ↓
Understand Spark execution
    ↓
Diagnose performance
    ↓
Optimize
    ↓
Productionize
```

Core principles:

- Build more than I read.
- Use PySpark as a **data engineer**, not as an isolated API.
- Treat schemas and data grain as first-class concerns.
- Predict Spark behavior before execution whenever practical.
- Validate predictions with evidence such as output, query plans, partition counts, tests, and the Spark UI.
- Test business correctness, not merely whether code executes.
- Prefer maintainable DataFrame and Spark SQL patterns over one-off scripts.
- Use Git history as a record of growing capability.
- Advance only after demonstrating mastery of each phase.

---

## Canonical Project Documents

The repository uses three canonical documents:

### [`CURRICULUM.md`](CURRICULUM.md)

Defines **what I am learning**.

It contains:

- all 18 phases;
- learning objectives;
- topics;
- engineering emphasis;
- mastery requirements;
- the final capstone standard.

### [`ROADMAP.md`](ROADMAP.md)

Defines **where I currently am**.

It tracks:

- phase completion;
- phase status;
- major milestones;
- handoff readiness.

### [`WORKFLOW.md`](WORKFLOW.md)

Defines **how I work through the curriculum**.

It documents:

- one dedicated ChatGPT chat per phase;
- the standard phase learning lifecycle;
- phase starter prompts;
- exercises vs. solutions;
- prediction-before-execution;
- testing practices;
- Git practices;
- mastery gates;
- end-of-phase handoffs.

Together:

```text
CURRICULUM.md → What to learn
ROADMAP.md    → What is complete
WORKFLOW.md   → How to learn it
Git repo      → Evidence of the work
```

---

## Curriculum

| Phase | Topic |
|---|---|
| 1 | DataFrame Fundamentals, Schemas & I/O |
| 2 | Joins, Aggregations, Windows & Data Modeling |
| 3 | Spark SQL |
| 4 | Spark's Execution Model |
| 5 | Catalyst, Query Plans & AQE |
| 6 | Storage, Partitioning & Shuffle Engineering |
| 7 | PySpark Performance Engineering |
| 8 | Spark UI, Monitoring & Debugging |
| 9 | PySpark Application Architecture |
| 10 | Data Quality & Schema Enforcement |
| 11 | Testing PySpark |
| 12 | Incremental Processing & Idempotency |
| 13 | Cloud Spark |
| 14 | Warehouse Integration & Analytical Serving |
| 15 | Batch Pipeline Architecture & Orchestration |
| 16 | Structured Streaming Fundamentals |
| 17 | Modern Table Formats & Production Engineering |
| 18 | Capstone: Production-Style PySpark Data Platform |

See [`CURRICULUM.md`](CURRICULUM.md) for the full curriculum.

---

## Learning Workflow

Each curriculum phase gets its own dedicated ChatGPT chat.

A typical phase follows:

```text
Phase roadmap
    ↓
Concept + mental model
    ↓
Guided example
    ↓
Prediction
    ↓
Implementation
    ↓
Evidence / observation
    ↓
Independent exercises
    ↓
Applied mini-task
    ↓
Mastery gate
    ↓
Commit
    ↓
ROADMAP update
    ↓
Starter prompt for next phase
```

The repository remains the permanent source of truth between chats.

See [`WORKFLOW.md`](WORKFLOW.md) for the complete operating procedure.

---

## Repository Structure

The repository should grow incrementally rather than being populated with empty placeholder files in advance.

A likely structure is:

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
│   └── ...
│
├── src/
│   └── pyspark_learning/
│
├── tests/
├── sql/
├── notebooks/
│
├── data/
│   ├── raw/
│   ├── curated/
│   └── rejected/
│
└── capstone/
    ├── README.md
    ├── docs/
    ├── src/
    ├── tests/
    ├── sql/
    └── config/
```

Directories should be created when they become useful.

---

## Data Strategy

Where practical, the phases should reuse an evolving data domain instead of unrelated toy datasets.

A retail domain can include:

```text
customers
products
stores
orders
order_items
inventory_snapshots
```

The same data can progressively support:

- schema enforcement;
- transformations;
- joins and windows;
- Spark SQL;
- execution analysis;
- partitioning;
- performance experiments;
- data-quality validation;
- testing;
- incremental processing;
- cloud execution;
- warehouse integration;
- the final capstone.

Large generated datasets and Parquet outputs should generally **not** be committed to Git. Small deterministic fixtures may be committed under `tests/fixtures/`.

---

## Current Status

**Phase 1 has not started yet.**

Progress is tracked in [`ROADMAP.md`](ROADMAP.md).

---

## Final Mastery Target

By the end of the project, I should be able to take a requirement such as:

> Process hundreds of gigabytes of transactional data per day.

and reason confidently about:

- ingestion;
- schemas;
- data quality;
- data grain;
- facts and dimensions;
- PySpark transformations;
- joins and shuffles;
- jobs, stages, and tasks;
- storage and execution partitioning;
- Parquet and table formats;
- query plans;
- Spark UI diagnosis;
- performance optimization;
- skew;
- incremental processing;
- idempotency;
- testing;
- cloud execution;
- warehouse integration;
- monitoring;
- retries;
- backfills;
- production operation.

The end goal is not merely to **know PySpark syntax**.

It is to **use Spark as a data engineer**.
