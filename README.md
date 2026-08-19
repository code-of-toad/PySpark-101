# PySpark 101

A hands-on learning repository for mastering **PySpark from a data-engineering perspective**.

The project follows an 18-phase curriculum that progresses from DataFrame fundamentals to distributed execution, performance engineering, testing, incremental processing, cloud Spark, warehouse integration, streaming, production engineering, and a final production-style capstone.

The goal is not to collect tutorials or memorize PySpark syntax. The goal is to build enough understanding and implementation evidence to **design, implement, test, debug, optimize, and explain real PySpark data pipelines independently**.

---

## Table of Contents

- [Learning Philosophy](#learning-philosophy)
- [Canonical Project Documents](#canonical-project-documents)
- [Dependencies & Virtual Environment](#dependencies--virtual-environment)
- [Curriculum](#curriculum)
- [Phase Learning Model](#phase-learning-model)
- [Phase README Standard](#phase-readme-standard)
- [Repository Structure](#repository-structure)
- [Repository Conventions](#repository-conventions)
- [Data Strategy](#data-strategy)
- [Git and Progress Tracking](#git-and-progress-tracking)
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
- Treat schemas, grain, correctness, and reliability as first-class concerns.
- Prefer practical implementation over passive study.
- Use evidence such as outputs, tests, query plans, partition counts, Spark UI observations, and repository state when useful.
- Preserve worthwhile implementation in the repository.
- Avoid unnecessary files, abstractions, and placeholder structure.
- Advance only after satisfying the mastery requirements for the current phase.

The learning process should be **structured enough to preserve rigor, but flexible enough to move quickly through material that does not need prolonged exercises**.

---

## Canonical Project Documents

The project uses these sources of truth:

### [`CURRICULUM.md`](CURRICULUM.md)

Defines **what must be learned**.

It contains:

- all 18 curriculum phases;
- learning objectives;
- required topics;
- engineering emphasis;
- mastery requirements;
- the final capstone standard.

`CURRICULUM.md` is the authoritative standard for whether a phase is complete.

### [`ROADMAP.md`](ROADMAP.md)

Defines **what has actually been completed**.

It tracks:

- current phase;
- phase completion;
- mastery status;
- major milestones;
- completion notes.

### [`README.md`](README.md)

Defines the **project-wide conventions and phase invariants**.

It documents:

- how phase work is organized;
- repository structure;
- documentation standards;
- source/data conventions;
- Git expectations;
- the flexible learning model used across phases.

### GitHub Repository

The repository is the source of truth for **implementation that has actually been preserved**.

Together:

```text
CURRICULUM.md → What must be learned
ROADMAP.md    → What has been completed
README.md     → Project-wide conventions
Git repo      → Evidence of completed work
```

`WORKFLOW.md` is not part of the active project model and should not be relied on.

---

## Dependencies & Virtual Environment

Project dependencies are declared in [`requirements.txt`](requirements.txt).

The repository currently pins:

```text
pyspark==4.2.0
```

Use a project-local virtual environment so PySpark and future Python dependencies remain isolated from the system Python installation.

### Create the virtual environment

From the repository root:

```powershell
python -m venv .venv
```

### Activate it

**Windows PowerShell**

```powershell
.\.venv\Scripts\Activate.ps1
```

**Windows Command Prompt**

```cmd
.venv\Scripts\activate.bat
```

**macOS / Linux**

```bash
source .venv/bin/activate
```

### Install dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Verify the environment

```powershell
python -c "import sys; print(sys.executable)"
python -c "import pyspark; print(pyspark.__version__)"
```

The Python executable should resolve inside `.venv`, and the PySpark version should match `requirements.txt`.

### Deactivate

```powershell
deactivate
```

The `.venv/` directory is intentionally excluded from Git. Commit `requirements.txt`, not the local virtual environment.


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

See [`CURRICULUM.md`](CURRICULUM.md) for the full curriculum and mastery requirements.

---

## Phase Learning Model

Use **one dedicated ChatGPT chat per curriculum phase**.

Each phase begins with a comprehensive but efficient set of Markdown notes covering the full curriculum scope for that phase.

After the phase notes are created, the learning path is intentionally flexible.

Depending on the material, the phase may involve:

- implementation;
- exercises;
- debugging;
- prediction-before-execution;
- design questions;
- quizzes;
- applied data-engineering tasks;
- code review;
- query-plan analysis;
- Spark UI inspection;
- testing;
- repository work;
- focused conceptual discussion.

Not every topic requires the same learning method.

The phase should be allowed to move quickly when understanding is already strong and go deeper when implementation, debugging, or reasoning reveals a weakness.

A typical phase may therefore look like:

```text
Comprehensive phase notes
        ↓
Flexible study / implementation
        ↓
Targeted exercises where useful
        ↓
Applied work
        ↓
Mastery check
        ↓
Preserve worthwhile repo work
        ↓
ROADMAP update
        ↓
Commit
        ↓
Next phase
```

The only hard requirement is:

> **Do not mark a phase complete until the mastery requirements in `CURRICULUM.md` have been satisfied.**

---

## Phase README Standard

Every curriculum phase should have a substantive phase README, for example:

```text
phases/
└── phase_01_dataframe_fundamentals/
    └── README.md
```

The phase README should serve as the main reference for that phase and should be created near the beginning of the phase.

It should contain:

- the phase objective;
- the complete curriculum coverage for that phase;
- concise mental models;
- important PySpark APIs and syntax;
- practical examples where useful;
- data-engineering implications;
- common mistakes and edge cases;
- important distinctions;
- mastery expectations.

### Table of contents requirement

**Every substantive README document in this repository must contain a table of contents.**

This includes:

- the root `README.md`;
- phase-level `README.md` files;
- capstone README files;
- any other README that contains meaningful project documentation.

Trivial placeholder READMEs should generally be avoided rather than created.

---

## Repository Structure

The repository should grow incrementally.

Do **not** create empty directories or placeholder files simply because they may be useful in a later phase.

A likely mature structure is:

```text
PySpark-101/
│
├── README.md
├── CURRICULUM.md
├── ROADMAP.md
├── .gitignore
├── requirements.txt
│
├── phases/
│   ├── phase_01_dataframe_fundamentals/
│   │   ├── README.md
│   │   └── ...
│   ├── phase_02_joins_aggregations_windows/
│   │   ├── README.md
│   │   └── ...
│   └── ...
│
├── docs/
│   └── ...
│
├── src/
│   └── pyspark_learning/
│
├── tests/
│
├── sql/
│
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

This is a **possible mature structure, not a scaffold to create in advance**.

Create files and directories only when they serve actual work.

---

## Repository Conventions

### Preserve worthwhile work

Anything that materially demonstrates PySpark or data-engineering capability should eventually live in the repository rather than only in chat history.

Examples:

- reusable code;
- meaningful exercises;
- schemas;
- tests;
- SQL;
- documentation;
- performance experiments;
- troubleshooting evidence;
- applied pipeline work.

### Avoid unnecessary structure

Do not require every phase to contain the same folders.

For example, create `exercises/`, `solutions/`, `tests/`, or `src/` only when the phase actually benefits from them.

### Keep notebooks secondary

Notebooks may be used for exploration and experimentation.

Anything worth preserving as part of the project should generally be refactored into structured repository code or documentation.

### Start testing when useful

Automated testing does not need to wait until the dedicated testing phase.

Once meaningful transformation logic exists, add tests where they provide real value.

### Prefer professional data-engineering practice

As relevant to the current phase, emphasize:

- schemas;
- grain;
- correctness;
- transformations;
- distributed execution;
- performance;
- data quality;
- testing;
- idempotency;
- reliability;
- maintainability;
- production design.

---

## Data Strategy

Where practical, phases should reuse an evolving data domain rather than unrelated toy datasets.

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
- storage experiments;
- performance engineering;
- data-quality validation;
- testing;
- incremental processing;
- cloud execution;
- warehouse integration;
- the final capstone.

Large generated datasets and generated analytical outputs should generally **not** be committed to Git.

Examples that normally stay out of Git:

```text
data/raw/
data/curated/
data/rejected/
*.parquet
```

Small deterministic fixtures may be committed when they are intentionally part of tests or exercises.

---

## Git and Progress Tracking

Git history should reflect meaningful learning and implementation milestones.

Prefer coherent commits such as:

```text
phase 01: add explicit retail schemas and I/O examples
phase 01: implement typed retail cleaning workflow
phase 06: demonstrate repartition and coalesce behavior
phase 11: add PySpark transformation tests
```

Avoid committing:

- virtual environments;
- large generated datasets;
- Parquet outputs;
- local runtime artifacts;
- credentials;
- secrets.

At phase completion:

1. Confirm the relevant `CURRICULUM.md` mastery requirements are satisfied.
2. Preserve important implementation and documentation.
3. Record concise completion notes in `ROADMAP.md`.
4. Mark the phase complete in `ROADMAP.md`.
5. Commit the finalized phase work.
6. Generate the starter prompt for the next dedicated phase chat.

---

## Current Status

**Phase 1 — DataFrame Fundamentals, Schemas & I/O**

No curriculum phase has been completed yet.

Progress is tracked in [`ROADMAP.md`](ROADMAP.md).

---

## Final Mastery Target

By the end of the project, I should be able to take a requirement such as:

> Process hundreds of gigabytes of transactional data per day.

and reason confidently about:

- ingestion;
- schemas;
- malformed and invalid data;
- data grain;
- facts and dimensions;
- PySpark transformations;
- joins and shuffles;
- jobs, stages, and tasks;
- execution partitions;
- storage partitioning;
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
- observability;
- retries;
- backfills;
- production operation.

The goal is not merely to **know PySpark syntax**.

It is to **use Spark independently and professionally as a data engineer**.
