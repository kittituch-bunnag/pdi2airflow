# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

**pdi2airflow-generic** converts Pentaho Data Integration (PDI/Kettle) workflows (`.kjb`, `.ktr`, `.kdb`) into Apache Airflow DAGs using only standard Python libraries (SQLAlchemy, pandas, psycopg2, pymssql). No proprietary plugins or custom operators are required. It fails loudly on any unresolvable PDI construct rather than silently producing wrong output.

## Commands

### Backend

```bash
# Install dependencies
pip install -r requirements.txt

# Run production server (port 8765, opens browser)
python run.py

# Run with hot-reload, no browser (dev backend)
python run.py --reload --no-browser
```

### Frontend

```bash
cd frontend
npm install
npm run build   # one-time build for production
npm run dev     # dev server at localhost:5173 (proxies /api → localhost:8765)
```

### Docker

```bash
# Build and run (production, port 8765)
docker compose up -d

# Rebuild after code changes
docker compose up -d --build

# Mount project-wide kettle.properties / repositories.xml:
# place the file(s) in ./config/ — the container picks them up automatically
```

### Development (two terminals)

Terminal 1: `python run.py --reload --no-browser`
Terminal 2: `cd frontend && npm run dev` → open http://localhost:5173

## Architecture

The tool is a strict three-stage pipeline:

```
PDI files (.kjb/.ktr/.kdb)
        ↓ parsers/
Intermediate Representation (IR)
        ↓ ReactFlow UI (edit nodes/edges in browser)
Edited IR
        ↓ generators/
Airflow DAG (.py) + migration_report.md
```

**Key invariant:** PDI specifics never leak into generators. Parsers produce IR; generators consume only IR.

### Backend (`backend/`)

- **`app.py`** — FastAPI entry point. Two endpoints:
  - `POST /api/upload` — ingests PDI files, returns IR as JSON
  - `POST /api/generate` — takes edited IR, returns DAG source + report (blocks with 422 if issues exist)

- **`models/ir.py`** — All IR data classes: `WorkflowGraph`, `Node`, `Edge`, `TransformationStep`, `ConnectionRef`, `VariableRef`, `NodeKind`. `WorkflowGraph.is_resolvable()` enforces the fail-loudly contract.

- **`parsers/orchestrator.py`** — `ingest_files(files)` is the single entry point. Runs four passes: project config, `.kjb`, `.ktr`, `.kdb`. Returns `IngestResult`.

- **`parsers/connection_mapper.py`** — Maps PDI DB connections to Airflow `conn_id`s via confidence tiers (exact → host_match → type_match → none). Only "none" blocks generation.

- **`generators/dag_generator.py`** — Emits a complete `.py` DAG file from IR. The generated file is self-contained: it includes helper functions (`_pg_engine`, `_mssql_engine`, `_df_insert`, `_upsert_pg`, `_exec_sql`) that build SQLAlchemy engines from standard Airflow connections. Conventions: `schedule_interval=None`, timezone `Asia/Bangkok`, `run_date` Jinja formula converts UTC to local date.

- **`generators/transformation_codegen.py`** — Emits the Python callable for `TRANSFORMATION` nodes. Two code paths (chosen automatically):
  1. **Direct cross-db path** — when the `.ktr` is a simple `TableInput → [SelectValues/WriteToLog] → TableOutput` pipeline, reads with `pd.read_sql` and writes with `_df_insert` or `_upsert_pg`.
  2. **Step-by-step df path** — when complex steps like `FilterRows` or `ScriptValueMod` exist, emits per-step code that accumulates a pandas `df`, then writes via `_upsert_pg` (PG) or `_df_insert` (MSSQL). Unknown step types produce `TODO` stubs and block generation.

### Frontend (`frontend/src/`)

- **`App.jsx`** — ReactFlow canvas. Converts between IR and ReactFlow state via `irToFlow()` / `flowToIr()`. Handles file upload and generation triggers.
- **`components/PdiNode.jsx`** — Custom ReactFlow node renderer for PDI node kinds.

## Extension Points

**Add an Airflow connection mapping** — edit `backend/parsers/connection_map.json` (checked-in defaults) or create `config/connection_map.json` (local override, git-ignored). Each entry has `airflow_id`, `db_type`, `pdi_names` (exact match list), `host_keywords`, `database_keywords`, `name_keywords`, and optional `port`. Entries are tried top-to-bottom within each confidence tier, so put more-specific entries first.

**Add a translatable PDI step type** — two steps:
1. Add to `KNOWN_STEP_TYPES` in `backend/parsers/ktr_parser.py`
2. Add code-emission branch in `_step_code()` in `backend/generators/transformation_codegen.py`

**Change DAG conventions** — edit `DAG_TEMPLATE` and helpers in `backend/generators/dag_generator.py`. The DB helper functions (`_pg_engine`, etc.) are emitted at the top of every generated DAG — adjust them there if you need different connection patterns (e.g., SSL, Kerberos).

**Add a DB type icon** — edit `SI_MAP` or add a custom SVG component in `frontend/src/components/TechIcon.jsx`. Keys are the lowercased `db_type` string from the connection mapper (e.g. `postgresql`, `mssql`).

## Optional Upload Files

Users may upload alongside PDI files:
- `kettle.properties` — populates `${VAR}` defaults in IR
- `repositories.xml` — auto-prefixes `dag_id` with repository name and adds tags (requires files uploaded as `.zip` to preserve paths)

Both files can also be placed in `config/` at the project root so they apply to every upload without re-uploading. `config/` is excluded from git (`.gitignore`) because it may contain internal hostnames or variable values.

## Generated DAG Structure

Every generated file has these sections in order:

```
# -*- coding: utf-8 -*-
# Imports (airflow, sqlalchemy, pandas, pendulum)
# DB helpers (_pg_engine, _mssql_engine, _df_insert, _upsert_pg, _exec_sql, _table_cols)
# Configuration (local_tz, run_date)
# CONN_* variables (one per PDI connection, ACTION REQUIRED block)
# Task callables (one function per TRANSFORMATION / SQL / EVAL node)
# DAG definition block (default_args, with DAG(...) as dag:)
```

## Known Intentional Limitations

- `schedule_interval=None` always — PDI scheduler config is unreliable; generated DAG has a TODO comment
- No live Airflow validation — generated code passes `ast.parse` but must be validated in a real Airflow environment
- JavaScript/Formula steps become `TODO` stubs with the original PDI script in a comment
- No PDI clustering/partitioning support
- **Connection mapping is heuristic** — every generated DAG includes an `# ACTION REQUIRED` block and the migration report includes a `[!WARNING]` callout. Users must verify every `CONN_*` conn\_id in Airflow (Admin → Connections) before enabling the DAG.
