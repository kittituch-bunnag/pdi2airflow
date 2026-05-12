# pdi2airflow

A local desktop tool for migrating Pentaho Data Integration (PDI / Kettle)
workflows into Apache Airflow DAGs using standard Python, SQLAlchemy, and pandas.
No proprietary plugins required.

- Upload `.kjb` / `.ktr` / `.kdb` (or a `.zip` containing them)
- Optionally upload `kettle.properties` and `repositories.xml` for richer
  variable resolution and repository-aware DAG tagging
- See the workflow as an editable React Flow diagram
- Add, remove, or rewire tasks in the browser
- Generate an Airflow DAG file plus a markdown migration report

The generated DAGs are self-contained: they include small helper functions
(`_pg_engine`, `_mssql_engine`, `_df_insert`, `_upsert_pg`, `_exec_sql`) that
build SQLAlchemy engines from standard Airflow connections — no external plugins
or custom operators needed.

## Architecture

```
PDI files (.kjb, .ktr, .kdb)
        │
        ▼
   [parsers]            backend/parsers/{kjb,ktr,kdb}_parser.py
        │
        ▼
   IR WorkflowGraph     backend/models/ir.py
        │
        ▼
   React Flow UI        frontend/src/App.jsx (edit, rewire)
        │
        ▼
   IR (edited)
        │
        ▼
   [generators]         backend/generators/dag_generator.py
        │
        ▼
   Airflow DAG (.py) + migration_report.md
```

The Intermediate Representation (IR) is the single source of truth.
PDI specifics never leak into the generators; they only consume IR.

## Optional config files

In addition to `.kjb` / `.ktr` / `.kdb`, you can upload:

**`kettle.properties`** (typically at `~/.kettle/kettle.properties`) provides
user-defined variables that PDI files reference as `${VAR}`. Built-in
`KETTLE_*` engine settings are recognised and ignored; only user variables
become defaults on the IR's `VariableRef.default_value`, removing them from
the "must be defined as Airflow Variable" list.

**`repositories.xml`** (typically at `~/.kettle/repositories.xml`) defines
the file-based and database-based PDI repositories. When the source `.kjb`
path falls under one of the configured `base_directory` values (most easily
arranged by uploading a `.zip` of the repository folder so paths are
preserved), the migration tool:
- prefixes the generated `dag_id` with the repository name
- adds the repository name to the DAG's `tags` list for grouping in the
  Airflow UI
- prefixes the DAG description with `[RepoName]`

Neither file is required; both are auto-detected by filename when uploaded.

## Mapping

| PDI                         | Airflow                                              |
|-----------------------------|------------------------------------------------------|
| `.kjb` job                  | One DAG (file)                                       |
| `.kjb` `<entry>` SPECIAL/start | `DummyOperator` (start anchor)                    |
| `.kjb` `<entry>` SPECIAL/end | `DummyOperator` with `trigger_rule="none_failed"`  |
| `.kjb` `<entry>` TRANS      | `PythonOperator`; callable inlines `.ktr` steps      |
| `.kjb` `<entry>` SQL        | `PythonOperator` using `_exec_sql` + engine helper   |
| `.kjb` `<entry>` SHELL      | `BashOperator`                                       |
| `.kjb` `<entry>` EVAL       | `BranchPythonOperator` (with TODO body)              |
| `.kjb` `<entry>` JOB (subjob) | `DummyOperator` placeholder + TODO                |
| `.ktr` TableInput           | `_pg_engine` / `_mssql_engine` + `pd.read_sql`       |
| `.ktr` TableOutput          | `_df_insert` (insert) or `_upsert_pg` (upsert, PG)  |
| `.ktr` ExecSQL              | `_exec_sql` + `_pg_engine` / `_mssql_engine`         |
| `.ktr` SelectValues         | column filter / rename — feeds `_des_col` in direct path, or `df.rename` / `df[cols]` in df path |
| `.ktr` FilterRows           | TODO comment + `df.query(...)` skeleton (forces df path) |
| `.kdb` connection           | Suggested Airflow Conn ID via host/db keyword match  |

Step types not in `KNOWN_STEP_TYPES` (`backend/parsers/ktr_parser.py`)
are parsed but generation is **blocked** until you either resolve them
in the UI or extend the registry — this is the **fail loudly** behavior.

## How DAGs are generated

### Overall structure

Every generated `.py` file has four sections:

```
# 1. Imports          — airflow, sqlalchemy, pandas, pendulum
# 2. DB helpers       — _pg_engine, _mssql_engine, _df_insert, _upsert_pg, _exec_sql
# 3. CONN_* vars      — one per PDI DB connection, mapped to an Airflow conn_id
# 4. Callables        — one Python function per TRANSFORMATION / SQL / EVAL node
# 5. DAG block        — with DAG(...) as dag: operators + wiring
```

### How TRANSFORMATION callables are chosen

#### Direct cross-db path (preferred)

Triggered when the `.ktr` contains exactly **one `TableInput`, one
`TableOutput`, and only `SelectValues` / `WriteToLog` in between**.

The generator reads from the source via `pd.read_sql` and writes to the
destination via `_df_insert` (append/truncate) or `_upsert_pg` (upsert, PG only).

```python
def run_my_transform(**context):
    _sql = """SELECT id, name FROM source_table"""
    _des_col = ['id', 'name']
    _src = _mssql_engine("mssql_prod")
    _df = pd.read_sql(_sql, _src)
    _df = _df[_des_col]
    # To upsert: _upsert_pg(_df, _pg_engine("pg_dw"), "public.target", pkey=["TODO_pk"])
    _df_insert(_df, _pg_engine("pg_dw"), "public.target", truncate=False)
```

#### Step-by-step df path (fallback)

Used when the `.ktr` contains complex intermediate steps such as
`FilterRows`, `ScriptValueMod`, `GetVariable`, `Constant`, etc. The
generator emits one code block per step, accumulating transformations
into a `df` variable, then writes via `_upsert_pg` (PG) or `_df_insert` (MSSQL).

### TODO markers in generated code

| Marker | What to do |
|--------|-----------|
| `pkey=["TODO_primary_key"]` | Replace with the real PK column(s) of the destination table |
| `pkey=["TODO_pk"]` (upsert comment) | Replace when switching from insert to upsert |
| `_des_col = []  # TODO` | Only for unknown DB types — manually list the target columns |
| `schedule_interval=None  # TODO` | Set the cron schedule after migrating |
| `# TODO: implement branching logic` | EVAL/branch nodes need manual Python logic |
| `# TODO: replace with TriggerDagRunOperator` | SUBJOB nodes — wire to the migrated sub-DAG |

## Airflow connection setup

Generated DAGs read database credentials from standard Airflow connections
(Admin → Connections). Each `CONN_*` variable in the generated file holds
the `conn_id` that the helper functions pass to `BaseHook.get_connection()`.

The connection type determines which engine helper is used:
- `POSTGRESQL` → `_pg_engine` (uses `psycopg2`)
- `MSSQL` → `_mssql_engine` (uses `pymssql`)

Add your connection mappings in `backend/parsers/connection_map.json`.

## ⚠ Action Required After Every Generation

Every generated DAG contains a `CONN_*` variable for each PDI connection.
These are **auto-mapped** — the tool does its best to suggest the right Airflow
`conn_id`, but you **must** verify each one before enabling the DAG.

| Badge in report | Confidence | What to do |
|---|---|---|
| ✅ exact | PDI name matched an Airflow conn\_id directly | Confirm it exists in Airflow |
| ⚠️ host\_match | Matched by hostname / DB-type keyword | Verify host, database, and credentials |
| 🔴 type\_match | Matched by DB type only | **Manually identify the correct conn\_id** |

## Install and Run

### Docker (recommended)

```bash
docker compose up -d
```

Open **http://localhost:8765**. No Python or Node.js required on the host.

To apply project-wide variable defaults, place `kettle.properties` or
`repositories.xml` in `./config/` before starting:

```bash
mkdir -p config
cp /path/to/kettle.properties config/
docker compose up -d
```

### Local (without Docker)

```bash
# 1. Backend
python -m venv .venv
source .venv/bin/activate           # or: .venv\Scripts\activate on Windows
pip install -r requirements.txt

# 2. Frontend (only needed once, or after frontend code changes)
cd frontend
npm install
npm run build                        # produces frontend/dist/
cd ..

# 3. Run
python run.py                        # opens http://localhost:8765 in your browser
```

### Development mode (auto-reload + hot-reload UI)

```bash
# Terminal 1
python run.py --reload --no-browser  # backend on :8765

# Terminal 2
cd frontend
npm run dev                          # Vite dev server on :5173, proxies /api -> :8765
```

Open http://localhost:5173.

## Extending the tool

### Add an Airflow connection mapping

Edit `backend/parsers/connection_map.json`. Each entry supports:
- `pdi_names` — exact PDI connection names (highest confidence)
- `host_keywords` — substrings matched against the PDI connection host
- `database_keywords` — substrings matched against the database name
- `name_keywords` — substrings matched against the PDI connection name

More-specific entries should appear first. See the example entries already
in the file for the expected structure.

### Add a translatable PDI step type

1. Add the type to `KNOWN_STEP_TYPES` in `backend/parsers/ktr_parser.py`
   and extract its config in `_parse_step`.
2. Add a code-emission branch in `_step_code` inside
   `backend/generators/transformation_codegen.py`.

Unknown step types emit a TODO stub — they never produce silently-wrong code.

### Change generated DAG conventions

Edit the `DAG_TEMPLATE` string and helper builders in
`backend/generators/dag_generator.py`. The timezone (`Asia/Bangkok`),
`run_date` Jinja formula, and `default_args` can all be adjusted there.

## What is intentionally not done

- **No schedule auto-detection.** Generated DAGs set `schedule_interval=None`
  with a TODO comment.
- **No live Airflow validation.** The generator emits Python that passes
  `ast.parse` but does not import `airflow` itself; validate in your environment.
- **JavaScript / Formula steps.** These become TODO stubs with the original
  PDI script preserved in a comment.
- **PDI clustering / partitioning.** Not supported; manual review required.

## Project layout

```
pdi2airflow-generic/
├── README.md
├── requirements.txt
├── run.py
├── Dockerfile
├── docker-compose.yml
├── backend/
│   ├── app.py                (FastAPI app)
│   ├── models/ir.py          (IR data classes)
│   ├── parsers/
│   │   ├── connection_map.json   (PDI -> Airflow conn_id mappings — edit this)
│   │   ├── connection_mapper.py
│   │   ├── kdb_parser.py
│   │   ├── ktr_parser.py
│   │   ├── kjb_parser.py
│   │   └── orchestrator.py
│   └── generators/
│       ├── transformation_codegen.py
│       └── dag_generator.py
├── frontend/
│   └── src/
│       ├── App.jsx
│       ├── styles.css
│       └── components/
│           ├── PdiNode.jsx
│           └── TechIcon.jsx
└── samples/
    ├── load_customer_daily.kjb
    └── load_customer.ktr
```
