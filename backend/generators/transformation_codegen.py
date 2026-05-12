"""
Generates Python code for the body of a TRANSFORMATION task's callable.

A .ktr typically follows the pattern:
    TableInput  -->  [SelectValues / FilterRows / ...]  -->  TableOutput

Two code paths are used:

1. **Direct cross-db path** (preferred): when the transformation is a simple
   TableInput -> [SelectValues/WriteToLog]* -> TableOutput pipeline, emit a
   single read-from-source / write-to-dest block using the _pg_engine /
   _mssql_engine / _df_insert / _upsert_pg helpers defined at the top of the
   generated DAG file.

2. **Step-by-step df path** (fallback): when there are complex intermediate
   steps (FilterRows, ScriptValueMod, etc.) that require a pandas DataFrame,
   emit individual code blocks per step, accumulating transformations into a
   df, then writing via _upsert_pg (PG) or _df_insert (MSSQL).

When a step's pdi_type is not recognised, a clearly-marked TODO stub is
emitted referencing the original PDI step name and type.
"""

from __future__ import annotations

import textwrap

from ..models.ir import ConnectionRef, Node, TransformationStep


def _topo_sort_steps(node: Node) -> list[TransformationStep]:
    """Topological sort of the steps using internal step_edges."""
    by_id = {s.step_id: s for s in node.steps}
    indeg = {s.step_id: 0 for s in node.steps}
    children: dict[str, list[str]] = {s.step_id: [] for s in node.steps}
    for e in node.step_edges:
        if e.source in by_id and e.target in by_id:
            children[e.source].append(e.target)
            indeg[e.target] += 1
    queue = [sid for sid, d in indeg.items() if d == 0]
    out: list[TransformationStep] = []
    while queue:
        sid = queue.pop(0)
        out.append(by_id[sid])
        for c in children[sid]:
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)
    seen = {s.step_id for s in out}
    for s in node.steps:
        if s.step_id not in seen:
            out.append(s)
    return out


def _airflow_conn_for(pdi_conn_name: str, conns: list[ConnectionRef]) -> str:
    for c in conns:
        if c.pdi_name == pdi_conn_name and c.suggested_airflow_conn:
            return c.suggested_airflow_conn
    return f"TODO_{pdi_conn_name}"


def _conn_db_type(pdi_conn_name: str, conns: list[ConnectionRef]) -> str:
    for c in conns:
        if c.pdi_name == pdi_conn_name:
            return (c.db_type or "").upper()
    return "UNKNOWN"


def _safe_sql_literal(sql: str) -> str:
    """
    Render a SQL string as a safe Python literal.
    Uses triple-double-quote and escapes any embedded triple-double-quotes.
    """
    if sql is None:
        return '""'
    cleaned = sql.replace('"""', '\\"\\"\\"')
    return f'"""{cleaned}"""'


def _step_code(step: TransformationStep, conns: list[ConnectionRef], df_var: str) -> str:
    """
    Emit the code lines for a single step (step-by-step df path).
    The helpers _pg_engine, _mssql_engine, _df_insert, _upsert_pg, _exec_sql
    are assumed to be defined at the top of the generated DAG file.
    """
    pdi_type = step.pdi_type
    name = step.name

    if pdi_type == "TableInput":
        conn = step.config.get("connection", "")
        airflow_conn = _airflow_conn_for(conn, conns)
        db_type = _conn_db_type(conn, conns)
        sql_lit = _safe_sql_literal(step.config.get("sql") or "")
        if db_type == "MSSQL":
            return textwrap.dedent(f"""
                # Step: {name} (PDI TableInput -> MSSQL)
                _src = _mssql_engine("{airflow_conn}")
                _sql = {sql_lit}
                {df_var} = pd.read_sql(_sql, _src)
            """).strip()
        elif db_type == "POSTGRESQL":
            return textwrap.dedent(f"""
                # Step: {name} (PDI TableInput -> PostgreSQL)
                _src = _pg_engine("{airflow_conn}")
                _sql = {sql_lit}
                {df_var} = pd.read_sql(_sql, _src)
            """).strip()
        else:
            return textwrap.dedent(f"""
                # Step: {name} (PDI TableInput -> {db_type})
                # TODO: db_type={db_type} not recognised — adjust engine helper if needed
                _src = _mssql_engine("{airflow_conn}")
                _sql = {sql_lit}
                {df_var} = pd.read_sql(_sql, _src)
            """).strip()

    if pdi_type == "TableOutput":
        conn = step.config.get("connection", "")
        airflow_conn = _airflow_conn_for(conn, conns)
        db_type = _conn_db_type(conn, conns)
        schema = step.config.get("schema", "") or ""
        table = step.config.get("table", "") or ""
        qualified_table = f"{schema}.{table}" if schema else table
        truncate = bool(step.config.get("truncate", False))
        if db_type == "POSTGRESQL":
            return textwrap.dedent(f"""
                # Step: {name} (PDI TableOutput -> PostgreSQL)
                _upsert_pg(
                    {df_var},
                    _pg_engine("{airflow_conn}"),
                    "{qualified_table}",
                    pkey=["TODO_primary_key"],  # TODO: specify primary key column(s)
                )
            """).strip()
        else:
            return textwrap.dedent(f"""
                # Step: {name} (PDI TableOutput -> {db_type})
                _df_insert({df_var}, _mssql_engine("{airflow_conn}"), "{qualified_table}", truncate={truncate})
            """).strip()

    if pdi_type == "ExecSQL":
        conn = step.config.get("connection", "")
        airflow_conn = _airflow_conn_for(conn, conns)
        db_type = _conn_db_type(conn, conns)
        sql_lit = _safe_sql_literal(step.config.get("sql") or "")
        engine_fn = "_mssql_engine" if db_type == "MSSQL" else "_pg_engine"
        return textwrap.dedent(f"""
            # Step: {name} (PDI ExecSQL)
            _exec_sql({engine_fn}("{airflow_conn}"), {sql_lit})
        """).strip()

    if pdi_type == "SelectValues":
        fields = step.config.get("fields") or []
        renames = {f["name"]: f["rename"] for f in fields if f.get("rename") and f.get("name")}
        cols = [f["rename"] or f["name"] for f in fields if f.get("name")]
        lines = [f"# Step: {name} (PDI SelectValues)"]
        if renames:
            lines.append(f"{df_var} = {df_var}.rename(columns={renames!r})")
        if cols:
            lines.append(f"{df_var} = {df_var}[{cols!r}]")
        return "\n".join(lines)

    if pdi_type == "FilterRows":
        cond = step.config.get("condition", "") or ""
        return textwrap.dedent(f"""
            # Step: {name} (PDI FilterRows)
            # Original PDI condition: {cond!r}
            # TODO: translate the PDI condition to a pandas .query() expression
            # {df_var} = {df_var}.query("...")
        """).strip()

    if pdi_type == "Constant":
        return f"# Step: {name} (PDI Constant) — TODO: add constant columns to {df_var}"

    if pdi_type == "GetVariable":
        fields = step.config.get("fields") or []
        lines = [f"# Step: {name} (PDI GetVariable)"]
        for f in fields:
            col = f.get("name", "var")
            var = f.get("variable", "")
            var_name = var.strip("${}")
            lines.append(f'{df_var}["{col}"] = Variable.get("{var_name}", default_var=None)')
        return "\n".join(lines)

    if pdi_type == "SetVariable":
        _XCOM_SCOPES = {"ROOT_JOB", "PARENT_JOB", "GRANDPARENT_JOB"}
        fields = step.config.get("fields") or []
        lines = [f"# Step: {name} (PDI SetVariable — publishes runtime variables via XCom)"]
        for f in fields:
            field_name = f.get("field_name", "")
            var_name = f.get("variable_name", "")
            var_type = f.get("variable_type", "PARENT_JOB")
            if var_type in _XCOM_SCOPES:
                lines.append(
                    f'context["ti"].xcom_push(key="{var_name}", value='
                    f'{df_var}["{field_name}"].iloc[0] if {df_var} is not None and len({df_var}) > 0 else None)'
                )
            else:
                lines.append(f"# {var_name}: scope={var_type} — transformation-local, no XCom push needed")
        return "\n".join(lines)

    if pdi_type == "SystemInfo":
        _SYSINFO_MAP = {
            "system date (variable)": "datetime.now()",
            "system date": "datetime.now()",
            "hostname": "__import__('socket').gethostname()",
            "current pid": "__import__('os').getpid()",
            "current user": "__import__('os').getenv('USER', __import__('os').getenv('USERNAME', 'unknown'))",
            "jvm memory total": "0",
            "jvm memory free": "0",
        }
        fields = step.config.get("fields") or []
        lines = [f"# Step: {name} (PDI SystemInfo)"]
        for f in fields:
            col = f.get("name", "info")
            stype = (f.get("type") or "").lower()
            expr = _SYSINFO_MAP.get(stype, f"None  # TODO: unsupported SystemInfo type {stype!r}")
            lines.append(f'{df_var}["{col}"] = {expr}')
        return "\n".join(lines)

    if pdi_type == "SetValueConstant":
        fields = step.config.get("fields") or []
        lines = [f"# Step: {name} (PDI SetValueConstant)"]
        for f in fields:
            col = f.get("name", "col")
            val = f.get("value", "")
            lines.append(f'{df_var}["{col}"] = {val!r}')
        return "\n".join(lines)

    if pdi_type == "JsonOutput":
        filename = step.config.get("filename") or ""
        json_bloc = step.config.get("json_bloc") or "data"
        fields = step.config.get("fields") or []
        cols = [f["name"] for f in fields if f.get("name")]
        col_expr = f"[{cols!r}]" if cols else ""
        return textwrap.dedent(f"""
            # Step: {name} (PDI JsonOutput)
            import json as _json
            _json_payload = {{"{json_bloc}": {df_var}{col_expr}.to_dict(orient="records")}}
            _json_str = _json.dumps(_json_payload, ensure_ascii=False, default=str)
            {df_var}["_json_output"] = _json_str
            # TODO: verify output destination (PDI filename: {filename!r})
        """).strip()

    if pdi_type == "Rest":
        url = step.config.get("url") or ""
        method = (step.config.get("method") or "GET").upper()
        body_field = step.config.get("body_field") or ""
        app_type = step.config.get("application_type") or "application/json"
        result_field = step.config.get("result_fieldname") or "_rest_response"
        result_code_field = step.config.get("result_code_fieldname") or "_rest_status_code"
        headers = step.config.get("headers") or []
        headers_dict = {h["name"]: h["value"] for h in headers if h.get("name")}
        body_expr = f'row["{body_field}"]' if body_field else "None"
        return textwrap.dedent(f"""
            # Step: {name} (PDI Rest — {method} {url!r})
            import requests as _requests
            _rest_headers = {{"Content-Type": "{app_type}", **{headers_dict!r}}}
            _rest_results = []
            _rest_codes = []
            for _idx, _row in {df_var}.iterrows():
                _url = "{url}" if "{url}" else _row.get("url", "")
                _body = {body_expr} if {bool(body_field)} else None
                _resp = _requests.request("{method}", _url, headers=_rest_headers, data=_body, timeout=30)
                _rest_results.append(_resp.text)
                _rest_codes.append(_resp.status_code)
            {df_var}["{result_field}"] = _rest_results
            {df_var}["{result_code_field}"] = _rest_codes
        """).strip()

    if pdi_type == "ScriptValueMod":
        scripts = step.config.get("scripts") or []
        fields = step.config.get("fields") or []
        field_names = [f["name"] for f in fields if f.get("name")]
        script_text = "\n".join(
            f"# --- {s.get('name', 'script')} ---\n# {s.get('script', '')}"
            for s in scripts
        )
        return textwrap.dedent(f"""
            # Step: {name} (PDI ScriptValueMod — JavaScript cannot be auto-translated)
            # Output fields expected: {field_names!r}
            # TODO: re-implement the following JavaScript logic in Python and populate {df_var}:
            {script_text}
        """).strip()

    if pdi_type == "WriteToLog":
        msg = step.config.get("log_message") or ""
        level = (step.config.get("log_level") or "Basic").upper()
        display_header = step.config.get("display_header", True)
        fields = step.config.get("fields") or []
        cols = [f["name"] for f in fields if f.get("name")]
        _LOG_LEVELS = {"BASIC": "info", "DETAILED": "info", "DEBUG": "debug",
                       "ROWLEVEL": "debug", "ERROR": "error", "MINIMAL": "warning"}
        log_fn = _LOG_LEVELS.get(level, "info")
        col_expr = f"[{cols!r}]" if cols else ""
        header_line = f'_log.info("-- {msg} --")' if display_header and msg else ""
        return textwrap.dedent(f"""
            # Step: {name} (PDI WriteToLog)
            import logging as _logging
            _log = _logging.getLogger(__name__)
            {header_line}
            _log.{log_fn}({df_var}{col_expr}.to_string(index=False))
        """).strip()

    # Unknown — emit a clearly-marked stub
    return textwrap.dedent(f"""
        # Step: {name} (PDI type: {pdi_type}) — UNMAPPED
        # TODO: implement equivalent logic. Original PDI config:
        # {step.config!r}
    """).strip()


# Steps that do not force the df path (transparent to the direct cross-db call).
_TRANSPARENT_STEP_TYPES = frozenset({"WriteToLog", "SelectValues"})


def _try_direct_cross_db(
    node: Node,
    sorted_steps: list,
    conns: list[ConnectionRef],
) -> str | None:
    """
    Detect a simple TableInput -> [SelectValues/WriteToLog]* -> TableOutput pipeline
    and emit a single read-then-write block using the DAG-level helpers.
    Returns function source code, or None to fall back to the df path.
    """
    input_steps  = [s for s in sorted_steps if s.pdi_type == "TableInput"]
    output_steps = [s for s in sorted_steps if s.pdi_type == "TableOutput"]
    other_steps  = [s for s in sorted_steps if s.pdi_type not in ("TableInput", "TableOutput")]

    if len(input_steps) != 1 or len(output_steps) != 1:
        return None
    if any(s.pdi_type not in _TRANSPARENT_STEP_TYPES for s in other_steps):
        return None

    inp = input_steps[0]
    out = output_steps[0]

    src_conn    = inp.config.get("connection", "")
    dst_conn    = out.config.get("connection", "")
    src_airflow = _airflow_conn_for(src_conn, conns)
    dst_airflow = _airflow_conn_for(dst_conn, conns)
    src_type    = _conn_db_type(src_conn, conns)
    dst_type    = _conn_db_type(dst_conn, conns)

    schema     = out.config.get("schema", "") or ""
    tbl        = out.config.get("table", "")  or ""
    qualified  = f"{schema}.{tbl}" if schema else tbl
    truncate   = bool(out.config.get("truncate", False))
    sql_lit    = _safe_sql_literal(inp.config.get("sql") or "")

    # Collect destination columns and renames from any SelectValues step
    des_col: list[str] = []
    col_mapping: dict[str, str] = {}
    for sv in (s for s in other_steps if s.pdi_type == "SelectValues"):
        fields = sv.config.get("fields") or []
        des_col = [f.get("rename") or f["name"] for f in fields if f.get("name")]
        col_mapping = {
            f["name"]: f["rename"]
            for f in fields
            if f.get("rename") and f.get("name") and f["name"] != f.get("rename")
        }

    # Right-hand side for _des_col: static list or runtime column discovery
    if des_col:
        des_col_rhs = repr(des_col)
    elif dst_type == "POSTGRESQL":
        des_col_rhs = f'_table_cols(_pg_engine("{dst_airflow}"), "{qualified}")'
    elif dst_type == "MSSQL":
        des_col_rhs = f'_table_cols(_mssql_engine("{dst_airflow}"), "{qualified}")'
    else:
        des_col_rhs = "[]  # TODO: list destination columns"

    fn  = f"run_{node.id}"
    lbl = node.label
    rename_line = f"    _df = _df.rename(columns={col_mapping!r})\n" if col_mapping else ""
    col_filter  = "    _df = _df[_des_col]\n" if des_col else "    if _des_col:\n        _df = _df[_des_col]\n"

    src_engine = "_mssql_engine" if src_type == "MSSQL" else "_pg_engine"

    if dst_type == "POSTGRESQL":
        write_comment = f'    # To upsert: _upsert_pg(_df, _pg_engine("{dst_airflow}"), "{qualified}", pkey=["TODO_pk"])\n'
        write_call    = f'    _df_insert(_df, _pg_engine("{dst_airflow}"), "{qualified}", truncate={truncate})\n'
    elif dst_type == "MSSQL":
        write_comment = "    # To upsert: implement manually for MSSQL (no native ON CONFLICT)\n"
        write_call    = f'    _df_insert(_df, _mssql_engine("{dst_airflow}"), "{qualified}", truncate={truncate})\n'
    else:
        write_comment = f"    # TODO: unknown destination db_type={dst_type!r} — choose appropriate write helper\n"
        write_call    = f'    _df_insert(_df, _mssql_engine("{dst_airflow}"), "{qualified}", truncate={truncate})  # TODO\n'

    return (
        f"def {fn}(**context):\n"
        f'    """Translated from PDI transformation: {lbl} ({src_type} -> {dst_type})"""\n'
        f"    _sql = {sql_lit}\n"
        f"    _des_col = {des_col_rhs}\n"
        f"    _src = {src_engine}(\"{src_airflow}\")\n"
        f"    _df = pd.read_sql(_sql, _src)\n"
        + rename_line
        + col_filter
        + write_comment
        + write_call
    )


def generate_transformation_callable(node: Node, conns: list[ConnectionRef]) -> tuple[str, str]:
    """
    Build the callable function for a TRANSFORMATION node.
    Returns (function_name, source_code).
    """
    func_name = f"run_{node.id}"
    sorted_steps = _topo_sort_steps(node)

    # Prefer the single cross-db call when the pipeline is simple enough
    direct_src = _try_direct_cross_db(node, sorted_steps, conns)
    if direct_src is not None:
        return func_name, direct_src

    # Fall back to step-by-step df approach for complex transformations
    df_var = "df"
    body_blocks: list[str] = []
    for s in sorted_steps:
        block = _step_code(s, conns, df_var)
        body_blocks.append(textwrap.indent(block, "    "))

    if not body_blocks:
        body_blocks.append("    pass  # transformation had no steps")

    src = (
        f"def {func_name}(**context):\n"
        f"    \"\"\"Translated from PDI transformation: {node.label}\"\"\"\n"
        f"    {df_var} = None\n"
        + "\n".join(body_blocks)
        + "\n"
    )
    return func_name, src
