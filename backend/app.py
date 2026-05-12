"""
FastAPI app for pdi2airflow.

Endpoints:
  POST /api/upload         — Accept .ktr/.kjb/.kdb/.zip files; return parsed IR JSON.
  POST /api/generate       — Accept (possibly edited) IR JSON; return DAG source + report.
  GET  /                   — Serve the React Flow UI (built static files in frontend/dist).

The server is intended to run locally:
    uvicorn backend.app:app --port 8765
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .generators.dag_generator import generate_dag_source, generate_migration_report
from .models.ir import WorkflowGraph
from .parsers.orchestrator import ingest_files


app = FastAPI(title="pdi2airflow", version="0.1.0")

# Project-level config directory — kettle.properties placed here applies to every upload.
_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

# CORS for local dev (Vite on :5173 talking to FastAPI on :8765)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _prepend_config_files(file_bytes: list[tuple[str, bytes]]) -> list[str]:
    """
    Prepend config/kettle.properties (and config/repositories.xml if present) to file_bytes
    so they act as a project-wide base. User-uploaded files of the same name are appended
    after, so they override the disk values via the normal merge in the orchestrator.
    Returns the list of auto-loaded filenames for the API response.
    """
    auto_loaded: list[str] = []
    for config_name in ("kettle.properties", "repositories.xml"):
        disk_path = _CONFIG_DIR / config_name
        if disk_path.exists():
            file_bytes.insert(0, (config_name, disk_path.read_bytes()))
            auto_loaded.append(f"config/{config_name}")
    return auto_loaded


@app.post("/api/upload")
async def upload(files: list[UploadFile]) -> JSONResponse:
    """Parse uploaded PDI files and return the IR graph as JSON."""
    if not files:
        raise HTTPException(400, "no files uploaded")

    file_bytes: list[tuple[str, bytes]] = []
    for f in files:
        content = await f.read()
        file_bytes.append((f.filename or "unnamed", content))

    # Auto-load project-wide config from disk (base); user uploads override.
    auto_loaded = _prepend_config_files(file_bytes)

    result = ingest_files(file_bytes)
    graph = result.primary_graph()

    if graph is None:
        return JSONResponse(
            {
                "error": "no parseable .kjb or .ktr found",
                "errors": result.errors,
                "received": [name for name, _ in file_bytes],
            },
            status_code=400,
        )

    response: dict = {
        "graph": graph.to_dict(),
        "parser_errors": result.errors,
        "standalone_ktr_count": len(result.standalone_ktrs),
        "auto_loaded_config": auto_loaded,
    }

    # Surface project-level config so the UI can show what was loaded
    if result.config.user_variables or result.config.engine_settings:
        response["kettle_properties"] = {
            "user_variable_count": len(result.config.user_variables),
            "engine_setting_count": len(result.config.engine_settings),
            "user_variables": result.config.user_variables,
        }

    if result.config.repositories is not None:
        response["repositories"] = {
            "file_repositories": [
                {"name": r.name, "base_directory": r.base_directory}
                for r in result.config.repositories.file_repositories
            ],
            "database_repositories": [
                {"name": r.name, "connection_name": r.connection_name}
                for r in result.config.repositories.database_repositories
            ],
        }

    return JSONResponse(response)


@app.post("/api/generate")
async def generate(payload: dict[str, Any]) -> JSONResponse:
    """
    Generate Airflow DAG source from a (possibly edited) IR graph.
    Refuses if there are missing references — returns 422 with a list.
    """
    if "graph" not in payload:
        raise HTTPException(400, "missing 'graph' in request body")
    try:
        graph = WorkflowGraph.from_dict(payload["graph"])
    except Exception as e:
        raise HTTPException(400, f"invalid IR graph: {e}")

    ok, issues = graph.is_resolvable()
    if not ok:
        return JSONResponse(
            {
                "error": "graph has unresolved references — generation blocked",
                "issues": issues,
            },
            status_code=422,
        )

    try:
        dag_src = generate_dag_source(graph)
        report = generate_migration_report(graph)
    except Exception as e:
        raise HTTPException(500, f"generation failed: {e}")

    return JSONResponse({
        "dag_filename": f"{graph.dag_id}.py",
        "dag_source": dag_src,
        "report_filename": f"{graph.dag_id}_migration_report.md",
        "report": report,
    })


# ----------------------------------------------------------------------
# Static frontend
# ----------------------------------------------------------------------

_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=_FRONTEND_DIR / "assets"), name="assets")

    @app.get("/")
    async def root() -> HTMLResponse:
        index = _FRONTEND_DIR / "index.html"
        return HTMLResponse(index.read_text(encoding="utf-8"))
else:
    @app.get("/")
    async def root_dev() -> HTMLResponse:
        return HTMLResponse(
            "<!doctype html><meta charset=utf-8>"
            "<title>pdi2airflow</title>"
            "<body style='font-family:ui-monospace,monospace;background:#0c0c0e;color:#d4d4d8;padding:2rem;'>"
            "<h1>pdi2airflow — backend running</h1>"
            "<p>Frontend not yet built. From the project root, run:</p>"
            "<pre>cd frontend &amp;&amp; npm install &amp;&amp; npm run dev</pre>"
            "<p>Then open <a href='http://localhost:5173' style='color:#facc15'>http://localhost:5173</a>.</p>"
            "<p>Or build for production:</p>"
            "<pre>cd frontend &amp;&amp; npm run build</pre>"
            "<p>...and reload this page.</p>"
            "</body>"
        )
