"""
High-level entry point for ingesting uploaded PDI files.

Accepts any combination of:
  - .kjb / .ktr / .kdb        : individual PDI artifacts
  - .zip                       : archive containing any of the above
  - kettle.properties          : user-defined variables (PDI's global var store)
  - repositories.xml           : repository definitions (file roots + DB repos)

Produces an IR WorkflowGraph with cross-references resolved where possible
and missing references explicitly enumerated.
"""

from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass, field
from io import BytesIO

from ..models.ir import (
    ConnectionRef,
    Edge,
    Node,
    NodeKind,
    VariableRef,
    WorkflowGraph,
)
from .kdb_parser import parse_kdb_bytes
from .kettle_properties_parser import KettlePropertiesResult, parse_kettle_properties_bytes
from .kjb_parser import parse_kjb_bytes
from .ktr_parser import parse_ktr_bytes
from .repositories_parser import RepositoriesResult, parse_repositories_xml_bytes


@dataclass
class ProjectConfig:
    """Aggregated config from kettle.properties and repositories.xml."""

    user_variables: dict[str, str] = field(default_factory=dict)
    engine_settings: dict[str, str] = field(default_factory=dict)
    repositories: RepositoriesResult | None = None

    def resolve_variable(self, name: str) -> str | None:
        """Return the user-defined default value for ${name}, if known."""
        return self.user_variables.get(name)


class IngestResult:
    """Accumulator returned to the API layer."""

    def __init__(self) -> None:
        self.graphs: list[WorkflowGraph] = []
        self.standalone_ktrs: dict[str, tuple[Node, list[ConnectionRef], list[VariableRef], list[str]]] = {}
        self.connections: dict[str, ConnectionRef] = {}
        self.errors: list[str] = []
        self.config: ProjectConfig = ProjectConfig()

    def primary_graph(self) -> WorkflowGraph | None:
        """Return the first .kjb-derived graph, or build one from a single .ktr."""
        if self.graphs:
            return self.graphs[0]
        if len(self.standalone_ktrs) == 1:
            (path, (node, conns, vars_, unmapped)) = next(iter(self.standalone_ktrs.items()))
            graph = WorkflowGraph(
                dag_id=node.id,
                nodes=[
                    Node(id="start_task", label="start_task", kind=NodeKind.START, position={"x": 0, "y": 0}),
                    node,
                    Node(id="end", label="end", kind=NodeKind.END, position={"x": 480, "y": 0}),
                ],
                edges=[],
                connections=list(conns),
                variables=list(vars_),
                unmapped_step_types=list(unmapped),
                source_kjb=path,
                description=node.config.get("description", "") if isinstance(node.config, dict) else "",
            )
            graph.nodes[1].position = {"x": 240, "y": 0}
            graph.edges = [
                Edge(source="start_task", target=node.id),
                Edge(source=node.id, target="end"),
            ]
            _apply_project_config(graph, self.config, source_path=path)
            _populate_missing(graph)
            return graph
        return None


def ingest_files(files: list[tuple[str, bytes]]) -> IngestResult:
    """
    Process a list of (filename, content_bytes) pairs.
    Zip files are unpacked transparently.
    """
    result = IngestResult()
    expanded = _expand_zips(files)

    # Pass 0: parse project-level config files. Done first so subsequent
    # passes can consult variable defaults and repository roots.
    for fname, content in expanded:
        base = os.path.basename(fname).lower()
        if base == "kettle.properties":
            try:
                kp = parse_kettle_properties_bytes(content)
                _merge_kettle_properties(result.config, kp)
            except Exception as e:
                result.errors.append(f"{fname}: failed to parse kettle.properties — {e}")
        elif base == "repositories.xml":
            try:
                repos = parse_repositories_xml_bytes(content)
                result.config.repositories = repos
                # Repository connections also count as known connections —
                # surface them so the .kdb pass can match against them.
                for c in repos.repo_connections:
                    result.connections.setdefault(c.pdi_name, c)
            except Exception as e:
                result.errors.append(f"{fname}: failed to parse repositories.xml — {e}")

    # Pass 1: parse .kdb files first so connection mappings are available
    for fname, content in expanded:
        ext = _ext(fname)
        if ext == ".kdb":
            try:
                conn = parse_kdb_bytes(content)
                result.connections[conn.pdi_name] = conn
            except Exception as e:
                result.errors.append(f"{fname}: failed to parse .kdb — {e}")

    # Pass 2: parse .ktr files (cache them by basename for kjb resolution)
    ktr_cache: dict[str, bytes] = {}
    for fname, content in expanded:
        ext = _ext(fname)
        if ext == ".ktr":
            base = os.path.basename(fname)
            ktr_cache[base] = content
            ktr_cache[fname] = content
            try:
                node, conns, vars_, unmapped = parse_ktr_bytes(content, fname)
                result.standalone_ktrs[fname] = (node, conns, vars_, unmapped)
                for c in conns:
                    result.connections.setdefault(c.pdi_name, c)
            except Exception as e:
                result.errors.append(f"{fname}: failed to parse .ktr — {e}")

    # Pass 3: parse .kjb files and resolve TRANSFORMATION nodes against ktr_cache
    for fname, content in expanded:
        ext = _ext(fname)
        if ext == ".kjb":
            try:
                graph, ktr_refs = parse_kjb_bytes(content, fname)
                _resolve_transformations(graph, ktr_cache, result)
                # Attach inline + standalone connections for the report/UI
                merged: dict[str, ConnectionRef] = {c.pdi_name: c for c in graph.connections}
                for name, c in result.connections.items():
                    merged.setdefault(name, c)
                graph.connections = list(merged.values())
                _apply_project_config(graph, result.config, source_path=fname)
                _populate_missing(graph)
                result.graphs.append(graph)
            except Exception as e:
                result.errors.append(f"{fname}: failed to parse .kjb — {e}")

    return result


# ----------------------------------------------------------------------
# Project-config integration
# ----------------------------------------------------------------------

def _merge_kettle_properties(config: ProjectConfig, kp: KettlePropertiesResult) -> None:
    """Merge a parsed kettle.properties into the running ProjectConfig."""
    for name, value in kp.user_variables.items():
        config.user_variables[name] = value
    for name, value in kp.engine_settings.items():
        config.engine_settings[name] = value


def _apply_project_config(graph: WorkflowGraph, config: ProjectConfig, source_path: str) -> None:
    """
    Resolve graph-level references using ProjectConfig:
      1. Populate VariableRef.default_value from kettle.properties user vars.
      2. Tag the DAG with the originating PDI repository name (if any).
      3. Re-resolve missing_files using repository base directories.
    """
    # Variable defaults from kettle.properties
    if config.user_variables:
        for v in graph.variables:
            if v.default_value is None:
                resolved = config.resolve_variable(v.name)
                if resolved is not None:
                    v.default_value = resolved

    # Repository hints from repositories.xml
    if config.repositories is not None:
        repo = config.repositories.find_repo_for_path(source_path)
        if repo is not None:
            tag = _safe_tag(repo.name)
            # Prefix the dag_id with the repository tag for clearer Airflow grouping
            if not graph.dag_id.startswith(f"{tag}_"):
                graph.dag_id = f"{tag}_{graph.dag_id}"
            # Mark the description with the repo name for the migration report
            if repo.name not in (graph.description or ""):
                graph.description = (
                    f"[{repo.name}] {graph.description}".strip()
                    if graph.description else f"[{repo.name}]"
                )

        # Improve missing-file messages with repo path hints
        if graph.missing_files:
            graph.missing_files = _resolve_missing_against_repos(
                graph.missing_files, config.repositories, source_path
            )


def _resolve_missing_against_repos(
    missing: list[str],
    repos: RepositoriesResult,
    kjb_source_path: str,
) -> list[str]:
    """
    Best-effort: for each missing file path, see if it would have resolved
    against one of the configured repository base_directories. We can't
    actually load the bytes (we don't have filesystem access from the
    uploaded set), but we can rephrase the message to be more useful.

    Returns the list of still-missing paths, with messages clarified.
    """
    source_repo = repos.find_repo_for_path(kjb_source_path)

    out: list[str] = []
    for path in missing:
        # Variable-based references like ${Internal.Job.Filename.Directory}/foo.ktr
        # can be rewritten to point at where the user should look on disk.
        if "${" in path and source_repo is not None:
            tail = path.split("/")[-1].split("\\")[-1]
            hint = f"{source_repo.normalized_path()}/.../{tail}"
            out.append(f"{path}  (expected near: {hint})")
        else:
            out.append(path)
    return out


def _safe_tag(name: str) -> str:
    """Convert a PDI repository name into a safe lower-case Airflow-friendly tag."""
    out: list[str] = []
    for ch in name.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in ("-", "_", " "):
            out.append("_")
    safe = "".join(out).strip("_")
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe or "pdi"


# ----------------------------------------------------------------------
# Existing helpers (kept from prior version)
# ----------------------------------------------------------------------

def _resolve_transformations(
    graph: WorkflowGraph,
    ktr_cache: dict[str, bytes],
    result: IngestResult,
) -> None:
    """
    For every TRANSFORMATION node in the graph, look up the referenced .ktr
    in the cache and attach its parsed steps. If not found, record it under
    graph.missing_files so generation will block.
    """
    for node in graph.nodes:
        if node.kind != NodeKind.TRANSFORMATION:
            continue
        ref = node.config.get("filename") or node.config.get("trans_name") or ""
        if not isinstance(ref, str) or not ref:
            graph.missing_files.append(f"<unnamed transformation referenced by '{node.label}'>")
            continue
        candidates = [ref, os.path.basename(ref), os.path.basename(ref) + ".ktr"]
        if "${" in ref:
            tail = ref.split("/")[-1].split("\\")[-1]
            candidates.append(tail)
            if not tail.endswith(".ktr"):
                candidates.append(tail + ".ktr")
        found_bytes = None
        for c in candidates:
            if c in ktr_cache:
                found_bytes = ktr_cache[c]
                break
        if found_bytes is None:
            graph.missing_files.append(ref)
            continue
        try:
            ktr_node, ktr_conns, ktr_vars, ktr_unmapped = parse_ktr_bytes(found_bytes, ref)
            node.steps = ktr_node.steps
            node.step_edges = ktr_node.step_edges
            for c in ktr_conns:
                if not any(existing.pdi_name == c.pdi_name for existing in graph.connections):
                    graph.connections.append(c)
            for v in ktr_vars:
                if not any(existing.name == v.name for existing in graph.variables):
                    graph.variables.append(v)
            for t in ktr_unmapped:
                if t not in graph.unmapped_step_types:
                    graph.unmapped_step_types.append(t)
        except Exception as e:
            result.errors.append(f"{ref}: failed to parse referenced .ktr — {e}")
            graph.missing_files.append(ref)


def _collect_runtime_set_variables(graph: WorkflowGraph) -> set[str]:
    """
    Return the names of PDI variables published by SetVariable steps anywhere in the graph.
    These are runtime-defined (ROOT_JOB / PARENT_JOB scope) and must not be flagged as
    missing — they are set dynamically by a sibling task and passed via XCom in Airflow.
    """
    names: set[str] = set()
    for node in graph.nodes:
        for step in node.steps:
            if step.pdi_type == "SetVariable":
                for f in (step.config.get("fields") or []):
                    var_name = f.get("variable_name")
                    if var_name:
                        names.add(var_name)
    return names


def _populate_missing(graph: WorkflowGraph) -> None:
    """Identify connections and variables that have no resolution. Idempotent."""
    runtime_set = _collect_runtime_set_variables(graph)

    graph.missing_connections = []
    graph.missing_variables = []
    for c in graph.connections:
        if c.confidence == "none" and c.pdi_name not in graph.missing_connections:
            graph.missing_connections.append(c.pdi_name)
    for v in graph.variables:
        # Variables published by a SetVariable step are defined at runtime via XCom;
        # mark them resolved so generation isn't blocked.
        if not v.default_value and v.name in runtime_set:
            v.default_value = "(runtime — set by SetVariable step, passed via XCom)"
        if not v.default_value and v.name not in graph.missing_variables:
            graph.missing_variables.append(v.name)


def _expand_zips(files: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    """Recursively unpack any .zip files in the input list."""
    out: list[tuple[str, bytes]] = []
    for fname, content in files:
        if _ext(fname) == ".zip":
            try:
                with zipfile.ZipFile(BytesIO(content)) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        sub = zf.read(info.filename)
                        out.append((info.filename, sub))
            except zipfile.BadZipFile:
                out.append((fname, content))
        else:
            out.append((fname, content))
    return out


def _ext(filename: str) -> str:
    return os.path.splitext(filename.lower())[1]
