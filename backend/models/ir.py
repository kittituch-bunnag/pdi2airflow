"""
Intermediate Representation (IR) for PDI -> Airflow migration.

Design philosophy:
  - PDI parsers produce IR objects.
  - The React Flow UI edits IR-equivalent JSON.
  - Airflow generators consume IR objects.
  - The IR is the single source of truth; PDI specifics never leak into generators.

A WorkflowGraph mirrors a single Airflow DAG. Nodes are tasks; edges are dependencies.
A node's `kind` determines which Airflow operator/codegen path it takes.
A node's `config` carries operator-specific parameters in a stable, documented shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class NodeKind(str, Enum):
    """
    Maps roughly 1:1 to a generated Airflow task type.
    Each kind has a documented `config` schema (see codegen).
    """

    # Structural
    START = "start"                # DummyOperator
    END = "end"                    # DummyOperator (trigger_rule="none_failed")
    DUMMY = "dummy"                # DummyOperator (e.g. SUCCESS entries)

    # PDI Job entries -> tasks
    TRANSFORMATION = "transformation"   # PythonOperator wrapping translated .ktr logic
    SUBJOB = "subjob"                   # TriggerDagRunOperator OR inlined subgraph
    SQL = "sql"                         # PythonOperator running SQL via plugin
    SHELL = "shell"                     # BashOperator
    EVAL = "eval"                       # BranchPythonOperator (PDI EVAL job entry)
    WAITFOR = "waitfor"                 # TimeDeltaSensor / equivalent

    # Catch-all
    UNKNOWN = "unknown"            # Placeholder PythonOperator with TODO


@dataclass
class Edge:
    """A directed dependency: source -> target.

    `condition` mirrors PDI hop semantics:
      - "unconditional": always follow
      - "success": follow on success (default for non-EVAL upstreams)
      - "failure": follow on failure (rare; usually mapped to trigger_rule='one_failed')
    """

    source: str         # node id
    target: str         # node id
    condition: str = "unconditional"


@dataclass
class TransformationStep:
    """
    One step inside a .ktr (e.g. TableInput, TableOutput, SelectValues).
    These do NOT become Airflow tasks — they are inlined into the Python callable
    of a TRANSFORMATION node (per the hybrid mapping decision).
    """

    step_id: str
    name: str
    pdi_type: str       # e.g. "TableInput", "TableOutput", "ExecSQL"
    config: dict[str, Any] = field(default_factory=dict)
    # Steps within a .ktr also have their own internal hops
    inputs: list[str] = field(default_factory=list)   # step_ids feeding this step
    outputs: list[str] = field(default_factory=list)  # step_ids fed by this step


@dataclass
class Node:
    """A single task in the generated DAG."""

    id: str                         # stable id, used as task_id in Airflow
    label: str                      # human-readable name from PDI
    kind: NodeKind
    config: dict[str, Any] = field(default_factory=dict)

    # Only populated when kind == TRANSFORMATION
    steps: list[TransformationStep] = field(default_factory=list)
    step_edges: list[Edge] = field(default_factory=list)

    # Source provenance (for the migration report)
    source_file: str | None = None  # original .ktr/.kjb path

    # UI hints
    position: dict[str, float] = field(default_factory=lambda: {"x": 0.0, "y": 0.0})


@dataclass
class ConnectionRef:
    """A PDI database connection referenced by the workflow."""

    pdi_name: str                   # name as it appears in PDI
    db_type: str                    # MSSQL, POSTGRESQL, etc.
    host: str | None = None
    database: str | None = None
    port: str | None = None
    # Filled by the connection mapper; None if no confident mapping exists
    suggested_airflow_conn: str | None = None
    confidence: str = "none"        # "exact", "host_match", "type_match", "none"


@dataclass
class VariableRef:
    """A PDI variable (${var.name}) referenced anywhere in the workflow."""

    name: str
    used_in: list[str] = field(default_factory=list)  # node ids that use it
    default_value: str | None = None


@dataclass
class WorkflowGraph:
    """The full intermediate representation of a single workflow / future DAG."""

    dag_id: str
    nodes: list[Node]
    edges: list[Edge]
    connections: list[ConnectionRef] = field(default_factory=list)
    variables: list[VariableRef] = field(default_factory=list)

    # Things the parser couldn't resolve. Per spec: generation must FAIL until empty.
    missing_files: list[str] = field(default_factory=list)         # referenced subjobs/.ktr not uploaded
    missing_connections: list[str] = field(default_factory=list)   # connection names with no mapping
    missing_variables: list[str] = field(default_factory=list)     # variables with no default and no override
    unmapped_step_types: list[str] = field(default_factory=list)   # PDI step types we don't know

    # Metadata
    source_kjb: str | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize for the React Flow front-end."""
        return {
            "dag_id": self.dag_id,
            "description": self.description,
            "source_kjb": self.source_kjb,
            "nodes": [
                {
                    "id": n.id,
                    "label": n.label,
                    "kind": n.kind.value,
                    "config": n.config,
                    "source_file": n.source_file,
                    "position": n.position,
                    "steps": [asdict(s) for s in n.steps],
                    "step_edges": [asdict(e) for e in n.step_edges],
                }
                for n in self.nodes
            ],
            "edges": [asdict(e) for e in self.edges],
            "connections": [asdict(c) for c in self.connections],
            "variables": [asdict(v) for v in self.variables],
            "missing_files": self.missing_files,
            "missing_connections": self.missing_connections,
            "missing_variables": self.missing_variables,
            "unmapped_step_types": self.unmapped_step_types,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowGraph":
        """Reconstruct from edited React Flow state."""
        nodes = []
        for nd in data.get("nodes", []):
            steps = [TransformationStep(**s) for s in nd.get("steps", [])]
            step_edges = [Edge(**e) for e in nd.get("step_edges", [])]
            nodes.append(
                Node(
                    id=nd["id"],
                    label=nd["label"],
                    kind=NodeKind(nd["kind"]),
                    config=nd.get("config", {}),
                    steps=steps,
                    step_edges=step_edges,
                    source_file=nd.get("source_file"),
                    position=nd.get("position", {"x": 0.0, "y": 0.0}),
                )
            )
        edges = [Edge(**e) for e in data.get("edges", [])]
        connections = [ConnectionRef(**c) for c in data.get("connections", [])]
        variables = [VariableRef(**v) for v in data.get("variables", [])]
        return cls(
            dag_id=data["dag_id"],
            nodes=nodes,
            edges=edges,
            connections=connections,
            variables=variables,
            missing_files=data.get("missing_files", []),
            missing_connections=data.get("missing_connections", []),
            missing_variables=data.get("missing_variables", []),
            unmapped_step_types=data.get("unmapped_step_types", []),
            source_kjb=data.get("source_kjb"),
            description=data.get("description", ""),
        )

    def is_resolvable(self) -> tuple[bool, list[str]]:
        """Return (ok, list of blocking issues). Used to enforce 'fail loudly'."""
        issues: list[str] = []
        for f in self.missing_files:
            issues.append(f"Missing referenced file: {f}")
        for c in self.missing_connections:
            issues.append(f"Unmapped PDI connection: {c}")
        for v in self.missing_variables:
            issues.append(f"Undefined variable: ${{{v}}}")
        for t in self.unmapped_step_types:
            issues.append(f"Unmapped PDI step type: {t}")
        return (len(issues) == 0, issues)
