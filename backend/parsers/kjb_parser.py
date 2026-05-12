"""
Parser for PDI .kjb files (jobs).

A PDI job has:
  - <name> ... </name>                     : job name -> dag_id
  - <connection> elements                  : inline DB connections
  - <entries><entry> elements              : one Airflow task per entry
  - <hops><hop>                            : dependencies between entries
  - <parameters>                           : declared variables

Each <entry> has a <type> which determines the IR NodeKind:
    SPECIAL (start=Y) -> START
    SPECIAL (dummy=Y / "SUCCESS") -> END (we treat the SUCCESS node as the sink)
    TRANS                      -> TRANSFORMATION   (linked to a .ktr)
    JOB                        -> SUBJOB           (linked to another .kjb)
    SQL                        -> SQL
    SHELL                      -> SHELL
    EVAL                       -> EVAL
    WAITFOR / DELAY            -> WAITFOR
    anything else              -> UNKNOWN          (parsed but flagged)
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from ..models.ir import (
    ConnectionRef,
    Edge,
    Node,
    NodeKind,
    VariableRef,
    WorkflowGraph,
)
from .kdb_parser import parse_inline_connection
from .xml_util import find_pdi_variables, parse_xml_bytes, text_of


# Map PDI <type> to NodeKind
ENTRY_TYPE_MAP: dict[str, NodeKind] = {
    "TRANS": NodeKind.TRANSFORMATION,
    "JOB": NodeKind.SUBJOB,
    "SQL": NodeKind.SQL,
    "SHELL": NodeKind.SHELL,
    "EVAL": NodeKind.EVAL,
    "EVAL_FILES": NodeKind.EVAL,
    "WAITFOR": NodeKind.WAITFOR,
    "DELAY": NodeKind.WAITFOR,
    "SUCCESS": NodeKind.END,
}


def parse_kjb_bytes(content: bytes, source_path: str = "") -> tuple[WorkflowGraph, list[str]]:
    """
    Parse a .kjb file's bytes.

    Returns:
      graph:           IR WorkflowGraph (without ktr step expansion — done later)
      ktr_references:  list of .ktr filenames this job needs in order to fully resolve
    """
    root = parse_xml_bytes(content)

    job_name = text_of(root, "name") or "unnamed_job"
    description = text_of(root, "description")

    # Inline connections (rare in jobs but possible)
    connections = [parse_inline_connection(e) for e in root.findall("connection")]

    # Job entries
    nodes: list[Node] = []
    ktr_refs: list[str] = []
    name_to_id: dict[str, str] = {}
    var_refs: dict[str, VariableRef] = {}

    # Job-level parameters declared in <parameters>/<parameter>.
    # These are runtime inputs analogous to dag_run.conf keys — they must NOT
    # block generation even when no default_value is provided in the XML.
    declared_params: dict[str, str | None] = {}
    params_elem = root.find("parameters")
    if params_elem is not None:
        for param in params_elem.findall("parameter"):
            pname = text_of(param, "name")
            if pname:
                declared_params[pname] = text_of(param, "default_value") or None

    entries_elem = root.find("entries")
    if entries_elem is not None:
        for entry in entries_elem.findall("entry"):
            node = _entry_to_node(entry)
            nodes.append(node)
            name_to_id[node.label] = node.id

            if node.kind == NodeKind.TRANSFORMATION:
                fname = node.config.get("filename") or node.config.get("trans_name")
                if fname:
                    ktr_refs.append(fname)
            elif node.kind == NodeKind.SUBJOB:
                fname = node.config.get("filename") or node.config.get("job_name")
                if fname:
                    ktr_refs.append(fname)

            # Collect variable references from the entry subtree
            for sub in entry.iter():
                for v in find_pdi_variables(sub.text):
                    var_refs.setdefault(v, VariableRef(name=v, used_in=[]))
                    if node.id not in var_refs[v].used_in:
                        var_refs[v].used_in.append(node.id)

    # Edges between entries
    edges: list[Edge] = []
    hops_elem = root.find("hops")
    if hops_elem is not None:
        for hop in hops_elem.findall("hop"):
            src = text_of(hop, "from")
            tgt = text_of(hop, "to")
            enabled = text_of(hop, "enabled", "Y").upper() == "Y"
            unconditional = text_of(hop, "unconditional", "N").upper() == "Y"
            evaluation = text_of(hop, "evaluation", "Y").upper() == "Y"
            if not (src and tgt and enabled):
                continue
            if src not in name_to_id or tgt not in name_to_id:
                # Hop refers to an entry we couldn't parse — skip rather than crash
                continue
            condition = (
                "unconditional" if unconditional
                else "success" if evaluation
                else "failure"
            )
            edges.append(Edge(source=name_to_id[src], target=name_to_id[tgt], condition=condition))

    # Auto-layout: simple BFS-based topological levels for initial UI placement
    _layout(nodes, edges)

    # Stamp declared job parameters so _populate_missing() doesn't flag them.
    # Use the PDI default_value when present, otherwise a descriptive sentinel.
    _PARAM_SENTINEL = "(job parameter — supply via dag_run.conf or Airflow Variable)"
    for pname, pdefault in declared_params.items():
        vref = var_refs.setdefault(pname, VariableRef(name=pname, used_in=[]))
        if vref.default_value is None:
            vref.default_value = pdefault if pdefault else _PARAM_SENTINEL

    graph = WorkflowGraph(
        dag_id=_safe_id(f"dms_{job_name}"),
        nodes=nodes,
        edges=edges,
        connections=connections,
        variables=list(var_refs.values()),
        source_kjb=source_path,
        description=description,
    )

    return graph, ktr_refs


def _entry_to_node(entry: ET.Element) -> Node:
    """Map a single <entry> to an IR Node."""
    name = text_of(entry, "name") or "unnamed_entry"
    pdi_type = text_of(entry, "type") or "UNKNOWN"

    # PDI marks the START entry with a <start>Y</start> child of type SPECIAL.
    # Terminal SUCCESS entries are also type=SPECIAL (typically named "SUCCESS"
    # or similar) with start=N, dummy=N — we treat these as DUMMY for now and
    # let the wiring layer recognize the terminal node by graph position.
    if pdi_type == "SPECIAL":
        is_start = text_of(entry, "start", "N").upper() == "Y"
        if is_start:
            kind = NodeKind.START
        else:
            # Includes both "DUMMY" and "SUCCESS" variants — both render as DummyOperator
            kind = NodeKind.DUMMY
    else:
        kind = ENTRY_TYPE_MAP.get(pdi_type, NodeKind.UNKNOWN)

    config: dict[str, object] = {"pdi_type": pdi_type}

    if kind == NodeKind.TRANSFORMATION:
        config["filename"] = text_of(entry, "filename")
        config["trans_name"] = text_of(entry, "transname")
        config["directory"] = text_of(entry, "directory")
    elif kind == NodeKind.SUBJOB:
        config["filename"] = text_of(entry, "filename")
        config["job_name"] = text_of(entry, "jobname")
        config["directory"] = text_of(entry, "directory")
    elif kind == NodeKind.SQL:
        config["connection"] = text_of(entry, "connection")
        config["sql"] = text_of(entry, "sql")
        config["use_variable_substitution"] = (
            text_of(entry, "useVariableSubstitution", "N").upper() == "Y"
        )
    elif kind == NodeKind.SHELL:
        config["script"] = text_of(entry, "script")
        config["filename"] = text_of(entry, "filename")
        config["work_directory"] = text_of(entry, "work_directory")
    elif kind == NodeKind.EVAL:
        config["script"] = text_of(entry, "script")
    elif kind == NodeKind.WAITFOR:
        config["maximum_timeout"] = text_of(entry, "maximumTimeout")
        config["check_cycle_time"] = text_of(entry, "checkCycleTime")
    elif kind == NodeKind.UNKNOWN:
        config["raw_xml"] = ET.tostring(entry, encoding="unicode")

    return Node(
        id=_safe_id(name),
        label=name,
        kind=kind,
        config=config,
    )


def _layout(nodes: list[Node], edges: list[Edge]) -> None:
    """
    Assign initial (x, y) positions for React Flow using a simple
    topological-level layout: x = depth from any START node, y = sibling rank.
    """
    if not nodes:
        return
    children: dict[str, list[str]] = {n.id: [] for n in nodes}
    indeg: dict[str, int] = {n.id: 0 for n in nodes}
    for e in edges:
        if e.source in children and e.target in indeg:
            children[e.source].append(e.target)
            indeg[e.target] += 1

    # Roots: indeg 0
    levels: dict[str, int] = {}
    queue = [n.id for n in nodes if indeg[n.id] == 0]
    for nid in queue:
        levels[nid] = 0
    visited = set(queue)
    while queue:
        nid = queue.pop(0)
        for ch in children[nid]:
            cand = levels[nid] + 1
            if ch not in levels or levels[ch] < cand:
                levels[ch] = cand
            if ch not in visited:
                visited.add(ch)
                queue.append(ch)
    # Any orphans (cycles or isolated) get level 0
    for n in nodes:
        levels.setdefault(n.id, 0)

    # Bucket by level for y positioning
    by_level: dict[int, list[str]] = {}
    for nid, lvl in levels.items():
        by_level.setdefault(lvl, []).append(nid)

    X_GAP = 240
    Y_GAP = 110
    for lvl, ids in by_level.items():
        for i, nid in enumerate(ids):
            n = next(n for n in nodes if n.id == nid)
            n.position = {"x": float(lvl * X_GAP), "y": float(i * Y_GAP)}


def _safe_id(s: str) -> str:
    out = []
    for ch in s:
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    safe = "".join(out).strip("_") or "task"
    if safe[0].isdigit():
        safe = "t_" + safe
    return safe
