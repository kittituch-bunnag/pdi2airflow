"""
Parser for PDI .ktr files (transformations).

A transformation has:
  - <info><name> ... </name></info>     : transformation name
  - <connection> elements (zero or more): inline DB connection defs
  - <step> elements (one or more)       : pipeline steps
  - <order><hop> elements               : connections between steps
  - <parameters>/<variable>             : declared variables

We map each transformation to ONE IR Node of kind TRANSFORMATION whose
`steps` list contains every <step>. Hops between steps are captured in
the node's `step_edges` so the React Flow UI can show them and the
generator can emit code in topological order.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from ..models.ir import (
    ConnectionRef,
    Edge,
    Node,
    NodeKind,
    TransformationStep,
    VariableRef,
)
from .kdb_parser import parse_inline_connection
from .xml_util import find_pdi_variables, parse_xml_bytes, text_of


# PDI step types we know how to translate to plugin-based Python code.
# Anything not in this set is parsed but generation will mark it unmapped.
KNOWN_STEP_TYPES = {
    "TableInput",        # SELECT from source DB
    "TableOutput",       # INSERT into target DB
    "ExecSQL",           # arbitrary SQL exec
    "SelectValues",      # rename/reorder/cast columns
    "FilterRows",        # WHERE-like filter
    "Constant",          # add literal columns
    "GetVariable",       # read PDI variable into stream
    "SetVariable",       # write stream value to PDI variable
    "SystemInfo",        # inject system metadata columns (date, hostname, PID…)
    "SetValueConstant",  # overwrite column values with literals
    "JsonOutput",        # serialize stream to JSON
    "Rest",              # call an HTTP REST endpoint
    "ScriptValueMod",    # run JavaScript to add/modify fields
    "WriteToLog",        # log rows to PDI log
}


def parse_ktr_bytes(content: bytes, source_path: str = "") -> tuple[Node, list[ConnectionRef], list[VariableRef], list[str]]:
    """
    Parse a .ktr file's bytes.

    Returns:
      node:        single IR Node of kind TRANSFORMATION holding all steps
      connections: inline <connection> elements found
      variables:   ${var} references found in any step config
      unmapped:    PDI step types we don't have a generator for
    """
    root = parse_xml_bytes(content)

    # Transformation name (becomes task_id)
    trans_name = text_of(root, "info/name") or "unnamed_transformation"
    trans_desc = text_of(root, "info/description")

    # Inline DB connections
    connections = [parse_inline_connection(e) for e in root.findall("connection")]

    # Steps
    steps: list[TransformationStep] = []
    unmapped: list[str] = []
    var_refs: dict[str, VariableRef] = {}

    for step_elem in root.findall("step"):
        step = _parse_step(step_elem)
        steps.append(step)
        if step.pdi_type not in KNOWN_STEP_TYPES:
            if step.pdi_type not in unmapped:
                unmapped.append(step.pdi_type)
        # Collect variable references
        for var in _step_variable_refs(step_elem):
            var_refs.setdefault(var, VariableRef(name=var, used_in=[]))
            if step.step_id not in var_refs[var].used_in:
                var_refs[var].used_in.append(step.step_id)

    # Hops between steps (internal to the .ktr)
    step_edges: list[Edge] = []
    order = root.find("order")
    if order is not None:
        for hop in order.findall("hop"):
            src = text_of(hop, "from")
            tgt = text_of(hop, "to")
            enabled = text_of(hop, "enabled", "Y").upper() == "Y"
            if src and tgt and enabled:
                # Use step ids by name -> keyed lookup later
                step_edges.append(Edge(source=src, target=tgt, condition="unconditional"))

    # Wire inputs/outputs onto each step using the hops
    name_to_step = {s.name: s for s in steps}
    for e in step_edges:
        if e.source in name_to_step and e.target in name_to_step:
            name_to_step[e.source].outputs.append(name_to_step[e.target].step_id)
            name_to_step[e.target].inputs.append(name_to_step[e.source].step_id)

    node = Node(
        id=_safe_id(trans_name),
        label=trans_name,
        kind=NodeKind.TRANSFORMATION,
        config={
            "description": trans_desc,
            "ktr_filename": source_path,
        },
        steps=steps,
        # Convert step_edges from name-based to step_id-based for IR consistency
        step_edges=[
            Edge(
                source=name_to_step[e.source].step_id,
                target=name_to_step[e.target].step_id,
                condition=e.condition,
            )
            for e in step_edges
            if e.source in name_to_step and e.target in name_to_step
        ],
        source_file=source_path,
    )

    return node, connections, list(var_refs.values()), unmapped


def _parse_step(step_elem: ET.Element) -> TransformationStep:
    """Pull the common fields plus type-specific config from a <step>."""
    name = text_of(step_elem, "name") or "unnamed_step"
    pdi_type = text_of(step_elem, "type") or "Unknown"

    config: dict[str, object] = {}

    if pdi_type == "TableInput":
        config["connection"] = text_of(step_elem, "connection")
        config["sql"] = text_of(step_elem, "sql")
        config["limit"] = text_of(step_elem, "limit")
    elif pdi_type == "TableOutput":
        config["connection"] = text_of(step_elem, "connection")
        config["schema"] = text_of(step_elem, "schema")
        config["table"] = text_of(step_elem, "table")
        config["truncate"] = text_of(step_elem, "truncate", "N").upper() == "Y"
        config["commit_size"] = text_of(step_elem, "commit", "1000")
    elif pdi_type == "ExecSQL":
        config["connection"] = text_of(step_elem, "connection")
        config["sql"] = text_of(step_elem, "sql")
        config["execute_each_row"] = text_of(step_elem, "execute_each_row", "N").upper() == "Y"
    elif pdi_type == "SelectValues":
        # PDI nests these under <fields><field>...
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "rename": text_of(f, "rename"),
                    "type": text_of(f, "type"),
                })
        config["fields"] = fields
    elif pdi_type == "FilterRows":
        config["condition"] = text_of(step_elem, "condition")
        config["send_true_to"] = text_of(step_elem, "send_true_to")
        config["send_false_to"] = text_of(step_elem, "send_false_to")
    elif pdi_type == "GetVariable":
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "variable": text_of(f, "variable"),
                    "type": text_of(f, "type"),
                })
        config["fields"] = fields
    elif pdi_type == "SetVariable":
        # Note: SetVariable uses <field_name>/<variable_name>, not <name>/<variable>
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "field_name": text_of(f, "field_name"),       # source column in the stream
                    "variable_name": text_of(f, "variable_name"), # PDI variable to publish
                    "variable_type": text_of(f, "variable_type", "PARENT_JOB"),
                    "default_value": text_of(f, "default_value"),
                })
        config["fields"] = fields
    elif pdi_type == "SystemInfo":
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "type": text_of(f, "type"),
                })
        config["fields"] = fields
    elif pdi_type == "SetValueConstant":
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "value": text_of(f, "value"),
                    "conversion_mask": text_of(f, "conversion_mask"),
                })
        config["fields"] = fields
    elif pdi_type == "JsonOutput":
        config["filename"] = text_of(step_elem, "filename")
        config["json_bloc"] = text_of(step_elem, "json_bloc")
        config["nr_rows_in_block"] = text_of(step_elem, "nr_rows_in_block")
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "element": text_of(f, "element"),
                })
        config["fields"] = fields
    elif pdi_type == "Rest":
        config["url"] = text_of(step_elem, "url")
        config["method"] = text_of(step_elem, "method", "GET")
        config["body_field"] = text_of(step_elem, "body_field")
        config["application_type"] = text_of(step_elem, "application_type")
        config["result_fieldname"] = text_of(step_elem, "result_fieldname")
        config["result_code_fieldname"] = text_of(step_elem, "result_code_fieldname")
        headers = []
        matrix_elem = step_elem.find("matrix")
        if matrix_elem is not None:
            for h in matrix_elem.findall("httpheader"):
                headers.append({
                    "name": text_of(h, "name"),
                    "value": text_of(h, "value"),
                })
        config["headers"] = headers
    elif pdi_type == "ScriptValueMod":
        scripts = []
        for js_elem in step_elem.findall("jsScript"):
            scripts.append({
                "name": text_of(js_elem, "jsScript_name"),
                "type": text_of(js_elem, "jsScript_type"),
                "script": text_of(js_elem, "jsScript_script"),
            })
        config["scripts"] = scripts
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({
                    "name": text_of(f, "name"),
                    "rename": text_of(f, "rename"),
                    "type": text_of(f, "type"),
                })
        config["fields"] = fields
    elif pdi_type == "WriteToLog":
        config["log_message"] = text_of(step_elem, "log_message")
        config["log_level"] = text_of(step_elem, "log_level", "Basic")
        config["display_header"] = text_of(step_elem, "display_header", "Y").upper() == "Y"
        fields = []
        fields_elem = step_elem.find("fields")
        if fields_elem is not None:
            for f in fields_elem.findall("field"):
                fields.append({"name": text_of(f, "name")})
        config["fields"] = fields
    else:
        # Unknown type — keep the raw subtree as a hint for the developer
        config["raw_xml"] = ET.tostring(step_elem, encoding="unicode")

    return TransformationStep(
        step_id=_safe_id(f"{pdi_type}_{name}"),
        name=name,
        pdi_type=pdi_type,
        config=config,
    )


def _step_variable_refs(step_elem: ET.Element) -> list[str]:
    """Walk the step subtree looking for ${var} references in any text field."""
    refs: list[str] = []
    for sub in step_elem.iter():
        for v in find_pdi_variables(sub.text):
            if v not in refs:
                refs.append(v)
    return refs


def _safe_id(s: str) -> str:
    """Make a string usable as both a Python identifier and an Airflow task_id."""
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
