"""
Maps PDI connection definitions (.kdb / inline <connection> elements) to
Airflow Connection IDs.

Mapping rules are loaded from connection_map.json (checked-in defaults) with
an optional override at config/connection_map.json (excluded from git).

The mapper is intentionally conservative — it returns 'none' confidence
when it can't be sure, and the workflow then surfaces the issue rather
than silently picking a wrong connection.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..models.ir import ConnectionRef


@dataclass(frozen=True)
class _ConnEntry:
    airflow_id: str
    db_type: str            # MSSQL | POSTGRESQL | MYSQL | ORACLE | BIGQUERY
    pdi_names: tuple[str, ...]      # exact PDI connection names -> "exact" tier
    host_keywords: tuple[str, ...]
    database_keywords: tuple[str, ...]
    name_keywords: tuple[str, ...]
    port: str | None = None         # exact port match (optional extra signal)


_DEFAULT_MAP = Path(__file__).parent / "connection_map.json"
_OVERRIDE_MAP = Path(__file__).parent.parent.parent / "config" / "connection_map.json"


def _normalize(s: str | None) -> str:
    return (s or "").lower().replace("-", "_").replace(" ", "_")


def _load_entries() -> list[_ConnEntry]:
    """Load connection map from JSON. Override file takes full precedence if present."""
    path = _OVERRIDE_MAP if _OVERRIDE_MAP.exists() else _DEFAULT_MAP
    with path.open(encoding="utf-8") as f:
        raw = json.load(f)

    entries: list[_ConnEntry] = []
    for item in raw:
        if "airflow_id" not in item:
            continue
        entries.append(
            _ConnEntry(
                airflow_id=item["airflow_id"],
                db_type=item["db_type"].upper(),
                # Normalize pdi_names so space/hyphen variants match the normalized pdi_name
                pdi_names=tuple(_normalize(n) for n in item.get("pdi_names", [])),
                host_keywords=tuple(_normalize(k) for k in item.get("host_keywords", [])),
                database_keywords=tuple(_normalize(k) for k in item.get("database_keywords", [])),
                name_keywords=tuple(_normalize(k) for k in item.get("name_keywords", [])),
                port=item.get("port"),
            )
        )
    return entries


def suggest_airflow_connection(conn: ConnectionRef) -> ConnectionRef:
    """
    Returns a new ConnectionRef with `suggested_airflow_conn` and `confidence` set.
    Confidence levels (highest to lowest):
        - "exact":      pdi_name matches an explicit pdi_names entry in the map
        - "host_match": host / database / port attributes match a known connection's keywords
        - "type_match": only db_type matches; suggested only if exactly one candidate
        - "none":       no plausible match — generation will block on this
    """
    entries = _load_entries()

    pdi_name = _normalize(conn.pdi_name)
    host = _normalize(conn.host)
    db = _normalize(conn.database)
    db_type = (conn.db_type or "").upper()
    port = (conn.port or "").strip()

    # Tier 1: exact PDI name match (explicit pdi_names list in JSON)
    for e in entries:
        if pdi_name in (n.lower() for n in e.pdi_names):
            return _set(conn, e.airflow_id, "exact")

    # Tier 1b: pdi_name equals the airflow_id itself (legacy behaviour)
    for e in entries:
        if pdi_name == e.airflow_id.lower():
            return _set(conn, e.airflow_id, "exact")

    # Tier 2: attribute keyword match within same db_type.
    #
    # Matching rules (applied per entry, top-to-bottom in the JSON):
    #   - Port filter: if the entry specifies a port and the connection has a port,
    #     they must match (splits DWH write port 5000 from read port 5001).
    #   - When an entry specifies BOTH host_keywords AND database_keywords, BOTH must
    #     match — this prevents false positives on shared-backend servers where many
    #     connections have the same host but different databases.
    #   - When name_keywords are also specified, name must match too.  Place more-
    #     specific (name-discriminated) entries before generic ones in the JSON so
    #     that e.g. UAT entries are tested before their prod counterparts.
    #   - If an entry specifies only one of host/db/name, that one signal suffices.
    for e in entries:
        if e.db_type != db_type:
            continue
        if e.port and port and port != e.port:
            continue
        host_hit = any(k in host for k in e.host_keywords) if host else False
        db_hit = any(k in db for k in e.database_keywords) if db else False
        name_hit = any(k in pdi_name for k in e.name_keywords)
        if e.host_keywords and e.database_keywords:
            if not (host_hit and db_hit):
                continue
            if e.name_keywords and not name_hit:
                continue
            return _set(conn, e.airflow_id, "host_match")
        elif host_hit or db_hit or name_hit:
            return _set(conn, e.airflow_id, "host_match")

    # Tier 3: db_type match only — weakest signal, only if exactly one candidate
    candidates = [e for e in entries if e.db_type == db_type]
    if len(candidates) == 1:
        return _set(conn, candidates[0].airflow_id, "type_match")

    # Tier 4: no match
    return _set(conn, None, "none")


def _set(conn: ConnectionRef, airflow_id: str | None, confidence: str) -> ConnectionRef:
    return ConnectionRef(
        pdi_name=conn.pdi_name,
        db_type=conn.db_type,
        host=conn.host,
        database=conn.database,
        port=conn.port,
        suggested_airflow_conn=airflow_id,
        confidence=confidence,
    )
