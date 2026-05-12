"""
Parser for PDI .kdb files (database connection definitions).

PDI's .kdb is a small XML file describing one connection. Schematic example:

    <?xml version="1.0" encoding="UTF-8"?>
    <connection>
      <name>source_mssql</name>
      <server>10.0.0.5</server>
      <type>MSSQL</type>
      <access>Native</access>
      <database>my_source_db</database>
      <port>1433</port>
      <username>${db.user}</username>
      <password>Encrypted ...</password>
    </connection>

Inline <connection> elements inside .ktr/.kjb files use the same shape, so this
parser is reused there.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from ..models.ir import ConnectionRef
from .connection_mapper import suggest_airflow_connection
from .xml_util import parse_xml_bytes, text_of


def parse_kdb_bytes(content: bytes) -> ConnectionRef:
    """Parse a standalone .kdb file's contents."""
    root = parse_xml_bytes(content)
    return _connection_from_element(root)


def parse_inline_connection(elem: ET.Element) -> ConnectionRef:
    """Parse a <connection> element embedded inside a .ktr or .kjb."""
    return _connection_from_element(elem)


def _connection_from_element(elem: ET.Element) -> ConnectionRef:
    raw = ConnectionRef(
        pdi_name=text_of(elem, "name") or "unnamed",
        db_type=text_of(elem, "type") or "UNKNOWN",
        host=text_of(elem, "server") or None,
        database=text_of(elem, "database") or None,
        port=text_of(elem, "port") or None,
    )
    return suggest_airflow_connection(raw)
