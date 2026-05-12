"""Small helpers shared across PDI XML parsers."""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET


def text_of(elem: ET.Element | None, path: str, default: str = "") -> str:
    """Get .text of a child element, or default if missing/empty."""
    if elem is None:
        return default
    found = elem.find(path)
    if found is None or found.text is None:
        return default
    return found.text.strip()


def all_text(elem: ET.Element | None, path: str) -> list[str]:
    """Get .text of all matching child elements."""
    if elem is None:
        return []
    return [e.text.strip() for e in elem.findall(path) if e.text and e.text.strip()]


VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


# Variables PDI itself provides at runtime — these are always "defined" from the
# user's perspective. We filter them out so they don't appear as missing references.
PDI_BUILTIN_VARIABLES: frozenset[str] = frozenset({
    "Internal.Job.Filename.Directory",
    "Internal.Job.Filename.Name",
    "Internal.Job.Name",
    "Internal.Transformation.Filename.Directory",
    "Internal.Transformation.Filename.Name",
    "Internal.Transformation.Name",
    "Internal.Entry.Current.Directory",
    "Internal.Step.Name",
    "Internal.Step.CopyNr",
    "user.dir",
    "user.home",
    "java.io.tmpdir",
})

# Variables that the *generated* DAG provides via Jinja or Airflow Variables.
# These are likewise not blocking. The generator wires them up automatically.
DAG_PROVIDED_VARIABLES: frozenset[str] = frozenset({
    "run_date",        # set by the run_date Jinja template at the top of every DAG
    "xcom_limit_size", # read from Airflow Variable in the DAG template
})

IGNORED_VARIABLES: frozenset[str] = PDI_BUILTIN_VARIABLES | DAG_PROVIDED_VARIABLES


def find_pdi_variables(text: str | None) -> list[str]:
    """
    Extract user-defined ${var.name} references from any string.
    PDI built-ins and DAG-provided variables are filtered out so they don't
    appear as missing references in the migration report.
    """
    if not text:
        return []
    return [v for v in VAR_PATTERN.findall(text) if v not in IGNORED_VARIABLES]


def parse_xml_bytes(content: bytes) -> ET.Element:
    """
    Parse XML bytes -> root Element. PDI files can be UTF-8 or CP874 (Thai).
    We try UTF-8 first, then fall back to CP874.
    """
    try:
        return ET.fromstring(content.decode("utf-8"))
    except UnicodeDecodeError:
        return ET.fromstring(content.decode("cp874", errors="replace"))
    except ET.ParseError:
        # Some PDI files have a BOM or stray bytes — try a more forgiving decode
        return ET.fromstring(content.decode("utf-8", errors="replace"))
