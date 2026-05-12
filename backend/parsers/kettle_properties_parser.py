"""
Parser for PDI's kettle.properties file (typically at ~/.kettle/kettle.properties).

The file is a standard Java .properties text file. PDI ships it pre-populated
with ~80 KETTLE_* engine-tuning variables — log table names, retry counts,
encoding flags, etc. None of those are useful for migration to Airflow.

What IS useful are user-defined variables that PDI users add by hand for
cross-job reuse: database hosts, root directories, SMTP servers, environment
markers (DEV/UAT/PROD). These are the values referenced from .kjb/.ktr files
as ${VAR}.

This parser splits the two cleanly:
  - engine_settings: every KETTLE_* / PENTAHO_* / SHIM_* / etc. line
  - user_variables: everything else with a non-empty value

Only user_variables flow into the IR; engine_settings are returned for
reporting purposes only.
"""

from __future__ import annotations

from dataclasses import dataclass


# Variable name prefixes that indicate a built-in PDI engine setting.
# Anything matching these is considered noise for migration purposes.
ENGINE_PREFIXES: tuple[str, ...] = (
    "KETTLE_",
    "PENTAHO_",
    "SHIM_",
    "SHARED_",
    "SHELL_STEP_",
)

# Specific engine setting names that don't share a common prefix.
ENGINE_EXACT: frozenset[str] = frozenset({
    "s3.vfs.partSize",
    "s3.vfs.useTempFileOnUploadData",
    "vfs.sftp.userDirIsRoot",
})


@dataclass
class KettlePropertiesResult:
    """Output of parsing a kettle.properties file."""

    user_variables: dict[str, str]   # name -> value, for user-defined variables only
    engine_settings: dict[str, str]  # KETTLE_* / etc., for the migration report
    raw_count: int                   # total non-comment, non-empty lines parsed


def parse_kettle_properties_bytes(content: bytes) -> KettlePropertiesResult:
    """Parse a kettle.properties file's contents."""
    # PDI properties files are usually ISO-8859-1 (Java default) but we accept UTF-8 too.
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("iso-8859-1", errors="replace")

    user_vars: dict[str, str] = {}
    engine: dict[str, str] = {}
    raw_count = 0

    for line in text.splitlines():
        stripped = line.strip()
        # Skip blank lines and comments. .properties supports both # and ! as comment chars.
        if not stripped or stripped.startswith("#") or stripped.startswith("!"):
            continue
        # .properties allows = or : as the separator; the first one wins.
        eq_pos = stripped.find("=")
        colon_pos = stripped.find(":")
        if eq_pos == -1 and colon_pos == -1:
            continue
        if eq_pos == -1:
            sep = colon_pos
        elif colon_pos == -1:
            sep = eq_pos
        else:
            sep = min(eq_pos, colon_pos)
        key = stripped[:sep].strip()
        value = stripped[sep + 1:].strip()
        raw_count += 1

        if _is_engine_setting(key):
            # Keep engine settings even if empty — useful in the report
            engine[key] = value
        else:
            # Only register user variables that actually have a value
            if value:
                user_vars[key] = value

    return KettlePropertiesResult(
        user_variables=user_vars,
        engine_settings=engine,
        raw_count=raw_count,
    )


def _is_engine_setting(key: str) -> bool:
    """True if `key` is a built-in PDI engine setting rather than a user variable."""
    if key in ENGINE_EXACT:
        return True
    for prefix in ENGINE_PREFIXES:
        if key.startswith(prefix):
            return True
    return False
