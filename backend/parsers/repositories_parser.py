"""
Parser for PDI's repositories.xml (typically at ~/.kettle/repositories.xml).

The file lists every PDI repository the developer has configured:
  * KettleFileRepository — a base_directory on the local filesystem
  * KettleDatabaseRepository — a connection to a PG/MSSQL/etc. server that
    stores .kjb/.ktr content as rows in tables (the so-called "Enterprise
    Repository" pattern)

For migration purposes the file gives us two important things:

1. base_directory mappings: when a .kjb references another job/transformation
   via ${Internal.Job.Repository.Directory}/foo.ktr, we now know what that
   directory actually is on the developer's machine. Cross-references that
   would otherwise fail-loud as "missing file" can be resolved.

2. Repository names as data-domain hints: names like 'etl_daily',
   'sync_pipeline', 'warehouse_load' identify the project area. These flow
   into the generated DAG as tags and as a dag_id prefix so migrated DAGs
   cluster naturally in the Airflow UI.

NOTE: The shipped repositories.xml has slightly malformed indentation in
its <attribute> tags (nested <attribute> inside <attribute> rather than
<value>); we read defensively to handle this.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

from ..models.ir import ConnectionRef
from .connection_mapper import suggest_airflow_connection
from .xml_util import parse_xml_bytes, text_of


@dataclass
class FileRepository:
    """A PDI file-based repository — maps a name to a filesystem root."""

    name: str
    base_directory: str
    description: str = ""
    is_default: bool = False

    def normalized_path(self) -> str:
        """Return the base directory with backslashes -> forward slashes."""
        return self.base_directory.replace("\\", "/")


@dataclass
class DatabaseRepository:
    """A PDI database-based repository — references a connection by name."""

    name: str
    connection_name: str
    description: str = ""


@dataclass
class RepositoriesResult:
    """Output of parsing a repositories.xml file."""

    file_repositories: list[FileRepository] = field(default_factory=list)
    database_repositories: list[DatabaseRepository] = field(default_factory=list)
    repo_connections: list[ConnectionRef] = field(default_factory=list)

    def find_repo_for_path(self, path: str) -> FileRepository | None:
        """
        Given a path like 'E:/my_project/my_pipeline/load_x.ktr', return the
        FileRepository whose base_directory is the longest matching prefix.
        Used by the orchestrator to resolve cross-references.
        """
        norm = path.replace("\\", "/")
        best: FileRepository | None = None
        best_len = -1
        for repo in self.file_repositories:
            base = repo.normalized_path().rstrip("/")
            if norm.lower().startswith(base.lower() + "/") or norm.lower() == base.lower():
                if len(base) > best_len:
                    best = repo
                    best_len = len(base)
        return best

    def find_repo_by_name(self, name: str) -> FileRepository | None:
        for repo in self.file_repositories:
            if repo.name == name:
                return repo
        return None


def parse_repositories_xml_bytes(content: bytes) -> RepositoriesResult:
    """Parse repositories.xml bytes into a RepositoriesResult."""
    root = parse_xml_bytes(content)

    result = RepositoriesResult()

    # <connection> elements at the top level — these are the connections used
    # by KettleDatabaseRepository entries below. Same shape as a .kdb file.
    for conn_elem in root.findall("connection"):
        raw = ConnectionRef(
            pdi_name=text_of(conn_elem, "name") or "unnamed",
            db_type=text_of(conn_elem, "type") or "UNKNOWN",
            host=text_of(conn_elem, "server") or None,
            database=text_of(conn_elem, "database") or None,
            port=text_of(conn_elem, "port") or None,
        )
        result.repo_connections.append(suggest_airflow_connection(raw))

    # <repository> elements — file-based and database-based both share this tag.
    for repo_elem in root.findall("repository"):
        repo_id = text_of(repo_elem, "id")
        name = text_of(repo_elem, "name") or "unnamed"
        description = text_of(repo_elem, "description")
        is_default = text_of(repo_elem, "is_default", "false").lower() == "true"

        if repo_id == "KettleFileRepository":
            base_dir = text_of(repo_elem, "base_directory")
            if base_dir:
                result.file_repositories.append(FileRepository(
                    name=name,
                    base_directory=base_dir,
                    description=description,
                    is_default=is_default,
                ))
        elif repo_id == "KettleDatabaseRepository":
            conn_name = text_of(repo_elem, "connection")
            if conn_name:
                result.database_repositories.append(DatabaseRepository(
                    name=name,
                    connection_name=conn_name,
                    description=description,
                ))
        # Ignore unknown repository types silently — PDI has plugin-based
        # repository implementations we don't claim to understand.

    return result
