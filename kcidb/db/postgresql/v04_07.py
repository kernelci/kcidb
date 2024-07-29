"""Kernel CI report database - PostgreSQL schema v4.7"""

from kcidb.misc import merge_dicts
from kcidb.db.postgresql.schema import Index
from .v04_06 import Schema as PreviousSchema


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v4.7"""

    # The schema's version.
    version = (4, 7)

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, dict(
        checkouts_tree_name=Index("checkouts", ["tree_name"]),
        issues_report_url=Index("issues", ["report_url"]),
        issues_culprit_code=Index("issues", ["culprit_code"]),
        issues_culprit_tool=Index("issues", ["culprit_tool"]),
        issues_culprit_harness=Index("issues", ["culprit_harness"]),
        incidents_issue_version=Index("incidents", ["issue_version"]),
        incidents_present=Index("incidents", ["present"]),
    ))

    @classmethod
    def _inherit(cls, conn):
        """
        Inerit the database data from the previous schema version (if any).

        Args:
            conn:   Connection to the database to inherit. The database must
                    comply with the previous version of the schema.
        """
        assert isinstance(conn, cls.Connection)
        with conn, conn.cursor() as cursor:
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
