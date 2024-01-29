"""Kernel CI report database - PostgreSQL schema v4.4"""

from kcidb.db.postgresql.schema import Index
from .v04_03 import Schema as PreviousSchema


class Schema(PreviousSchema):
    """PostgreSQL database schema v4.4"""

    # The schema's version.
    version = (4, 4)

    # A map of index names and schemas
    INDEXES = dict(
        checkouts_git_commit_hash_patchset_hash=Index(
            "checkouts", ["git_commit_hash", "patchset_hash"]
        ),
        builds_checkout_id=Index("builds", ["checkout_id"]),
        tests_build_id=Index("tests", ["build_id"]),
        incidents_build_id=Index("incidents", ["build_id"]),
        incidents_test_id=Index("incidents", ["test_id"]),
        incidents_issue_id=Index("incidents", ["issue_id"]),
    )

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
