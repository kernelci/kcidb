"""Kernel CI report database - PostgreSQL schema v4.3"""

from kcidb.db.postgresql.schema import Index
from .v04_02 import Schema as PreviousSchema


class Schema(PreviousSchema):
    """PostgreSQL database schema v4.3"""

    # The schema's version.
    version = (4, 3)

    # A map of index names and schemas
    INDEXES = {
        f"{table}_{column}": Index(table, [column])
        for table in PreviousSchema.TABLES
        for column in ("_timestamp",)
    }

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
