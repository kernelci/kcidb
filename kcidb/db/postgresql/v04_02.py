"""Kernel CI report database - PostgreSQL schema v4.2"""

import datetime
import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.postgresql.schema import \
    TimestampColumn, Table
from .v04_01 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)

# The new _timestamp column added to every table in this schema
TIMESTAMP_COLUMN = TimestampColumn(
    conflict_func="GREATEST",
    metadata_expr="CURRENT_TIMESTAMP"
)


class Schema(PreviousSchema):
    """PostgreSQL database schema v4.2"""

    # The schema's version.
    version = (4, 2)
    # The I/O schema the database schema supports
    io = io.schema.V4_3

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = {
        name: merge_dicts(
            args,
            columns=dict(_timestamp=TIMESTAMP_COLUMN, **args["columns"])
        )
        for name, args in PreviousSchema.TABLES_ARGS.items()
    }

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
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
            # For all tables
            for name, schema in cls.TABLES.items():
                # Get the _timestamp table column
                column = schema.columns["_timestamp"]
                # Add the _timestamp column
                cursor.execute(f"""
                    ALTER TABLE {name} ADD COLUMN {column.format_def()}
                """)
                # Set missing _timestamps to start_time, or current time
                expr = column.schema.metadata_expr
                if "start_time" in schema.columns:
                    expr = f"COALESCE(start_time, {expr})"
                cursor.execute(f"""
                    UPDATE {name}
                    SET {column.name} = {expr}
                    WHERE {column.name} IS NULL
                """)

    def purge(self, before):
        """
        Remove all the data from the database that arrived before the
        specified time, if the database supports that.

        Args:
            before: An "aware" datetime.datetime object specifying the
                    earliest (database server) time the data to be *preserved*
                    should've arrived. Any other data will be purged.
                    Can be None to have nothing removed. The latter can be
                    used to test if the database supports purging.

        Returns:
            True if the database supports purging, and the requested data was
            purged. False if the database doesn't support purging.
        """
        assert before is None or \
            isinstance(before, datetime.datetime) and before.tzinfo
        if before is not None:
            before_json = before.isoformat(timespec='microseconds')
            with self.conn, self.conn.cursor() as cursor:
                for name, schema in self.TABLES.items():
                    # Get the _timestamp table column
                    column = schema.columns["_timestamp"]
                    # Purge
                    cursor.execute(
                        f"""
                            DELETE FROM {name}
                            WHERE {column.name} < {schema.placeholder}
                        """,
                        (column.schema.pack(before_json),)
                    )
        return True
