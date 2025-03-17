"""Kernel CI report database - SQLite schema v5.1"""

import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.sqlite.schema import TimestampColumn, Table
from .v05_00 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


# Don't be so narrow-minded, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """SQLite database schema v5.1"""

    # The schema's version.
    version = (5, 1)
    # The I/O schema the database schema supports
    io = io.schema.V5_1

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        # Checkouts
        checkouts=merge_dicts(
            PreviousSchema.TABLES_ARGS["checkouts"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["checkouts"]["columns"],
                {
                    "origin_builds_finish_time": TimestampColumn(),
                    "origin_tests_finish_time": TimestampColumn(),
                }
            )
        ),
    )

    # A map of table names and schemas
    TABLES = {name: Table(**args) for name, args in TABLES_ARGS.items()}

    @classmethod
    def _inherit(cls, conn):
        """
        Inerit the database data from the previous schema version (if any).

        Args:
            conn:   Connection to the database to inherit. The database must
                    comply with the previous version of the schema.
        """
        assert isinstance(conn, cls.Connection)
        with conn:
            cursor = conn.cursor()
            try:
                # Add new columns
                # For all tables
                for name, schema in cls.TABLES.items():
                    if name not in PreviousSchema.TABLES:
                        continue
                    # For each added column
                    for column_name in sorted(
                        set(cls.TABLES_ARGS[name]["columns"]) -
                        set(PreviousSchema.TABLES_ARGS[name]["columns"])
                    ):
                        cursor.execute(f"""
                            ALTER TABLE {name} ADD COLUMN
                            {schema.columns[column_name].format_def()}
                        """)
            finally:
                cursor.close()
