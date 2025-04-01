"""Kernel CI report database - SQLite schema v4.3"""

import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.sqlite.schema import \
    Column, TextColumn, JSONColumn, Table
from .v04_02 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """SQLite database schema v4.3"""

    # The schema's version.
    version = (4, 3)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # A map of table names and Table constructor arguments
    # For use by descendants
    # Add environment.compatible, and number.* fields
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        tests=merge_dicts(
            PreviousSchema.TABLES_ARGS["tests"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["tests"]["columns"],
                {
                    "environment.compatible": JSONColumn(),
                    "number.value": Column("REAL"),
                    "number.unit": TextColumn(),
                    "number.prefix": TextColumn()
                }
            )
        )
    )

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
    }

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        test=merge_dicts(
            PreviousSchema.OO_QUERIES["test"],
            statement="SELECT\n"
                      "    id,\n"
                      "    build_id,\n"
                      "    origin,\n"
                      "    path,\n"
                      "    \"environment.comment\" AS environment_comment,\n"
                      "    \"environment.compatible\" AS "
                      "environment_compatible,\n"
                      "    \"environment.misc\" AS environment_misc,\n"
                      "    log_url,\n"
                      "    status,\n"
                      "    \"number.value\" AS number_value,\n"
                      "    \"number.unit\" AS number_unit,\n"
                      "    \"number.prefix\" AS number_prefix,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    output_files,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM tests",
        ),
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
        with conn:
            cursor = conn.cursor()
            try:
                # For all tables
                for name, schema in cls.TABLES.items():
                    if name not in PreviousSchema.TABLES:
                        continue
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
