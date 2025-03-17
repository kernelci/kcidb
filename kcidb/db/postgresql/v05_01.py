"""Kernel CI report database - PostgreSQL schema v5.1"""

from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import \
    Table, Index, TimestampColumn
from .v05_00 import Schema as PreviousSchema


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v5.1"""

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

    # A map of index names and schemas
    INDEXES = merge_dicts(
        PreviousSchema.INDEXES,
        checkouts_origin_builds_finish_time=Index(
            "checkouts", ["origin_builds_finish_time"]
        ),
        checkouts_origin_tests_finish_time=Index(
            "checkouts", ["origin_tests_finish_time"]
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
        with conn, conn.cursor() as cursor:
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

            # Create new indexes
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
