"""Kernel CI report database - PostgreSQL schema v5.3"""

from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import \
    Table, JSONColumn
from .v05_02 import Schema as PreviousSchema


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v5.3"""

    # The schema's version.
    version = (5, 3)
    # The I/O schema the database schema supports
    io = io.schema.V5_3

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        tests=merge_dicts(
            PreviousSchema.TABLES_ARGS["tests"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["tests"]["columns"],
                input_files=JSONColumn(),
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
