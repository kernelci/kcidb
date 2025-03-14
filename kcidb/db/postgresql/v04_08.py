"""Kernel CI report database - PostgreSQL schema v4.8"""

import textwrap
from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import \
    Column, FloatColumn, TextColumn, TextArrayColumn, \
    Table, Index
from .v04_07 import Schema as PreviousSchema


CREATE_UNIT_PREFIX_TYPE_STATEMENT = textwrap.dedent("""\
    DO $$ BEGIN
        CREATE TYPE UNIT_PREFIX AS ENUM ('metric', 'binary');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$
""")


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v4.8"""

    # The schema's version.
    version = (4, 8)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # A map of table names and Table constructor arguments
    # For use by descendants
    # Add environment_compatible, and number_* fields
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        tests=merge_dicts(
            PreviousSchema.TABLES_ARGS["tests"],
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["tests"]["columns"],
                {
                    "environment.compatible": TextArrayColumn(),
                    "number.value": FloatColumn(),
                    "number.unit": TextColumn(),
                    "number.prefix": Column("UNIT_PREFIX")
                }
            )
        )
    )

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
    }

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, dict(
        tests_environment_compatible=Index("tests",
                                           ["environment_compatible"],
                                           method="GIN"),
        tests_number_value=Index("tests", ["number_value"]),
        tests_number_unit=Index("tests", ["number_unit"]),
    ))

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
                      "    environment_comment,\n"
                      "    environment_compatible,\n"
                      "    environment_misc,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    status,\n"
                      "    number_value,\n"
                      "    number_unit,\n"
                      "    number_prefix,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    output_files,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM tests",
        ),
    )

    def init(self):
        """
        Initialize the database.
        The database must be uninitialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(CREATE_UNIT_PREFIX_TYPE_STATEMENT)
        super().init()

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """
        super().cleanup()
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute("DROP TYPE IF EXISTS UNIT_PREFIX")

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
            cursor.execute(CREATE_UNIT_PREFIX_TYPE_STATEMENT)
            # For all tables
            for name, schema in cls.TABLES.items():
                if name not in PreviousSchema.TABLES:
                    continue
                new_column_names = \
                    set(cls.TABLES_ARGS[name]["columns"]) - \
                    set(PreviousSchema.TABLES_ARGS[name]["columns"])
                if not new_column_names:
                    continue
                cursor.execute(
                    f"ALTER TABLE {name}" + ",".join(
                        " ADD COLUMN " + schema.columns[n].format_def()
                        for n in new_column_names
                    )
                )

            # For all indexes
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
