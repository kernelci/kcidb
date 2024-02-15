"""Kernel CI report database - PostgreSQL schema v4.5"""

import textwrap
from kcidb.misc import merge_dicts
from kcidb.db.postgresql.schema import Table, Column, Index
from .v04_04 import Schema as PreviousSchema


# Source: https://stackoverflow.com/a/60260190/1161045
CREATE_ENCODE_URI_COMPONENT_STATEMENT = textwrap.dedent("""\
    CREATE OR REPLACE FUNCTION encode_uri_component(TEXT)
    RETURNS TEXT AS $$
        SELECT string_agg(
            CASE
                WHEN len_bytes > 1 OR char !~ '[0-9a-zA-Z_.!~*''()-]+' THEN
                    regexp_replace(
                        encode(convert_to(char, 'utf-8')::bytea, 'hex'),
                        '(..)',
                        E'%\\\\1',
                        'g'
                    )
                else
                    char
            end,
            ''
        )
        FROM (
            SELECT char, octet_length(char) AS len_bytes
            FROM regexp_split_to_table($1, '') char
        ) AS chars;
    $$ LANGUAGE SQL IMMUTABLE STRICT;
""")

CREATE_STATUS_TYPE_STATEMENT = textwrap.dedent("""\
    DO $$ BEGIN
        CREATE TYPE STATUS AS ENUM (
            'FAIL', 'ERROR', 'MISS', 'PASS', 'DONE', 'SKIP'
        );
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$
""")


class Schema(PreviousSchema):
    """PostgreSQL database schema v4.5"""

    # The schema's version.
    version = (4, 5)

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        tests=dict(
            columns=merge_dicts(
                PreviousSchema.TABLES_ARGS["tests"]["columns"],
                status=Column("STATUS"),
            )
        ),
    )

    # A map of table names and schemas
    TABLES = {
        name: Table(**args) for name, args in TABLES_ARGS.items()
    }

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, dict(
        checkouts_origin=Index("checkouts", ["origin"]),
        checkouts_start_time=Index("checkouts", ["start_time"]),
        checkouts_git_repository_url=Index(
            "checkouts", ["git_repository_url"]
        ),
        checkouts_git_repository_branch=Index(
            "checkouts", ["git_repository_branch"]
        ),
        builds_origin=Index("builds", ["origin"]),
        builds_start_time=Index("builds", ["start_time"]),
        builds_architecture=Index("builds", ["architecture"]),
        builds_config_name=Index("builds", ["config_name"]),
        tests_origin=Index("tests", ["origin"]),
        tests_start_time=Index("tests", ["start_time"]),
        tests_path=Index("tests", ["path"]),
        tests_status=Index("tests", ["status"]),
        issues_origin=Index("issues", ["origin"]),
        incidents_origin=Index("incidents", ["origin"]),
    ))

    def init(self):
        """
        Initialize the database.
        The database must be uninitialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(CREATE_STATUS_TYPE_STATEMENT)
            cursor.execute(CREATE_ENCODE_URI_COMPONENT_STATEMENT)
        super().init()

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """
        super().cleanup()
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                "DROP FUNCTION IF EXISTS encode_uri_component(text)"
            )
            cursor.execute(
                "DROP TYPE IF EXISTS STATUS"
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
            cursor.execute(CREATE_STATUS_TYPE_STATEMENT)
            cursor.execute(CREATE_ENCODE_URI_COMPONENT_STATEMENT)
            cursor.execute(textwrap.dedent("""\
                ALTER TABLE tests
                ALTER COLUMN status TYPE STATUS
                USING status::STATUS
            """))
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
