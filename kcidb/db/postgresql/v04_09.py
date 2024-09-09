"""Kernel CI report database - PostgreSQL schema v4.9"""

import textwrap
from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import Index, View
from .v04_08 import Schema as PreviousSchema


CREATE_UNIT_PREFIX_TYPE_STATEMENT = textwrap.dedent("""\
    DO $$ BEGIN
        CREATE TYPE UNIT_PREFIX AS ENUM ('metric', 'binary');
    EXCEPTION
        WHEN duplicate_object THEN null;
    END $$
""")


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v4.9"""

    # The schema's version.
    version = (4, 9)
    # The I/O schema the database schema supports
    io = io.schema.V4_4

    # A map of view names and schemas
    VIEWS = merge_dicts(PreviousSchema.VIEWS, dict(
        current_bugs=View(textwrap.dedent("""\
            SELECT
                MAX(_timestamp) AS _timestamp,
                report_url AS url,
                FIRST(report_subject) AS subject,
                BOOL_OR(culprit_code) AS culprit_code,
                BOOL_OR(culprit_tool) AS culprit_tool,
                BOOL_OR(culprit_harness) AS culprit_harness
            FROM (
                SELECT DISTINCT ON (id)
                    _timestamp,
                    report_url,
                    report_subject,
                    culprit_code,
                    culprit_tool,
                    culprit_harness
                FROM issues
                ORDER BY id, version DESC
            ) AS current_issues
            GROUP BY report_url
        """), refresh_period=5),
        current_issues=View(textwrap.dedent("""\
            SELECT DISTINCT ON (id)
                _timestamp,
                id,
                version,
                origin,
                report_url,
                report_subject,
                culprit_code,
                culprit_tool,
                culprit_harness,
                build_valid,
                test_status,
                comment,
                misc
            FROM issues
            ORDER BY id, version DESC
        """), refresh_period=5),
        current_incidents=View(textwrap.dedent("""\
            SELECT
                _timestamp,
                id,
                origin,
                issue_id,
                issue_version,
                build_id,
                test_id,
                present,
                comment,
                misc
            FROM (
                SELECT
                    _timestamp,
                    id,
                    origin,
                    issue_id,
                    issue_version,
                    build_id,
                    test_id,
                    present,
                    comment,
                    misc,
                    RANK() OVER (
                        PARTITION BY
                            issue_id,
                            build_id,
                            test_id
                        ORDER BY issue_version DESC
                    ) AS precedence
                FROM incidents
                WHERE
                    present IS NOT NULL AND
                    EXISTS (
                        SELECT TRUE
                        FROM issues
                        WHERE
                            incidents.issue_id = issues.id AND
                            incidents.issue_version = issues.version
                    )
            ) AS prioritized_known_incidents
            WHERE prioritized_known_incidents.precedence = 1
        """), refresh_period=5),
    ))

    # A map of index names and schemas
    INDEXES = merge_dicts(PreviousSchema.INDEXES, {
        f"{table}_{column.rstrip('!')}":
            Index(table, [column.rstrip('!')], unique=column.endswith('!'))
        for table, columns in dict(
            current_bugs="""
                _timestamp
                url!
                culprit_code
                culprit_tool
                culprit_harness
            """,
            current_issues="""
                _timestamp
                id!
                origin
                report_url
                culprit_code
                culprit_tool
                culprit_harness
            """,
            current_incidents="""
                _timestamp
                id!
                issue_id
                origin
                present
                build_id
                test_id
            """
        ).items()
        for column in columns.split()
    })

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        bug=merge_dicts(
            PreviousSchema.OO_QUERIES["bug"],
            statement="SELECT\n"
                      "    url,\n"
                      "    subject,\n"
                      "    culprit_code,\n"
                      "    culprit_tool,\n"
                      "    culprit_harness\n"
                      "FROM current_bugs",
        ),
        issue=merge_dicts(
            PreviousSchema.OO_QUERIES["issue"],
            statement="SELECT\n"
                      "    id,\n"
                      "    version,\n"
                      "    origin,\n"
                      "    report_url,\n"
                      "    report_subject,\n"
                      "    culprit_code,\n"
                      "    culprit_tool,\n"
                      "    culprit_harness,\n"
                      "    build_valid,\n"
                      "    test_status,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM current_issues",
        ),
        incident=merge_dicts(
            PreviousSchema.OO_QUERIES["incident"],
            statement="SELECT\n"
                      "    id,\n"
                      "    origin,\n"
                      "    issue_id,\n"
                      "    issue_version,\n"
                      "    build_id,\n"
                      "    test_id,\n"
                      "    present,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM current_incidents",
        ),
    )

    def init(self):
        """
        Initialize the database.
        The database must be uninitialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(textwrap.dedent("""\
                CREATE EXTENSION IF NOT EXISTS dblink
            """))
        super().init()

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
            for view_name, view_schema in cls.VIEWS.items():
                if view_name not in PreviousSchema.VIEWS:
                    try:
                        cursor.execute(view_schema.format_create(view_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating view {view_name!r}"
                        ) from exc
            for index_name, index_schema in cls.INDEXES.items():
                if index_name not in PreviousSchema.INDEXES:
                    try:
                        cursor.execute(index_schema.format_create(index_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating index {index_name!r}"
                        ) from exc
            for view_name, view_schema in cls.VIEWS.items():
                if view_name not in PreviousSchema.VIEWS:
                    try:
                        for command in view_schema.format_setup(view_name):
                            print(command)
                            cursor.execute(command)
                    except Exception as exc:
                        raise Exception(
                            f"Failed setting up view {view_name!r}"
                        ) from exc
