"""Kernel CI report database - PostgreSQL schema v4.1"""

import logging
import kcidb.io as io
from kcidb.misc import merge_dicts
from kcidb.db.postgresql.schema import \
    Constraint, BoolColumn, IntegerColumn, \
    TextColumn, JSONColumn, Table
from .v04_00 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """PostgreSQL database schema v4.1"""

    # The schema's version.
    version = (4, 1)
    # The I/O schema the database schema supports
    io = io.schema.V4_1

    # A map of table names to table definitions
    TABLES = dict(
        **PreviousSchema.TABLES,
        issues=Table({
            "id": TextColumn(constraint=Constraint.NOT_NULL),
            "version": IntegerColumn(constraint=Constraint.NOT_NULL),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "report_url": TextColumn(),
            "report_subject": TextColumn(),
            "culprit.code": BoolColumn(),
            "culprit.tool": BoolColumn(),
            "culprit.harness": BoolColumn(),
            "build_valid": BoolColumn(),
            "test_status": TextColumn(),
            "comment": TextColumn(),
            "misc": JSONColumn()
        }, primary_key=["id", "version"]),
        incidents=Table({
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "issue_id": TextColumn(constraint=Constraint.NOT_NULL),
            "issue_version": IntegerColumn(constraint=Constraint.NOT_NULL),
            "build_id": TextColumn(),
            "test_id": TextColumn(),
            "present": BoolColumn(),
            "comment": TextColumn(),
            "misc": JSONColumn(),
        }),
    )

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    # NOTE: Relying on dictionaries preserving order in Python 3.6+
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        bug=merge_dicts(
            PreviousSchema.OO_QUERIES["bug"],
            statement="SELECT\n"
                      "    report_url AS url,\n"
                      "    FIRST(report_subject) AS subject,\n"
                      "    BOOL_OR(culprit_code) AS culprit_code,\n"
                      "    BOOL_OR(culprit_tool) AS culprit_tool,\n"
                      "    BOOL_OR(culprit_harness) AS culprit_harness\n"
                      "FROM (\n"
                      "    SELECT\n"
                      "        report_url,\n"
                      "        report_subject,\n"
                      "        culprit_code,\n"
                      "        culprit_tool,\n"
                      "        culprit_harness,\n"
                      "        ROW_NUMBER() OVER (\n"
                      "            PARTITION BY id\n"
                      "            ORDER BY version DESC\n"
                      "        ) AS precedence\n"
                      "    FROM issues\n"
                      ") AS prioritized_issues\n"
                      "WHERE precedence = 1\n"
                      "GROUP BY report_url",
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
                      "FROM (\n"
                      "    SELECT\n"
                      "        id,\n"
                      "        version,\n"
                      "        origin,\n"
                      "        report_url,\n"
                      "        report_subject,\n"
                      "        culprit_code,\n"
                      "        culprit_tool,\n"
                      "        culprit_harness,\n"
                      "        build_valid,\n"
                      "        test_status,\n"
                      "        comment,\n"
                      "        misc,\n"
                      "        ROW_NUMBER() OVER (\n"
                      "            PARTITION BY id\n"
                      "            ORDER BY version DESC\n"
                      "        ) AS precedence\n"
                      "    FROM issues\n"
                      ") AS prioritized_issues\n"
                      "WHERE precedence = 1",
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
                      "    comment,\n"
                      "    misc\n"
                      "FROM (\n"
                      "    SELECT\n"
                      "        id,\n"
                      "        origin,\n"
                      "        issue_id,\n"
                      "        issue_version,\n"
                      "        build_id,\n"
                      "        test_id,\n"
                      "        present,\n"
                      "        comment,\n"
                      "        misc,\n"
                      "        DENSE_RANK() OVER (\n"
                      "            PARTITION BY\n"
                      "                issue_id, build_id, test_id\n"
                      "            ORDER BY issue_version DESC\n"
                      "        ) AS precedence\n"
                      "    FROM incidents\n"
                      ") AS prioritized_incidents\n"
                      "WHERE precedence = 1 AND present",
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
            for table_name, table_schema in cls.TABLES.items():
                if table_name not in PreviousSchema.TABLES:
                    try:
                        cursor.execute(table_schema.format_create(table_name))
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating table {table_name!r}"
                        ) from exc
