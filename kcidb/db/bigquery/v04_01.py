"""Kernel CI report database - BigQuery schema v4.1"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v04_00 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


class Schema(PreviousSchema):
    """BigQuery database schema v4.1"""

    # The schema's version.
    version = (4, 1)
    # The I/O schema the database schema supports
    io = io.schema.V4_2

    # Culprit fields
    CULPRIT_FIELDS = (
        Field(
            "code", "BOOL",
            description="The built/tested code.",
        ),
        Field(
            "tool", "BOOL",
            description="The tool - the static "
                        "analyzer, the build toolchain, "
                        "the test, etc."
        ),
        Field(
            "harness", "BOOL",
            description="The harness - the system controlling "
                        "the execution of the build/test."
        ),
    )

    # A map of table names to their BigQuery schemas
    TABLE_MAP = dict(
        **PreviousSchema.TABLE_MAP,
        issues=[
            Field(
                "id", "STRING",
                description="Issue ID",
            ),
            Field(
                "version", "INTEGER",
                description="Issue version number",
            ),
            Field(
                "origin", "STRING",
                description="The name of the CI system which submitted "
                            "the issue",
            ),
            Field(
                "report_url", "STRING",
                description="The URL of a report of the issue.",
            ),
            Field(
                "report_subject", "STRING",
                description="The subject of the issue report.",
            ),
            Field(
                "culprit", "RECORD", fields=CULPRIT_FIELDS,
                description="Layers of the execution stack responsible "
                            "for the issue."
            ),
            Field(
                "build_valid", "BOOL",
                description="Status to assign to incident builds",
            ),
            Field(
                "test_status", "STRING",
                description="Status to assign to incident tests, "
                            "one of the following. "
                            "\"ERROR\" - the test is faulty, "
                            "the status of the tested code is unknown. "
                            "\"FAIL\" - the test has failed, the tested "
                            "code is faulty. "
                            "\"PASS\" - the test has passed, the tested "
                            "code is correct. "
                            "\"DONE\" - the test has finished successfully, "
                            "the status of the tested code is unknown. "
                            "\"SKIP\" - the test wasn't executed, "
                            "the status of the tested code is unknown. "
            ),
            Field(
                "comment", "STRING",
                description="A human-readable comment regarding the issue",
            ),
            Field(
                "misc", "STRING",
                description="Miscellaneous extra data about the issue "
                            "in JSON format",
            ),
        ],
        incidents=[
            Field(
                "id", "STRING",
                description="Incident ID",
            ),
            Field(
                "origin", "STRING",
                description="The name of the CI system which submitted "
                            "the incident",
            ),
            Field(
                "issue_id", "STRING",
                description="ID of the incident issue",
            ),
            Field(
                "issue_version", "INTEGER",
                description="Version number of the incident issue",
            ),
            Field(
                "build_id", "STRING",
                description="ID of the build with the incident",
            ),
            Field(
                "test_id", "STRING",
                description="ID of the test with the incident",
            ),
            Field(
                "present", "BOOL",
                description="True if the issue occurred in the linked "
                            "objects. False if it was absent."
            ),
            Field(
                "comment", "STRING",
                description="A human-readable comment regarding the issue",
            ),
            Field(
                "misc", "STRING",
                description="Miscellaneous extra data about the issue "
                            "in JSON format",
            ),
        ],
    )

    # A map of table names and their "primary key" fields
    KEYS_MAP = dict(
        **PreviousSchema.KEYS_MAP,
        issues=("id", "version",),
        incidents=("id",),
    )

    # Queries for each type of raw object-oriented data
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        issue=merge_dicts(
            PreviousSchema.OO_QUERIES["issue"],
            statement="SELECT\n"
                      "    id,\n"
                      "    ANY_VALUE(origin) AS origin\n"
                      "FROM issues\n"
                      "GROUP BY id",
        ),
        issue_version=merge_dicts(
            PreviousSchema.OO_QUERIES["issue_version"],
            statement="SELECT\n"
                      "    id,\n"
                      "    version AS version_num,\n"
                      "    origin,\n"
                      "    report_url,\n"
                      "    report_subject,\n"
                      "    culprit.code AS culprit_code,\n"
                      "    culprit.tool AS culprit_tool,\n"
                      "    culprit.harness AS culprit_harness,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM issues",
        ),
        incident=merge_dicts(
            PreviousSchema.OO_QUERIES["incident"],
            statement="SELECT\n"
                      "    id,\n"
                      "    origin,\n"
                      "    issue_id,\n"
                      "    issue_version AS issue_version_num,\n"
                      "    build_id,\n"
                      "    test_id,\n"
                      "    present,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM incidents",
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
        # Create new tables
        for table_name in cls.TABLE_MAP:
            if table_name not in PreviousSchema.TABLE_MAP:
                cls._create_table(conn, table_name)
