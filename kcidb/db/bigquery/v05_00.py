"""Kernel CI report database - BigQuery schema v5.0"""

import logging
from google.cloud.bigquery.schema import SchemaField as Field
from google.cloud.bigquery.table import Table
import kcidb.io as io
from kcidb.misc import merge_dicts
from .v04_04 import Schema as PreviousSchema

# Module's logger
LOGGER = logging.getLogger(__name__)


# The "status" column to be added to builds
BUILDS_STATUS_FIELD = Field(
    "status", "STRING",
    description="The build status, one of the following. "
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
                "\"FAIL\" - the build completed and reported the "
                "code being built as faulty, "
                "\"ERROR\" - the build didn't complete due to a "
                "toolchain failure, and the status of the "
                "code being built is unknown."
                "\"MISS\" - the build didn't run due to a "
                "failure in the build harness, and the status "
                "of both the toolchain and the code being built "
                "is unknown."
)


class Schema(PreviousSchema):
    """BigQuery database schema v5.0"""

    # The schema's version.
    version = (5, 0)
    # The I/O schema the database schema supports
    io = io.schema.V5_0

    # A map of table names to their BigQuery schemas
    TABLE_MAP = merge_dicts(
        PreviousSchema.TABLE_MAP,
        checkouts=list(filter(
            # Remove "contacts" column
            lambda f: f.name != "contacts",
            PreviousSchema.TABLE_MAP["checkouts"]
        )),
        builds=list(filter(
            # Remove "valid" column
            lambda f: f.name != "valid",
            PreviousSchema.TABLE_MAP["builds"]
        )) + [
            # Add "status" column
            BUILDS_STATUS_FIELD,
        ],
        tests=list(filter(
            # Remove "waived" column
            lambda f: f.name != "waived",
            PreviousSchema.TABLE_MAP["tests"]
        )),
        issues=list(filter(
            # Remove "build_valid" and "test_status" columns
            lambda f: f.name not in ("build_valid", "test_status"),
            PreviousSchema.TABLE_MAP["issues"]
        )),
    )

    # Queries for each type of raw object-oriented data
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        revision=merge_dicts(
            PreviousSchema.OO_QUERIES["revision"],
            statement="SELECT\n"
                      "    git_commit_hash,\n"
                      "    patchset_hash,\n"
                      "    ANY_VALUE(patchset_files) AS patchset_files,\n"
                      "    ANY_VALUE(git_commit_name) AS git_commit_name\n"
                      "FROM checkouts\n"
                      "GROUP BY git_commit_hash, patchset_hash",
        ),
        build=merge_dicts(
            PreviousSchema.OO_QUERIES["build"],
            statement="SELECT\n"
                      "    id,\n"
                      "    checkout_id,\n"
                      "    origin,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    architecture,\n"
                      "    command,\n"
                      "    compiler,\n"
                      "    input_files,\n"
                      "    output_files,\n"
                      "    config_name,\n"
                      "    config_url,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    comment,\n"
                      "    status,\n"
                      "    misc\n"
                      "FROM builds",
        ),
        test=merge_dicts(
            PreviousSchema.OO_QUERIES["test"],
            statement="SELECT\n"
                      "    id,\n"
                      "    build_id,\n"
                      "    origin,\n"
                      "    path,\n"
                      "    environment.comment AS environment_comment,\n"
                      "    environment.compatible AS "
                      "environment_compatible,\n"
                      "    environment.misc AS environment_misc,\n"
                      "    status,\n"
                      "    number.value AS number_value,\n"
                      "    number.unit AS number_unit,\n"
                      "    number.prefix AS number_prefix,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    output_files,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM tests",
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

        # Migrate the builds' "valid" to "status"

        # Add the "status" field to the "_builds" table.
        # Do not remove the "valid" column yet
        builds_table = Table(conn.dataset_ref.table("_builds"))
        builds_table.schema = \
            PreviousSchema.TABLE_MAP["builds"] + [BUILDS_STATUS_FIELD]
        conn.client.update_table(builds_table, ["schema"])
        # Fill it in with the "valid" data
        conn.query_create("""
            UPDATE _builds SET
                status = CASE valid
                    WHEN TRUE THEN 'PASS'
                    WHEN FALSE THEN 'FAIL'
                    ELSE NULL
                END
            WHERE valid IS NOT NULL
        """).result()

        # Migrate "waived" to incidents of an issue
        waived_issue_origin = '_'
        waived_issue_id = f'{waived_issue_origin}:waived'
        waived_issue_version = '1'
        query = conn.query_create(f"""
            INSERT INTO _incidents (
                id,
                origin,
                issue_id,
                issue_version,
                test_id,
                present
            )
            SELECT
                '{waived_issue_id}:{waived_issue_version}:' || id AS id,
                '{waived_issue_origin}' AS origin,
                '{waived_issue_id}' AS issue_id,
                {waived_issue_version} AS issue_version,
                id AS test_id,
                TRUE AS present
            FROM tests
            WHERE waived
        """)
        query.result()
        if query.num_dml_affected_rows != 0:
            conn.query_create(f"""
                INSERT INTO _issues (id, version, origin, comment)
                VALUES (
                    '{waived_issue_id}',
                    {waived_issue_version},
                    '{waived_issue_origin}',
                    'Test waived as unreliable'
                )
            """).result()

        # Drop removed columns for all tables
        for table_name, table_schema in cls.TABLE_MAP.items():
            dropped_columns = {
                f.name for f in PreviousSchema.TABLE_MAP.get(table_name, [])
            } - {
                f.name for f in table_schema
            }
            if not dropped_columns:
                continue
            # Update the schema for the view to stop using dropped columns
            view = Table(conn.dataset_ref.table(table_name))
            view.view_query = cls._format_view_query(conn, table_name)
            conn.client.update_table(view, ["view_query"])
            # For each column to drop
            for dropped_column in dropped_columns:
                conn.query_create(f"""
                    ALTER TABLE _{table_name} DROP COLUMN {dropped_column}
                """).result()
