"""Kernel CI report database - PostgreSQL schema v5.0"""

from kcidb.misc import merge_dicts
import kcidb.io as io
from kcidb.db.postgresql.schema import \
    Table, Index, Column
from .v04_09 import Schema as PreviousSchema


# It's OK, pylint: disable=too-many-ancestors
class Schema(PreviousSchema):
    """PostgreSQL database schema v5.0"""

    # The schema's version.
    version = (5, 0)
    # The I/O schema the database schema supports
    io = io.schema.V5_0

    # A map of table names and Table constructor arguments
    # For use by descendants
    TABLES_ARGS = merge_dicts(
        PreviousSchema.TABLES_ARGS,
        # Checkouts
        checkouts=merge_dicts(
            PreviousSchema.TABLES_ARGS["checkouts"],
            # Remove "contacts" column
            columns=dict(filter(
                lambda kv: kv[0] != "contacts",
                PreviousSchema.TABLES_ARGS["checkouts"]["columns"].items()
            ))
        ),
        # Builds
        builds=merge_dicts(
            PreviousSchema.TABLES_ARGS["builds"],
            columns=dict(
                # Remove "valid" column
                **dict(filter(
                    lambda kv: kv[0] != "valid",
                    PreviousSchema.TABLES_ARGS["builds"]["columns"].items()
                )),
                # Add "status" column
                status=Column("STATUS"),
            )
        ),
        # Tests
        tests=merge_dicts(
            PreviousSchema.TABLES_ARGS["tests"],
            # Remove "waived" column
            columns=dict(filter(
                lambda kv: kv[0] != "waived",
                PreviousSchema.TABLES_ARGS["tests"]["columns"].items()
            ))
        ),
        # Issues
        issues=merge_dicts(
            PreviousSchema.TABLES_ARGS["issues"],
            # Remove "build_valid" and "test_status" columns
            columns=dict(filter(
                lambda kv: kv[0] not in ("build_valid", "test_status"),
                PreviousSchema.TABLES_ARGS["issues"]["columns"].items()
            ))
        ),
    )

    # A map of table names and schemas
    TABLES = {name: Table(**args) for name, args in TABLES_ARGS.items()}

    # A map of index names and schemas
    INDEXES = merge_dicts(
        # Remove indexes for columns removed above
        dict(filter(lambda kv: kv[0] not in ("builds_valid", "tests_waived"),
                    PreviousSchema.INDEXES.items())),
        # Add index on build status
        builds_status=Index("builds", ["status"]),
    )

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order, and match the order in
    # kcidb.orm.data.SCHEMA
    OO_QUERIES = merge_dicts(
        PreviousSchema.OO_QUERIES,
        revision=merge_dicts(
            PreviousSchema.OO_QUERIES["revision"],
            statement="SELECT\n"
                      "    git_commit_hash,\n"
                      "    patchset_hash,\n"
                      "    FIRST(patchset_files),\n"
                      "    FIRST(git_commit_name)\n"
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
                      "    environment_comment,\n"
                      "    environment_compatible,\n"
                      "    environment_misc,\n"
                      "    log_url,\n"
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
        issue_version=dict(
            PreviousSchema.OO_QUERIES["issue_version"],
            statement="SELECT\n"
                      "    id,\n"
                      "    version AS version_num,\n"
                      "    origin,\n"
                      "    report_url,\n"
                      "    report_subject,\n"
                      "    culprit_code,\n"
                      "    culprit_tool,\n"
                      "    culprit_harness,\n"
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

            # Migrate data to the new columns
            cursor.execute("""
                UPDATE builds SET
                    status = CASE valid
                        WHEN TRUE THEN 'PASS'::STATUS
                        WHEN FALSE THEN 'FAIL'::STATUS
                        ELSE NULL
                    END
            """)

            # Migrate data to new rows
            waived_issue_origin = '_'
            waived_issue_id = f'{waived_issue_origin}:waived'
            waived_issue_version = '1'
            timestamp_expr = cls.TABLES['incidents']. \
                columns['_timestamp'].schema.metadata_expr
            cursor.execute(f"""
                INSERT INTO incidents (
                    _timestamp,
                    id,
                    origin,
                    issue_id,
                    issue_version,
                    test_id,
                    present
                )
                SELECT
                    {timestamp_expr},
                    '{waived_issue_id}:{waived_issue_version}:' || id AS id,
                    '{waived_issue_origin}' AS origin,
                    '{waived_issue_id}' AS issue_id,
                    {waived_issue_version} AS issue_version,
                    id AS test_id,
                    TRUE AS present
                FROM tests
                WHERE waived
            """)
            # If we inserted any waived incidents
            if cursor.rowcount != 0:
                # Add the waived issue
                timestamp_expr = cls.TABLES['issues']. \
                    columns['_timestamp'].schema.metadata_expr
                cursor.execute(f"""
                    INSERT INTO issues (
                        _timestamp, id, version, origin, comment
                    )
                    VALUES (
                        {timestamp_expr},
                        '{waived_issue_id}',
                        {waived_issue_version},
                        '{waived_issue_origin}',
                        'Test waived as unreliable'
                    )
                """)

            # Remove old columns
            # For all tables
            for name, schema in PreviousSchema.TABLES.items():
                if name not in cls.TABLES:
                    continue
                # For each removed column
                for column_name in sorted(
                    set(PreviousSchema.TABLES_ARGS[name]["columns"]) -
                    set(cls.TABLES_ARGS[name]["columns"])
                ):
                    cursor.execute(f"""
                        ALTER TABLE {name} DROP COLUMN
                        {schema.columns[column_name].name}
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
