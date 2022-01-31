"""
Kernel CI BigQuery report database schema.

Always corresponds to the current I/O schema.
"""
from google.cloud.bigquery.schema import SchemaField as Field

# Resource record fields
RESOURCE_FIELDS = (
    Field("name", "STRING", description="Resource name"),
    Field("url", "STRING", description="Resource URL"),
)

# Test environment fields
ENVIRONMENT_FIELDS = (
    Field(
        "comment", "STRING",
        description="A human-readable comment regarding the environment."
    ),
    Field(
        "misc", "STRING",
        description="Miscellaneous extra data about the environment "
                    "in JSON format",
    ),
)

# A map of table names to their BigQuery schemas
TABLE_MAP = dict(
    checkouts=[
        Field(
            "id", "STRING",
            description="Source code checkout ID",
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the checkout",
        ),
        Field(
            "tree_name", "STRING",
            description="The widely-recognized name of the sub-tree (fork) "
                        "of the main code tree where the checked out base "
                        "source code came from.",
        ),
        Field(
            "git_repository_url", "STRING",
            description="The URL of the Git repository which contains the "
                        "checked out base source code. The shortest "
                        "possible https:// URL, or, if that's not available, "
                        "the shortest possible git:// URL.",
        ),
        Field(
            "git_commit_hash", "STRING",
            description="The full commit hash of the checked out base source "
                        "code",
        ),
        Field(
            "git_commit_name", "STRING",
            description="A human-readable name of the commit containing the "
                        "checked out base source code, as would be output by "
                        "\"git describe\", at the checkout time."
        ),
        Field(
            "git_repository_branch", "STRING",
            description="The Git repository branch from which the commit "
                        "with the base source code was checked out."
        ),
        Field(
            "patchset_files", "RECORD", mode="REPEATED",
            fields=RESOURCE_FIELDS,
            description="List of patch files representing the patchset "
                        "applied to the checked out base source code, "
                        "in order of application. "
                        "Each linked file must be in a format accepted by "
                        "\"git apply\".",
        ),
        Field(
            "patchset_hash", "STRING",
            description="The patchset hash.\n"
                        "\n"
                        "A sha256 hash over newline-terminated sha256 hashes "
                        "of each patch from the patchset, in order. E.g. "
                        "generated with this shell command: \""
                        "sha256sum *.patch | cut -c-64 | sha256sum | "
                        "cut -c-64\".\n"
                        "\n"
                        "An empty string, if no patches were applied to the "
                        "checked out base source code.\n",
        ),
        Field(
            "message_id", "STRING",
            description="The value of the Message-ID header of the e-mail "
                        "message introducing the checked-out source code, "
                        "if any. E.g. a message with the applied patchset, "
                        "or a release announcement sent to a maillist.",
        ),
        Field(
            "comment", "STRING",
            description="A human-readable comment regarding the checkout. "
                        "E.g. the checked out release version, or the "
                        "subject of the message with the applied patchset."
        ),
        Field(
            "start_time", "TIMESTAMP",
            description="The time the checkout was started.",
        ),
        Field(
            "contacts", "STRING", mode="REPEATED",
            description="List of e-mail addresses of contacts concerned with "
                        "the checked out source code, such as authors, "
                        "reviewers, and mail lists",
        ),
        Field(
            "log_url", "STRING",
            description="The URL of the log file of the checkout attempt. "
                        "E.g. 'git am' output.",
        ),
        Field(
            "log_excerpt", "STRING",
            description="A part of the log file of the checkout attempt most "
                        "relevant to its outcome.",
        ),
        Field(
            "valid", "BOOL",
            description="True if the checkout succeeded, i.e. if the source "
                        "code parts could be combined. False if not, e.g. if "
                        "the patches failed to apply.",
        ),
        Field(
            "misc", "STRING",
            description="Miscellaneous extra data about the checkout "
                        "in JSON format",
        ),
    ],
    builds=[
        Field(
            "checkout_id", "STRING",
            description="ID of the built source code checkout.",
        ),
        Field(
            "id", "STRING",
            description="Build ID",
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the build",
        ),
        Field(
            "comment", "STRING",
            description="A human-readable comment regarding the build"
        ),
        Field(
            "start_time", "TIMESTAMP",
            description="The time the build was started",
        ),
        Field(
            "duration", "FLOAT64",
            description="The number of seconds it took to complete the build",
        ),
        Field(
            "architecture", "STRING",
            description="Target architecture of the build",
        ),
        Field(
            "command", "STRING",
            description="Full shell command line used to make the build, "
                        "including environment variables",
        ),
        Field(
            "compiler", "STRING",
            description="Name and version of the compiler used to make the "
                        "build",
        ),
        Field(
            "input_files", "RECORD", mode="REPEATED", fields=RESOURCE_FIELDS,
            description="A list of build input files. E.g. configuration.",
        ),
        Field(
            "output_files", "RECORD", mode="REPEATED", fields=RESOURCE_FIELDS,
            description="A list of build output files: images, packages, etc.",
        ),
        Field(
            "config_name", "STRING",
            description="A name describing the build configuration options.",
        ),
        Field(
            "config_url", "STRING",
            description="The URL of the build configuration file.",
        ),
        Field(
            "log_url", "STRING",
            description="The URL of the build log file.",
        ),
        Field(
            "log_excerpt", "STRING",
            description="A part of the log file of the build most relevant "
                        "to its outcome.",
        ),
        Field(
            "valid", "BOOL",
            description="True if the build is valid, i.e. if it could be "
                        "completed. False if not.",
        ),
        Field(
            "misc", "STRING",
            description="Miscellaneous extra data about the build "
                        "in JSON format",
        ),
    ],
    tests=[
        Field(
            "build_id", "STRING",
            description="ID of the tested build",
        ),
        Field(
            "id", "STRING",
            description="Test run ID",
        ),
        Field(
            "origin", "STRING",
            description="The name of the CI system which submitted "
                        "the test run",
        ),
        Field(
            "environment", "RECORD", fields=ENVIRONMENT_FIELDS,
            description="The environment the test ran in. "
                        "E.g. a host, a set of hosts, or a lab; "
                        "amount of memory/storage/CPUs, for each host; "
                        "process environment variables, etc.",
        ),
        Field(
            "path", "STRING",
            description="Dot-separated path to the node in the test "
                        "classification tree the executed test belongs to. "
                        "E.g. \"LTPlite.sem01\". The empty string, or the "
                        "absence of the property signify the root of the "
                        "tree, i.e. an abstract test.",
        ),
        Field(
            "comment", "STRING",
            description="A human-readable comment regarding the test run"
        ),
        Field(
            "log_url", "STRING",
            description="The URL of the main test output/log file.",
        ),
        Field(
            "log_excerpt", "STRING",
            description="A part of the main test output/log file most "
                        "relevant to its outcome.",
        ),
        Field(
            "status", "STRING",
            description="The test status, one of the following. "
                        "\"ERROR\" - the test is faulty, "
                        "the status of the tested code is unknown. "
                        "\"FAIL\" - the test has failed, the tested code is "
                        "faulty. "
                        "\"PASS\" - the test has passed, the tested code is "
                        "correct. "
                        "\"DONE\" - the test has finished successfully, "
                        "the status of the tested code is unknown. "
                        "\"SKIP\" - the test wasn't executed, "
                        "the status of the tested code is unknown. "
        ),
        Field(
            "waived", "BOOL",
            description="True if the test status should be ignored",
        ),
        Field(
            "start_time", "TIMESTAMP",
            description="The time the test run was started",
        ),
        Field(
            "duration", "FLOAT64",
            description="The number of seconds it took to run the test",
        ),
        Field(
            "output_files", "RECORD", mode="REPEATED", fields=RESOURCE_FIELDS,
            description="A list of test outputs: logs, dumps, etc.",
        ),
        Field(
            "misc", "STRING",
            description="Miscellaneous extra data about the test run "
                        "in JSON format",
        ),
    ]
)


class JSONInvalidError(Exception):
    """JSON doesn't match table schema error"""


def validate_json_value(field, value):
    """
    Validate a JSON value against the schema for the field it's supposed to be
    loaded into.

    Args:
        field:  The field schema to validate against, an instance of
                google.cloud.bigquery.schema.SchemaField.
        value:  The JSON value to validate.

    Returns:
        The validated JSON value.

    Raises:
        JSONInvalidError: the JSON value doesn't match the field type.
    """
    assert isinstance(field, Field)

    if field.field_type == "BOOL":
        if not isinstance(value, bool):
            raise JSONInvalidError(f"Value is not a boolean: {value!r}")
    if field.field_type == "STRING":
        if not isinstance(value, str):
            raise JSONInvalidError(f"Value is not a string: {value!r}")
    elif field.field_type == "FLOAT64":
        if not isinstance(value, (int, float)):
            raise JSONInvalidError(f"Value is not numeric: {value!r}")
    elif field.field_type == "TIMESTAMP":
        if not isinstance(value, str):
            raise JSONInvalidError(
                f"Timestamp value is not a string: {value!r}"
            )
    elif field.field_type == "RECORD":
        if not isinstance(value, dict):
            raise JSONInvalidError(
                f"Record value is not a dictionary: {value!r}"
            )
        validate_json_obj(field.fields, value)
    else:
        ValueError(f"Unknown field type: {field.field_type!r}")

    return value


def validate_json_obj(field_list, obj):
    """
    Validate a JSON object against the schema of the table it's to be loaded
    into.

    Args:
        field_list: The list of table fields to validate against. A list of
                    google.cloud.bigquery.schema.SchemaField instances.
        obj:        The JSON object to validate.

    Returns:
        The validated JSON object.

    Raises:
        JSONInvalidError: the JSON object doesn't match the schema.
    """
    assert isinstance(field_list, (tuple, list)) and \
           all(isinstance(f, Field) for f in field_list)
    assert isinstance(obj, dict)

    field_map = {f.name: f for f in field_list}
    unseen_field_map = field_map.copy()
    for name, value in obj.items():
        try:
            field = field_map[name]
            unseen_field_map.pop(name, None)
        except KeyError:
            raise JSONInvalidError(f"Unknown field: {name!r}") from None
        try:
            if field.mode == "REPEATED":
                if not isinstance(value, list):
                    raise JSONInvalidError(
                        f"Repeated field value is not a list: {value!r}"
                    )
                for idx, item in enumerate(value):
                    try:
                        validate_json_value(field, item)
                    except JSONInvalidError as err:
                        raise JSONInvalidError(
                            f"Repeated value at index {idx} is invalid"
                        ) from err
            else:
                validate_json_value(field, value)
        except JSONInvalidError as err:
            raise JSONInvalidError(
                f"Value of field {name!r} is invalid"
            ) from err

    for unseen_name, unseen_field in unseen_field_map.items():
        if unseen_field.mode == "REQUIRED":
            raise JSONInvalidError(
                f"Required field {unseen_name!r} is missing"
            )

    return obj


def validate_json_obj_list(field_list, obj_list):
    """
    Validate a list of JSON objects against the schema of the table they are
    to be loaded into.

    Args:
        field_list: The list of table fields to validate against. A list of
                    google.cloud.bigquery.schema.SchemaField instances.
        obj_list:   The list of JSON objects to validate.

    Returns:
        The validated JSON object list.

    Raises:
        JSONInvalidError: the JSON object doesn't match the schema.
    """
    assert isinstance(field_list, list) and \
           all(isinstance(f, Field) for f in field_list)
    assert isinstance(obj_list, list) and \
           all(isinstance(o, dict) for o in obj_list)

    for obj in obj_list:
        validate_json_obj(field_list, obj)

    return obj_list


# Queries for each type of raw object-oriented data
OO_QUERIES = dict(
    revision="SELECT\n"
             "   git_commit_hash,\n"
             "   patchset_hash,\n"
             "   ANY_VALUE(patchset_files) AS patchset_files,\n"
             "   ANY_VALUE(git_commit_name) AS git_commit_name,\n"
             "   ANY_VALUE(contacts) AS contacts\n"
             "FROM checkouts\n"
             "GROUP BY git_commit_hash, patchset_hash",
    checkout="SELECT\n"
             "   id,\n"
             "   git_commit_hash,\n"
             "   patchset_hash,\n"
             "   origin,\n"
             "   git_repository_url,\n"
             "   git_repository_branch,\n"
             "   tree_name,\n"
             "   message_id,\n"
             "   start_time,\n"
             "   log_url,\n"
             "   log_excerpt,\n"
             "   comment,\n"
             "   valid,\n"
             "   misc\n"
             "FROM checkouts",
    build="SELECT\n"
          "   id,\n"
          "   checkout_id,\n"
          "   origin,\n"
          "   start_time,\n"
          "   duration,\n"
          "   architecture,\n"
          "   command,\n"
          "   compiler,\n"
          "   input_files,\n"
          "   output_files,\n"
          "   config_name,\n"
          "   config_url,\n"
          "   log_url,\n"
          "   log_excerpt,\n"
          "   comment,\n"
          "   valid,\n"
          "   misc\n"
          "FROM builds",
    test="SELECT\n"
         "   id,\n"
         "   build_id,\n"
         "   origin,\n"
         "   path,\n"
         "   environment.comment AS environment_comment,\n"
         "   environment.misc AS environment_misc,\n"
         "   status,\n"
         "   waived,\n"
         "   start_time,\n"
         "   duration,\n"
         "   output_files,\n"
         "   log_url,\n"
         "   log_excerpt,\n"
         "   comment,\n"
         "   misc\n"
         "FROM tests",
)
