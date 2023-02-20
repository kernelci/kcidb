"""Kernel CI report database - BigQuery schema v4.0"""

import decimal
import json
import logging
import textwrap
import datetime
from functools import reduce
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField as Field
from google.api_core.exceptions import BadRequest as GoogleBadRequest
from google.api_core.exceptions import NotFound as GoogleNotFound
import kcidb.io as io
import kcidb.orm as orm
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.schematic import \
    Schema as AbstractSchema, \
    Connection as AbstractConnection
from kcidb.db.misc import NotFound
from kcidb.db.bigquery.schema import validate_json_obj_list

# We'll manage for now, pylint: disable=too-many-lines

# Module's logger
LOGGER = logging.getLogger(__name__)


class Connection(AbstractConnection):
    """
    Kernel CI BigQuery report database connection.
    """

    # Documentation of the connection parameters
    _PARAMS_DOC = textwrap.dedent("""\
        Parameters: <PROJECT_ID>.<DATASET>
                or: <DATASET>

        <PROJECT_ID>    ID of the Google Cloud project hosting the dataset.
                        If not specified, the project from the credentials
                        file pointed to by GOOGLE_APPLICATION_CREDENTIALS
                        environment variable is used.

        <DATASET>       The name of the dataset containing the report data,
                        located within the specified (or inferred) Google Cloud
                        project.
    """)

    def __init__(self, params):
        """
        Initialize a BigQuery connection.

        Args:
            params: A parameter string describing the database to access. See
                    Connection._PARAMS_DOC for documentation. Cannot be None
                    (must be specified).

        Raises:
            NotFound        - the database does not exist,
        """
        assert params is None or isinstance(params, str)
        if params is None:
            raise Exception("Database parameters must be specified\n\n" +
                            self._PARAMS_DOC)
        super().__init__(params)
        try:
            dot_pos = params.index(".")
            project_id = params[:dot_pos]
            dataset_name = params[dot_pos + 1:]
        except ValueError:
            project_id = None
            dataset_name = params
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_name)
        try:
            self.client.get_dataset(self.dataset_ref)
        except GoogleNotFound as exc:
            raise NotFound(params) from exc

    def query_create(self, query_string, query_parameters=None):
        """
        Creates a Query job configured for a given query string and
        optional parameters. BigQuery can run the job to query the database.

        Args:
            query_string:       The SQL query string.
            query_parameters:   A list containing the optional query parameters
                                (google.cloud.bigquery.ArrayQueryParameter).
                                The default is an empty list.

        Returns:
            The Query job (google.cloud.bigquery.job.QueryJob)
        """
        if query_parameters is None:
            query_parameters = []
        LOGGER.debug("Query string: %s", query_string)
        LOGGER.debug("Query params: %s", query_parameters)
        job_config = bigquery.job.QueryJobConfig(
                query_parameters=query_parameters,
                default_dataset=self.dataset_ref)
        return self.client.query(query_string, job_config=job_config)

    def set_schema_version(self, version):
        """
        Set the schema version of the connected database (or remove it) in a
        separate transaction. Does not modify the data or upgrade its actual
        schema.

        Args:
            version:    A tuple of (major, minor) schema version numbers (both
                        non-negative integers) to set. None to remove the
                        version.
        """
        assert version is None or \
            isinstance(version, tuple) and len(version) == 2 and \
            isinstance(version[0], int) and version[0] >= 0 and \
            isinstance(version[1], int) and version[1] >= 0
        dataset = self.client.get_dataset(self.dataset_ref)
        if version is None:
            dataset.labels["version_major"] = None
            dataset.labels["version_minor"] = None
        else:
            dataset.labels["version_major"] = str(version[0])
            dataset.labels["version_minor"] = str(version[1])
        self.client.update_dataset(dataset, ["labels"])

    def get_schema_version(self):
        """
        Retrieve the schema version of the connected database, in a separate
        transaction.

        Returns:
            The major and the minor version numbers of the database schema,
            or None, if not initialized.
        """
        dataset = self.client.get_dataset(self.dataset_ref)
        if "version_major" in dataset.labels and \
           "version_minor" in dataset.labels:
            try:
                return int(dataset.labels["version_major"]), \
                    int(dataset.labels["version_minor"])
            except ValueError:
                return None
        if set(tli.table_id for tli in
               self.client.list_tables(self.dataset_ref)) == \
           {"revisions", "builds", "tests"}:
            return io.schema.V1.major, io.schema.V1.minor
        return None

    def get_last_modified(self):
        """
        Get the time the data in the connected database was last modified.
        Can return the minimum timestamp constant, if the database is not
        initialized or its data loading interface is not limited in the amount
        of load() method calls.

        Returns:
            A timezone-aware datetime object representing the last
            modification time.
        """
        return next(iter(self.query_create(
            "SELECT TIMESTAMP_MILLIS(MAX(last_modified_time)) "
            "FROM __TABLES__"
        ).result()))[0] or \
            datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


class Schema(AbstractSchema):
    """BigQuery database schema v4.0"""

    # The connection class to use for talking to the database.
    Connection = Connection
    # The schema's version.
    version = (4, 0)
    # The I/O schema the database schema supports
    io = io.schema.V4_0

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
                description="The widely-recognized name of the sub-tree "
                            "(fork) of the main code tree where the checked "
                            "out base source code came from.",
            ),
            Field(
                "git_repository_url", "STRING",
                description="The URL of the Git repository which contains the "
                            "checked out base source code. The shortest "
                            "possible https:// URL, or, if that's not "
                            "available, the shortest possible git:// URL.",
            ),
            Field(
                "git_commit_hash", "STRING",
                description="The full commit hash of the checked out base "
                            "source code",
            ),
            Field(
                "git_commit_name", "STRING",
                description="A human-readable name of the commit containing "
                            "the checked out base source code, as would be "
                            "output by \"git describe\", at the checkout "
                            "time."
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
                            "A sha256 hash over newline-terminated sha256 "
                            "hashes of each patch from the patchset, in "
                            "order. E.g. generated with this shell "
                            "command: \"sha256sum *.patch | cut -c-64 | "
                            "sha256sum | cut -c-64\".\n"
                            "\n"
                            "An empty string, if no patches were applied to "
                            "the checked out base source code.\n",
            ),
            Field(
                "message_id", "STRING",
                description="The value of the Message-ID header of the "
                            "e-mail message introducing the checked-out "
                            "source code, if any. E.g. a message with the "
                            "applied patchset, or a release announcement "
                            "sent to a maillist.",
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
                description="List of e-mail addresses of contacts concerned "
                            "with the checked out source code, such as "
                            "authors, reviewers, and mail lists",
            ),
            Field(
                "log_url", "STRING",
                description="The URL of the log file of the checkout attempt. "
                            "E.g. 'git am' output.",
            ),
            Field(
                "log_excerpt", "STRING",
                description="A part of the log file of the checkout attempt "
                            "most relevant to its outcome.",
            ),
            Field(
                "valid", "BOOL",
                description="True if the checkout succeeded, i.e. if the "
                            "source code parts could be combined. False if "
                            "not, e.g. if the patches failed to apply.",
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
                description="The number of seconds it took to complete the "
                            "build",
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
                description="Name and version of the compiler used to make "
                            "the build",
            ),
            Field(
                "input_files", "RECORD", mode="REPEATED",
                fields=RESOURCE_FIELDS,
                description="A list of build input files. "
                            "E.g. configuration.",
            ),
            Field(
                "output_files", "RECORD", mode="REPEATED",
                fields=RESOURCE_FIELDS,
                description="A list of build output files: images, packages, "
                            "etc.",
            ),
            Field(
                "config_name", "STRING",
                description="A name describing the build configuration "
                            "options.",
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
                description="A part of the log file of the build most "
                            "relevant to its outcome.",
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
                            "classification tree the executed test belongs "
                            "to. E.g. \"LTPlite.sem01\". The empty string, "
                            "signifies the root of the tree, i.e. an "
                            "abstract test.",
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
                "output_files", "RECORD", mode="REPEATED",
                fields=RESOURCE_FIELDS,
                description="A list of test outputs: logs, dumps, etc.",
            ),
            Field(
                "misc", "STRING",
                description="Miscellaneous extra data about the test run "
                            "in JSON format",
            ),
        ]
    )

    # A map of table names and their "primary key" fields
    KEY_MAP = dict(
        checkouts=("id",),
        builds=("id",),
        tests=("id",),
    )

    # Queries for each type of raw object-oriented data
    OO_QUERIES = dict(
        revision="SELECT\n"
                 "    git_commit_hash,\n"
                 "    patchset_hash,\n"
                 "    ANY_VALUE(patchset_files) AS patchset_files,\n"
                 "    ANY_VALUE(git_commit_name) AS git_commit_name,\n"
                 "    ANY_VALUE(contacts) AS contacts\n"
                 "FROM checkouts\n"
                 "GROUP BY git_commit_hash, patchset_hash",
        checkout="SELECT\n"
                 "    id,\n"
                 "    git_commit_hash,\n"
                 "    patchset_hash,\n"
                 "    origin,\n"
                 "    git_repository_url,\n"
                 "    git_repository_branch,\n"
                 "    tree_name,\n"
                 "    message_id,\n"
                 "    start_time,\n"
                 "    log_url,\n"
                 "    log_excerpt,\n"
                 "    comment,\n"
                 "    valid,\n"
                 "    misc\n"
                 "FROM checkouts",
        build="SELECT\n"
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
              "    valid,\n"
              "    misc\n"
              "FROM builds",
        test="SELECT\n"
             "    id,\n"
             "    build_id,\n"
             "    origin,\n"
             "    path,\n"
             "    environment.comment AS environment_comment,\n"
             "    environment.misc AS environment_misc,\n"
             "    status,\n"
             "    waived,\n"
             "    start_time,\n"
             "    duration,\n"
             "    output_files,\n"
             "    log_url,\n"
             "    log_excerpt,\n"
             "    comment,\n"
             "    misc\n"
             "FROM tests",
        bug='SELECT\n'
            '    ""    AS url,\n'
            '    ""    AS subject,\n'
            '    FALSE AS culprit_code,\n'
            '    FALSE AS culprit_tool,\n'
            '    FALSE AS culprit_harness\n'
            'FROM UNNEST([])',
        issue='SELECT\n'
              '    ""    AS id,\n'
              '    0     AS version,\n'
              '    ""    AS origin,\n'
              '    ""    AS report_url,\n'
              '    ""    AS report_subject,\n'
              '    FALSE AS culprit_code,\n'
              '    FALSE AS culprit_tool,\n'
              '    FALSE AS culprit_harness,\n'
              '    FALSE AS build_valid,\n'
              '    ""    AS test_status,\n'
              '    ""    AS comment,\n'
              '    ""    AS misc\n'
              'FROM UNNEST([])',
        incident='SELECT\n'
                 '    "" AS id,\n'
                 '    "" AS origin,\n'
                 '    "" AS issue_id,\n'
                 '    0  AS issue_version,\n'
                 '    "" AS build_id,\n'
                 '    "" AS test_id,\n'
                 '    "" AS comment,\n'
                 '    "" AS misc\n'
                 'FROM UNNEST([])',
    )

    @classmethod
    def _create_table(cls, conn, table_name, table_schema):
        """
        Create a table and its view.

        Args:
            conn:           The connection to create the table with.
            table_name:     Name of the table being created.
            table_schema:   Schema of the table being created.
        """
        assert isinstance(conn, cls.Connection)
        assert isinstance(table_name, str)
        assert isinstance(table_schema, list)
        # Create raw table with duplicate records
        table_ref = conn.dataset_ref.table("_" + table_name)
        table = bigquery.table.Table(table_ref, schema=table_schema)
        conn.client.create_table(table)
        # Create a view deduplicating the table records
        view_ref = conn.dataset_ref.table(table_name)
        view = bigquery.table.Table(view_ref)
        view.view_query = \
            "SELECT " + \
            ", ".join(
                f"`{n}`" if n in cls.KEY_MAP[table_name]
                else f"ANY_VALUE(`{n}`) AS `{n}`"
                for n in (f.name for f in table_schema)
            ) + \
            f" FROM `{table_ref}` GROUP BY " + \
            ", ".join(cls.KEY_MAP[table_name])
        conn.client.create_table(view)

    def init(self):
        """
        Initialize the database. The database must be uninitialized.
        """
        # Create tables and corresponding views
        for table_name, table_schema in self.TABLE_MAP.items():
            self._create_table(self.conn, table_name, table_schema)

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """
        for table_name in self.TABLE_MAP:
            view_ref = self.conn.dataset_ref.table(table_name)
            try:
                self.conn.client.delete_table(view_ref)
            except GoogleNotFound:
                pass
            table_ref = self.conn.dataset_ref.table("_" + table_name)
            try:
                self.conn.client.delete_table(table_ref)
            except GoogleNotFound:
                pass

    def empty(self):
        """
        Empty the database, removing all data.
        The database must be initialized.
        """
        for table_name in self.TABLE_MAP:
            self.conn.query_create(
                f"DELETE FROM `_{table_name}` WHERE TRUE"
            ).result()

    @classmethod
    def _unpack_node(cls, node, drop_null=True):
        """
        Unpack a retrieved data node (and all its children) to
        the JSON-compatible and schema-complying representation.

        Args:
            node:       The node to unpack.
            drop_null:  Drop nodes with NULL values, if true.
                        Keep them otherwise.

        Returns:
            The unpacked node.
        """
        if isinstance(node, decimal.Decimal):
            node = float(node)
        elif isinstance(node, datetime.datetime):
            node = node.isoformat(timespec='microseconds')
        elif isinstance(node, list):
            for index, value in enumerate(node):
                node[index] = cls._unpack_node(value)
        elif isinstance(node, dict):
            for key, value in list(node.items()):
                if value == []:
                    node[key] = None
                    value = None
                if value is None:
                    if drop_null:
                        del node[key]
                elif key == "misc" or key.endswith("_misc"):
                    node[key] = json.loads(value)
                else:
                    node[key] = cls._unpack_node(value)
        return node

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the I/O version
            of the database schema, each containing at most the specified
            number of objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        obj_num = 0
        data = self.io.new()
        for obj_list_name in self.TABLE_MAP:
            query_string = f"SELECT * FROM `{obj_list_name}`"
            query_job = self.conn.query_create(query_string)
            obj_list = None
            for row in query_job:
                if obj_list is None:
                    obj_list = []
                    data[obj_list_name] = obj_list
                obj_list.append(self._unpack_node(dict(row.items())))
                obj_num += 1
                if objects_per_report and obj_num >= objects_per_report:
                    assert self.io.is_compatible_exactly(data)
                    assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
                    yield data
                    obj_num = 0
                    data = self.io.new()
                    obj_list = None

        if obj_num:
            assert self.io.is_compatible_exactly(data)
            assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
            yield data

    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from the database, in object number-limited
        chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the I/O version
            of the database schema, each containing at most the specified
            number of objects.
        """
        # Calm down, we'll get to it,
        # pylint: disable=too-many-locals,too-many-statements
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        # A dictionary of object list names and two-element lists,
        # containing a SELECT statement (returning IDs of the objects to
        # fetch), and the list of its parameters.
        obj_list_queries = {
            obj_list_name: [
                "SELECT id FROM UNNEST(?) AS id\n",
                [
                    bigquery.ArrayQueryParameter(
                        None, "STRING", ids.get(obj_list_name, [])
                    ),
                ],
            ]
            for obj_list_name in self.io.graph if obj_list_name
        }

        # Add referenced parents if requested
        if parents:
            def add_parents(obj_list_name):
                """Add parent IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in self.io.graph[obj_list_name]:
                    add_parents(child_list_name)
                    child_query = obj_list_queries[child_list_name]
                    query[0] += \
                        f"UNION DISTINCT\n" \
                        f"SELECT {child_list_name}.{obj_name}_id AS id " \
                        f"FROM {child_list_name} " + \
                        "INNER JOIN (\n" + \
                        textwrap.indent(child_query[0], " " * 4) + \
                        ") USING(id)\n"
                    query[1] += child_query[1]

            for obj_list_name in self.io.graph[""]:
                add_parents(obj_list_name)

        # Add referenced children if requested
        if children:
            def add_children(obj_list_name):
                """Add child IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in self.io.graph[obj_list_name]:
                    child_query = obj_list_queries[child_list_name]
                    child_query[0] += \
                        f"UNION DISTINCT\n" \
                        f"SELECT {child_list_name}.id AS id " \
                        f"FROM {child_list_name} " + \
                        "INNER JOIN (\n" + \
                        textwrap.indent(query[0], " " * 4) + \
                        f") AS {obj_list_name} ON " \
                        f"{child_list_name}.{obj_name}_id = " \
                        f"{obj_list_name}.id\n"
                    child_query[1] += query[1]
                    add_children(child_list_name)

            for obj_list_name in self.io.graph[""]:
                add_children(obj_list_name)

        # Fetch the data
        obj_num = 0
        data = self.io.new()
        for obj_list_name, query in obj_list_queries.items():
            query_parameters = query[1]
            query_string = \
                f"SELECT * FROM `{obj_list_name}` INNER JOIN (\n" + \
                textwrap.indent(query[0], " " * 4) + \
                ") USING(id)\n"
            query_job = self.conn.query_create(query_string, query_parameters)
            obj_list = None
            for row in query_job:
                if obj_list is None:
                    obj_list = []
                    data[obj_list_name] = obj_list
                obj_list.append(self._unpack_node(dict(row.items())))
                obj_num += 1
                if objects_per_report and obj_num >= objects_per_report:
                    assert self.io.is_compatible_exactly(data)
                    assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
                    yield data
                    obj_num = 0
                    data = self.io.new()
                    obj_list = None

        if obj_num:
            assert self.io.is_compatible_exactly(data)
            assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
            yield data

    @classmethod
    def _oo_query_render(cls, pattern):
        """
        Render a pattern matching raw OO data into a query.

        Args:
            pattern:    The pattern (instance of kcidb.orm.Pattern) to
                        render.

        Returns:
            The SQL query string and the query parameters.
        """
        assert isinstance(pattern, orm.Pattern)
        obj_type = pattern.obj_type
        type_query_string = cls.OO_QUERIES[obj_type.name]
        if pattern.obj_id_set:
            obj_id_fields = obj_type.id_fields
            query_string = "SELECT obj.* FROM (\n" + \
                textwrap.indent(type_query_string, " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                "    SELECT * FROM UNNEST(?)\n" + \
                ") AS ids USING(" + ", ".join(obj_id_fields) + ")"
            query_parameters = [
                bigquery.ArrayQueryParameter(
                    None,
                    "STRUCT",
                    [
                        bigquery.StructQueryParameter(
                            None,
                            *(
                                bigquery.ScalarQueryParameter(c, "STRING", v)
                                for c, v in zip(obj_id_fields, obj_id)
                            )
                        )
                        for obj_id in pattern.obj_id_set
                    ]
                )
            ]
        else:
            query_string = type_query_string
            if pattern.obj_id_set is not None:
                # Workaround empty array parameters not having element type
                query_string += " LIMIT 0"
            query_parameters = []

        if pattern.base:
            base_query_string, base_query_parameters = \
                cls._oo_query_render(pattern.base)
            base_obj_type = pattern.base.obj_type
            if pattern.child:
                column_pairs = zip(
                    base_obj_type.children[obj_type.name].ref_fields,
                    base_obj_type.id_fields
                )
            else:
                column_pairs = zip(
                    obj_type.id_fields,
                    obj_type.children[base_obj_type.name].ref_fields
                )

            query_string = "SELECT obj.* FROM (\n" + \
                textwrap.indent(query_string, " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                textwrap.indent(base_query_string, " " * 4) + "\n" + \
                ") AS base ON " + \
                " AND ".join(
                    [f"obj.{o} = base.{b}" for o, b in column_pairs]
                )
            query_parameters += base_query_parameters

        return query_string, query_parameters

    def oo_query(self, pattern_set):
        """
        Query raw object-oriented data from the database.

        Args:
            pattern_set:    A set of patterns ("kcidb.orm.Pattern"
                            instances) matching objects to fetch.
        Returns:
            A dictionary of object type names and lists containing retrieved
            objects of the corresponding type.
        """
        assert isinstance(pattern_set, set)
        assert all(isinstance(r, orm.Pattern) for r in pattern_set)

        # Render all queries for each type
        obj_type_queries = {}
        for obj_type in orm.SCHEMA.types.values():
            for pattern in pattern_set:
                # TODO: Avoid adding the same patterns multiple times
                if pattern.obj_type == obj_type:
                    if obj_type not in obj_type_queries:
                        obj_type_queries[obj_type] = []
                    obj_type_queries[obj_type]. \
                        append(self._oo_query_render(pattern))

        # Execute all the queries
        objs = {}
        for obj_type, queries in obj_type_queries.items():
            # Workaround lack of equality operation for array columns
            # required for "UNION DISTINCT"
            query_string = "SELECT obj.* FROM (\n" + \
                textwrap.indent(self.OO_QUERIES[obj_type.name],
                                " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                "    SELECT DISTINCT " + \
                ", ".join(obj_type.id_fields) + \
                " FROM (\n" + \
                textwrap.indent("\nUNION ALL\n".join(q[0] for q in queries),
                                " " * 8) + "\n" + \
                "    )\n" + \
                ") AS ids USING(" + ", ".join(obj_type.id_fields) + ")"
            query_parameters = reduce(lambda x, y: x + y,
                                      (q[1] for q in queries))
            job = self.conn.query_create(query_string, query_parameters)
            objs[obj_type.name] = [
                self._unpack_node(dict(row.items()), drop_null=False)
                for row in job.result()
            ]

        assert LIGHT_ASSERTS or orm.SCHEMA.is_valid(objs)
        return objs

    @classmethod
    def _pack_node(cls, node):
        """
        Pack a loaded data node (and all its children) to
        the BigQuery storage-compatible representation.

        Args:
            node:   The node to pack.

        Returns:
            The packed node.
        """
        if isinstance(node, list):
            node = node.copy()
            for index, value in enumerate(node):
                node[index] = cls._pack_node(value)
        elif isinstance(node, dict):
            node = node.copy()
            for key, value in list(node.items()):
                # Flatten the "misc" fields
                if key == "misc":
                    node[key] = json.dumps(value)
                else:
                    node[key] = cls._pack_node(value)
        return node

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the I/O version of the database schema.
        """
        assert self.io.is_compatible_directly(data)
        assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)

        # Load the data
        for obj_list_name, table_schema in self.TABLE_MAP.items():
            if obj_list_name in data:
                obj_list = self._pack_node(data[obj_list_name])
                if not LIGHT_ASSERTS:
                    validate_json_obj_list(table_schema, obj_list)
                job_config = bigquery.job.LoadJobConfig(autodetect=False,
                                                        schema=table_schema)
                job = self.conn.client.load_table_from_json(
                    obj_list,
                    self.conn.dataset_ref.table("_" + obj_list_name),
                    job_config=job_config)
                try:
                    job.result()
                except GoogleBadRequest as exc:
                    raise Exception("".join([
                        f"ERROR: {error['message']}\n" for error in job.errors
                    ])) from exc
