"""Kernel CI report database - PostgreSQL schema v4.0"""

import random
import logging
import textwrap
import datetime
import psycopg2
import psycopg2.extras
import psycopg2.errors
import kcidb.io as io
import kcidb.orm as orm
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.schematic import \
    Schema as AbstractSchema, \
    Connection as AbstractConnection
from kcidb.db.postgresql.schema import \
    Constraint, BoolColumn, FloatColumn, IntegerColumn, TimestampColumn, \
    VarcharColumn, TextColumn, JSONColumn, Table

# Module's logger
LOGGER = logging.getLogger(__name__)


class Connection(AbstractConnection):
    """
    Kernel CI PostgreSQL report database connection.
    Exposes PostgreSQL connection interface.
    """

    # Documentation of the connection parameters
    _PARAMS_DOC = textwrap.dedent("""\
        Parameters: [<CONNECTION>]

        <CONNECTION>    A libpq connection string described in
                        https://www.postgresql.org/docs/current/
                        libpq-connect.html#LIBPQ-CONNSTRING
                        specifying the database to connect to, as well as
                        authentication credentials.

                        The password can also be specified in a .pgpass file
                        described in https://www.postgresql.org/docs/current/
                        libpq-pgpass.html

                        Environment variables are consulted as described in
                        https://www.postgresql.org/docs/current/
                        libpq-envars.html

                        If starts with an exclamation mark ('!'), the
                        in-database data is prioritized explicitly initially,
                        instead of randomly. Double to include one literally.
    """)

    def __init__(self, params):
        """
        Initialize a PostgreSQL connection.

        Args:
            params:         A parameter string describing the database to
                            connect to. See Connection._PARAMS_DOC for
                            documentation. Assumed to be empty string, if
                            None.
        """
        assert params is None or isinstance(params, str)
        if params is None:
            params = ""

        self.load_prio_db = bool(random.randint(0, 1))
        if params.startswith("!"):
            if not params.startswith("!!"):
                self.load_prio_db = True
            params = params[1:]

        super().__init__(params)
        # Create the connection
        self.conn = psycopg2.connect(
            params,
            connection_factory=psycopg2.extras.LoggingConnection
        )
        # Specify the logger to the LoggingConnection
        # It logs with DEBUG level, judging from the source (but not the docs)
        self.conn.initialize(LOGGER)
        # Set session timezone to UTC, overriding local settings
        with self, self.cursor() as cursor:
            cursor.execute("SET SESSION TIME ZONE 'UTC'")

    def __getattr__(self, name):
        """
        Retrieve missing attributes from the PostgreSQL connection object.
        """
        return getattr(self.conn, name)

    def __enter__(self):
        """Enter the connection runtime context"""
        return self.conn.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        """Leave the connection runtime context"""
        return self.conn.__exit__(exc_type, exc_value, traceback)

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
        # Oh, but the connection is, pylint: disable=not-context-manager
        with self, self.cursor() as cursor:
            if version is None:
                cursor.execute("DROP FUNCTION IF EXISTS get_version()")
            else:
                number = int(version[0] * 1000 + version[1] % 1000)
                cursor.execute(textwrap.dedent("""\
                    CREATE OR REPLACE FUNCTION get_version()
                    RETURNS integer
                    LANGUAGE SQL
                    IMMUTABLE
                    RETURN %s
                """), (number, ))

    def get_schema_version(self):
        """
        Retrieve the schema version of the connected database, in a separate
        transaction.

        Returns:
            The major and the minor version numbers of the database schema,
            or None, if not initialized.
        """
        try:
            # Oh, but the connection is, pylint: disable=not-context-manager
            with self, self.cursor() as cursor:
                cursor.execute("SELECT get_version()")
                number = cursor.fetchone()[0]
                return int(number / 1000), int(number % 1000)
        # It's auto-generated, pylint: disable=no-member
        except psycopg2.errors.UndefinedFunction:
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
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


class Schema(AbstractSchema):
    """PostgreSQL database schema v4.0"""

    # The connection class to use for talking to the database.
    Connection = Connection
    # The schema's version.
    version = (4, 0)
    # The I/O schema the database schema supports
    io = io.schema.V4_0

    # A map of table names to table definitions
    TABLES = dict(
        checkouts=Table({
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "tree_name": TextColumn(),
            "git_repository_url": TextColumn(),
            "git_commit_hash": TextColumn(),
            "git_commit_name": TextColumn(),
            "git_repository_branch": TextColumn(),
            "patchset_files": JSONColumn(),
            "patchset_hash": TextColumn(),
            "message_id": TextColumn(),
            "comment": TextColumn(),
            "start_time": TimestampColumn(),
            "contacts": JSONColumn(),
            "log_url": TextColumn(),
            "log_excerpt": VarcharColumn(16384),
            "valid": BoolColumn(),
            "misc": JSONColumn(),
        }),
        builds=Table({
            "checkout_id": TextColumn(constraint=Constraint.NOT_NULL),
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "comment": TextColumn(),
            "start_time": TimestampColumn(),
            "duration": FloatColumn(),
            "architecture": TextColumn(),
            "command": TextColumn(),
            "compiler": TextColumn(),
            "input_files": JSONColumn(),
            "output_files": JSONColumn(),
            "config_name": TextColumn(),
            "config_url": TextColumn(),
            "log_url": TextColumn(),
            "log_excerpt": VarcharColumn(16384),
            "valid": BoolColumn(),
            "misc": JSONColumn(),
        }),
        tests=Table({
            "build_id": TextColumn(constraint=Constraint.NOT_NULL),
            "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
            "origin": TextColumn(constraint=Constraint.NOT_NULL),
            "environment.comment": TextColumn(),
            "environment.misc": JSONColumn(),
            "path": TextColumn(),
            "comment": TextColumn(),
            "log_url": TextColumn(),
            "log_excerpt": VarcharColumn(16384),
            "status": TextColumn(),
            "waived": BoolColumn(),
            "start_time": TimestampColumn(),
            "duration": FloatColumn(),
            "output_files": JSONColumn(),
            "misc": JSONColumn()
        }),
    )

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    # NOTE: Relying on dictionaries preserving order in Python 3.6+
    OO_QUERIES = dict(
        revision=dict(
            statement="SELECT\n"
                      "    git_commit_hash,\n"
                      "    patchset_hash,\n"
                      "    FIRST(patchset_files),\n"
                      "    FIRST(git_commit_name),\n"
                      "    FIRST(contacts)\n"
                      "FROM checkouts\n"
                      "GROUP BY git_commit_hash, patchset_hash",
            schema=Table(dict(
                git_commit_hash=TextColumn(),
                patchset_hash=TextColumn(),
                patchset_files=JSONColumn(),
                git_commit_name=TextColumn(),
                contacts=JSONColumn(),
            )),
        ),
        checkout=dict(
            statement="SELECT\n"
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
            schema=Table(dict(
                id=TextColumn(),
                git_commit_hash=TextColumn(),
                patchset_hash=TextColumn(),
                origin=TextColumn(),
                git_repository_url=TextColumn(),
                git_repository_branch=TextColumn(),
                tree_name=TextColumn(),
                message_id=TextColumn(),
                start_time=TimestampColumn(),
                log_url=TextColumn(),
                log_excerpt=TextColumn(),
                comment=TextColumn(),
                valid=BoolColumn(),
                misc=JSONColumn(),
            )),
        ),
        build=dict(
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
                      "    valid,\n"
                      "    misc\n"
                      "FROM builds",
            schema=Table(dict(
                id=TextColumn(),
                checkout_id=TextColumn(),
                origin=TextColumn(),
                start_time=TimestampColumn(),
                duration=FloatColumn(),
                architecture=TextColumn(),
                command=TextColumn(),
                compiler=TextColumn(),
                input_files=JSONColumn(),
                output_files=JSONColumn(),
                config_name=TextColumn(),
                config_url=TextColumn(),
                log_url=TextColumn(),
                log_excerpt=TextColumn(),
                comment=TextColumn(),
                valid=BoolColumn(),
                misc=JSONColumn(),
            )),
        ),
        test=dict(
            statement="SELECT\n"
                      "    id,\n"
                      "    build_id,\n"
                      "    origin,\n"
                      "    path,\n"
                      "    environment_comment,\n"
                      "    environment_misc,\n"
                      "    log_url,\n"
                      "    log_excerpt,\n"
                      "    status,\n"
                      "    waived,\n"
                      "    start_time,\n"
                      "    duration,\n"
                      "    output_files,\n"
                      "    comment,\n"
                      "    misc\n"
                      "FROM tests",
            schema=Table(dict(
                id=TextColumn(),
                build_id=TextColumn(),
                origin=TextColumn(),
                path=TextColumn(),
                environment_comment=TextColumn(),
                environment_misc=JSONColumn(),
                log_url=TextColumn(),
                log_excerpt=TextColumn(),
                status=TextColumn(),
                waived=BoolColumn(),
                start_time=TimestampColumn(),
                duration=FloatColumn(),
                output_files=JSONColumn(),
                comment=TextColumn(),
                misc=JSONColumn(),
            )),
        ),
        bug=dict(
            statement="SELECT\n"
                      "    NULL AS url,\n"
                      "    NULL AS subject,\n"
                      "    NULL AS culprit_code,\n"
                      "    NULL AS culprit_tool,\n"
                      "    NULL AS culprit_harness\n"
                      "WHERE FALSE",
            schema=Table(dict(
                url=TextColumn(),
                subject=TextColumn(),
                culprit_code=BoolColumn(),
                culprit_tool=BoolColumn(),
                culprit_harness=BoolColumn(),
            )),
        ),
        issue=dict(
            statement="SELECT\n"
                      "    NULL AS id,\n"
                      "    NULL AS version,\n"
                      "    NULL AS origin,\n"
                      "    NULL AS report_url,\n"
                      "    NULL AS report_subject,\n"
                      "    NULL AS culprit_code,\n"
                      "    NULL AS culprit_tool,\n"
                      "    NULL AS culprit_harness,\n"
                      "    NULL AS build_valid,\n"
                      "    NULL AS test_status,\n"
                      "    NULL AS comment,\n"
                      "    NULL AS misc\n"
                      "WHERE FALSE",
            schema=Table(dict(
                id=TextColumn(),
                version=IntegerColumn(),
                origin=TextColumn(),
                report_url=TextColumn(),
                report_subject=TextColumn(),
                culprit_code=BoolColumn(),
                culprit_tool=BoolColumn(),
                culprit_harness=BoolColumn(),
                build_valid=BoolColumn(),
                test_status=TextColumn(),
                comment=TextColumn(),
                misc=JSONColumn(),
            )),
        ),
        incident=dict(
            statement="SELECT\n"
                      "    NULL AS id,\n"
                      "    NULL AS origin,\n"
                      "    NULL AS issue_id,\n"
                      "    NULL AS issue_version,\n"
                      "    NULL AS build_id,\n"
                      "    NULL AS test_id,\n"
                      "    NULL AS comment,\n"
                      "    NULL AS misc\n"
                      "WHERE FALSE",
            schema=Table(dict(
                id=TextColumn(),
                origin=TextColumn(),
                issue_id=TextColumn(),
                issue_version=IntegerColumn(),
                build_id=TextColumn(),
                test_id=TextColumn(),
                comment=TextColumn(),
                misc=JSONColumn(),
            )),
        ),
    )

    def init(self):
        """
        Initialize the database.
        The database must be uninitialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in self.TABLES.items():
                try:
                    cursor.execute(table_schema.format_create(table_name))
                except Exception as exc:
                    raise Exception(
                        f"Failed creating table {table_name!r}"
                    ) from exc
            # Create the "first" and "last" aggregations
            # Source: https://wiki.postgresql.org/wiki/First/last_(aggregate)
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE FUNCTION first_agg(anyelement, anyelement)
                RETURNS anyelement
                LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
                'SELECT $1'
            """))
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE FUNCTION last_agg(anyelement, anyelement)
                RETURNS anyelement
                LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
                'SELECT $2';
            """))
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE AGGREGATE first(anyelement) (
                    SFUNC = first_agg,
                    STYPE = anyelement,
                    PARALLEL = safe
                )
            """))
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE AGGREGATE last(anyelement) (
                    SFUNC = last_agg,
                    STYPE = anyelement,
                    PARALLEL = safe
                )
            """))

    def cleanup(self):
        """
        Cleanup (deinitialize) the database, removing all data.
        The database must be initialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute("DROP AGGREGATE IF EXISTS last(anyelement)")
            cursor.execute("DROP AGGREGATE IF EXISTS first(anyelement)")
            cursor.execute(
                "DROP FUNCTION IF EXISTS last_agg(anyelement, anyelement)"
            )
            cursor.execute(
                "DROP FUNCTION IF EXISTS first_agg(anyelement, anyelement)"
            )
            for name in self.TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")

    def empty(self):
        """
        Empty the database, removing all data.
        The database must be initialized.
        """
        with self.conn, self.conn.cursor() as cursor:
            for name, schema in self.TABLES.items():
                cursor.execute(schema.format_delete(name))

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the I/O
            version of the database schema, each containing at most the
            specified number of objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        obj_num = 0
        data = self.io.new()
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in self.TABLES.items():
                obj_list = None
                cursor.execute(table_schema.format_dump(table_name))
                for obj in table_schema.unpack_iter(cursor):
                    if obj_list is None:
                        obj_list = []
                        data[table_name] = obj_list
                    obj_list.append(obj)
                    obj_num += 1
                    if objects_per_report and \
                            obj_num >= objects_per_report:
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

    # We can live with this for now, pylint: disable=too-many-arguments
    def query_iter(self, ids, children, parents, objects_per_report):
        """
        Match and fetch objects from the database, in object number-limited
        chunks.

        Args:
            ids:                A dictionary of object list names, and lists
                                of IDs of objects to match. None means empty
                                dictionary.
            children:           True if children of matched objects should be
                                matched as well.
            parents:            True if parents of matched objects should be
                                matched as well.
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the I/O
            version of the database schema, each containing at most the
            specified number of objects.
        """
        # Calm down, we'll get to it,
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-branches
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        # Build a dictionary of object list (table) names and tuples
        # containing a SELECT statement and the list of its parameters,
        # returning IDs of the objects to fetch.
        obj_list_queries = {}
        for obj_list_name in self.io.graph:
            if not obj_list_name:
                continue
            table_ids = ids.get(obj_list_name, [])
            if table_ids:
                obj_list_queries[obj_list_name] = [
                    "WITH ids(id) AS (VALUES " +
                    ", ".join(["(%s)"] * len(table_ids)) +
                    ") SELECT * FROM ids\n",
                    [*table_ids]
                ]
            else:
                obj_list_queries[obj_list_name] = [
                    "SELECT NULL as id WHERE FALSE\n",
                    []
                ]

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
                        f"UNION\n" \
                        f"SELECT {child_list_name}.{obj_name}_id AS id " \
                        f"FROM {child_list_name} " + \
                        "INNER JOIN (\n" + \
                        textwrap.indent(child_query[0], " " * 4) + \
                        ") AS ids USING(id)\n"
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
                        f"UNION\n" \
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
        with self.conn, self.conn.cursor() as cursor:
            for obj_list_name, query in obj_list_queries.items():
                table_schema = self.TABLES[obj_list_name]
                cursor.execute(
                    f"SELECT {table_schema.columns_list}\n"
                    f"FROM {obj_list_name} INNER JOIN (\n" +
                    textwrap.indent(query[0], " " * 4) +
                    ") AS ids USING(id)\n",
                    query[1]
                )
                obj_list = None
                for obj in table_schema.unpack_iter(cursor):
                    if obj_list is None:
                        obj_list = []
                        data[obj_list_name] = obj_list
                    obj_list.append(obj)
                    obj_num += 1
                    if objects_per_report and \
                            obj_num >= objects_per_report:
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
        Render a pattern for raw OO data into a query.

        Args:
            pattern:    The pattern (instance of kcidb.orm.Pattern) to
                        render.

        Returns:
            The SQL query string and the query parameters.
        """
        assert isinstance(pattern, orm.Pattern)
        obj_type = pattern.obj_type
        type_query_string = cls.OO_QUERIES[obj_type.name]["statement"]
        if pattern.obj_id_set:
            obj_id_fields = obj_type.id_fields
            query_string = \
                f"/* {obj_type.name.capitalize()}s with pattern IDs */\n" + \
                "SELECT obj.* FROM (\n" + \
                textwrap.indent(type_query_string, " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                f"    /* {obj_type.name.capitalize()} pattern IDs */\n" + \
                "    WITH ids(" + \
                ", ".join(obj_id_fields) + \
                ") AS (VALUES\n" + \
                ",\n".join(
                    [
                        "        (" +
                        ", ".join(["%s", ] * len(obj_id_fields)) +
                        ")"
                    ] *
                    len(pattern.obj_id_set)
                ) + \
                "\n    ) SELECT * FROM ids\n" + \
                ") AS ids USING(" + ", ".join(obj_id_fields) + ")"
            query_parameters = [
                obj_id_field
                for obj_id in pattern.obj_id_set
                for obj_id_field in obj_id
            ]
        else:
            query_string = type_query_string
            if pattern.obj_id_set is not None:
                # We cannot represent empty "VALUES"
                query_string += " WHERE FALSE"
            query_parameters = []

        if pattern.base:
            base_query_string, base_query_parameters = \
                cls._oo_query_render(pattern.base)
            base_obj_type = pattern.base.obj_type
            if pattern.child:
                base_relation = "parent"
                column_pairs = list(zip(
                    base_obj_type.children[obj_type.name].ref_fields,
                    base_obj_type.id_fields
                ))
            else:
                base_relation = "child"
                column_pairs = list(zip(
                    obj_type.id_fields,
                    obj_type.children[base_obj_type.name].ref_fields
                ))
                base_query_string = \
                    "SELECT DISTINCT " + \
                    ", ".join(b for o, b in column_pairs) + "\n" \
                    "FROM (\n" + \
                    textwrap.indent(base_query_string, " " * 4) + "\n" + \
                    ") AS non_distinct_base"

            query_string = \
                f"/* {obj_type.name.capitalize()}s with " + \
                f"{base_relation} {base_obj_type.name}s */\n" + \
                "SELECT obj.* FROM (\n" + \
                textwrap.indent(query_string, " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                textwrap.indent(base_query_string, " " * 4) + "\n" + \
                ") AS base ON " + \
                " AND ".join(f"obj.{o} = base.{b}" for o, b in column_pairs)
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
                if pattern.obj_type == obj_type:
                    if obj_type not in obj_type_queries:
                        obj_type_queries[obj_type] = []
                    obj_type_queries[obj_type]. \
                        append(self._oo_query_render(pattern))

        # Execute all the queries for each type
        with self.conn, self.conn.cursor() as cursor:
            objs = {}
            for obj_type, queries in obj_type_queries.items():
                oo_query = self.OO_QUERIES[obj_type.name]
                query_string = "\nUNION\n".join(q[0] for q in queries)
                query_parameters = [p for q in queries for p in q[1]]
                cursor.execute(query_string, query_parameters)
                objs[obj_type.name] = list(
                    oo_query["schema"].unpack_iter(cursor, drop_null=False)
                )

        assert LIGHT_ASSERTS or orm.SCHEMA.is_valid(objs)
        return objs

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the I/O version of the database schema.
        """
        assert self.io.is_compatible_directly(data)
        assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in self.TABLES.items():
                if table_name in data:
                    psycopg2.extras.execute_batch(
                        cursor,
                        table_schema.format_insert(
                            table_name, self.conn.load_prio_db
                        ),
                        table_schema.pack_iter(data[table_name])
                    )
        # Flip priority for the next load to maintain (rough)
        # parity with non-determinism of BigQuery's ANY_VALUE()
        self.conn.load_prio_db = not self.conn.load_prio_db
