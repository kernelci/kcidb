"""Kernel CI report database - PostgreSQL driver"""

import random
import logging
import textwrap
import datetime
import psycopg2
import psycopg2.extras
import psycopg2.errors
import kcidb.io as io
import kcidb.orm
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.postgresql import schema
from kcidb.db.misc import Driver as AbstractDriver

# Module's logger
LOGGER = logging.getLogger(__name__)


class Driver(AbstractDriver):
    """Kernel CI PostgreSQL report database driver"""

    DOC = textwrap.dedent("""\
        The PostgreSQL driver allows connection to a PostgreSQL database.

        Parameters: <CONNECTION>

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
    """)

    # Yes, parent is an abstract class, pylint: disable=super-init-not-called
    def __init__(self, params, load_prio_db=None):
        """
        Initialize the PostgreSQL driver.

        Args:
            params:         A parameter string describing the database to
                            access. See Driver.DOC for documentation.
                            Cannot be None (must be specified).
            load_prio_db:   If True, prioritize the database values for the
                            first load. If False - prioritize the loaded data.
                            If None, pick the priority at random. Each further
                            load flips the priority.

        """
        assert params is None or isinstance(params, str)
        if params is None:
            raise Exception("Database parameters must be specified\n\n" +
                            Driver.DOC)
        # Create the connection
        self.conn = psycopg2.connect(
            params,
            connection_factory=psycopg2.extras.LoggingConnection
        )
        # Specify the logger to the LoggingConnection
        # It logs with DEBUG level, judging from the source (but not the docs)
        self.conn.initialize(LOGGER)
        if load_prio_db is None:
            self.load_prio_db = bool(random.randint(0, 1))
        else:
            self.load_prio_db = load_prio_db

    def _get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.

        Returns:
            Major and minor version numbers, or (0, 0), if the dataset
            is not initialized.
        """
        try:
            with self.conn, self.conn.cursor() as cursor:
                cursor.execute("SELECT get_version()")
                version = cursor.fetchone()[0]
                return int(version / 1000), int(version % 1000)
        # It's auto-generated, pylint: disable=no-member
        except psycopg2.errors.UndefinedFunction:
            return 0, 0

    def is_initialized(self):
        """
        Check if the database is initialized (not empty).

        Returns:
            True if the database is initialized, False otherwise.
        """
        major, minor = self._get_schema_version()
        return not (major == 0 and minor == 0)

    def get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.
        Assumes the database is initialized.

        Returns:
            Major and minor version numbers.
        """
        major, minor = self._get_schema_version()
        assert not (major == 0 and minor == 0)
        return major, minor

    def init(self):
        """
        Initialize the database.
        The database must be empty (uninitialized).
        """
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in schema.TABLES.items():
                try:
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS " + table_name +
                        " (\n" +
                        textwrap.indent(table_schema.columns_def, " " * 4) +
                        "\n)"
                    )
                except Exception as exc:
                    raise Exception(
                        f"Failed creating table {table_name!r}"
                    ) from exc
            version = int(io.SCHEMA.major * 1000 +
                          io.SCHEMA.minor % 1000)
            # Create the "first" aggregation
            # Source: https://wiki.postgresql.org/wiki/First/last_(aggregate)
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE FUNCTION first_agg(anyelement, anyelement)
                RETURNS anyelement
                LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
                'SELECT $1'
            """))
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE AGGREGATE first(anyelement) (
                    SFUNC = public.first_agg,
                    STYPE = anyelement,
                    PARALLEL = safe
                )
            """))
            cursor.execute(textwrap.dedent("""\
                CREATE OR REPLACE FUNCTION get_version()
                RETURNS integer
                LANGUAGE SQL
                IMMUTABLE
                RETURN %s
            """), (version, ))

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized (not empty).
        """
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute("DROP FUNCTION IF EXISTS get_version()")
            cursor.execute("DROP AGGREGATE IF EXISTS first(anyelement)")
            cursor.execute(
                "DROP FUNCTION IF EXISTS first_agg(anyelement, anyelement)"
            )
            for name in schema.TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")

    def get_last_modified(self):
        """
        Get the time the data in the database was last modified.
        The database must be initialized (not empty).

        Returns:
            The datetime object representing the last modification time.
        """
        return datetime.datetime.min

    def dump_iter(self, objects_per_report):
        """
        Dump all data from the database in object number-limited chunks.

        Args:
            objects_per_report: An integer number of objects per each returned
                                report data, or zero for no limit.

        Returns:
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        obj_num = 0
        data = io.SCHEMA.new()
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in schema.TABLES.items():
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
                        assert LIGHT_ASSERTS or \
                                io.SCHEMA.is_valid_exactly(data)
                        yield data
                        obj_num = 0
                        data = io.SCHEMA.new()
                        obj_list = None

        if obj_num:
            assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
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
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
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
        for obj_list_name in io.SCHEMA.tree:
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
                for child_list_name in io.SCHEMA.tree[obj_list_name]:
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

            for obj_list_name in io.SCHEMA.tree[""]:
                add_parents(obj_list_name)

        # Add referenced children if requested
        if children:
            def add_children(obj_list_name):
                """Add child IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in io.SCHEMA.tree[obj_list_name]:
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

            for obj_list_name in io.SCHEMA.tree[""]:
                add_children(obj_list_name)

        # Fetch the data
        obj_num = 0
        data = io.SCHEMA.new()
        with self.conn, self.conn.cursor() as cursor:
            for obj_list_name, query in obj_list_queries.items():
                table_schema = schema.TABLES[obj_list_name]
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
                        assert LIGHT_ASSERTS or \
                                io.SCHEMA.is_valid_exactly(data)
                        yield data
                        obj_num = 0
                        data = io.SCHEMA.new()
                        obj_list = None

        if obj_num:
            assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
            yield data

    @staticmethod
    def _oo_query_render(pattern):
        """
        Render a pattern for raw OO data into a query.

        Args:
            pattern:    The pattern (instance of kcidb.orm.Pattern) to
                        render.

        Returns:
            The SQL query string and the query parameters.
        """
        assert isinstance(pattern, kcidb.orm.Pattern)
        obj_type = pattern.obj_type
        type_query_string = schema.OO_QUERIES[obj_type.name]["statement"]
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
                Driver._oo_query_render(pattern.base)
            base_obj_type = pattern.base.obj_type
            if pattern.child:
                base_relation = "parent"
                column_pairs = zip(
                    base_obj_type.children[obj_type.name].ref_fields,
                    base_obj_type.id_fields
                )
            else:
                base_relation = "child"
                column_pairs = zip(
                    obj_type.id_fields,
                    obj_type.children[base_obj_type.name].ref_fields
                )

            query_string = \
                f"/* {obj_type.name.capitalize()}s with " + \
                f"{base_relation} {base_obj_type.name}s */\n" + \
                "SELECT obj.* FROM (\n" + \
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
        assert all(isinstance(r, kcidb.orm.Pattern) for r in pattern_set)

        # Render all queries for each type
        obj_type_queries = {}
        for obj_type in kcidb.orm.SCHEMA.types.values():
            for pattern in pattern_set:
                if pattern.obj_type == obj_type:
                    if obj_type not in obj_type_queries:
                        obj_type_queries[obj_type] = []
                    obj_type_queries[obj_type]. \
                        append(Driver._oo_query_render(pattern))

        # Execute all the queries for each type
        with self.conn, self.conn.cursor() as cursor:
            objs = {}
            for obj_type, queries in obj_type_queries.items():
                oo_query = schema.OO_QUERIES[obj_type.name]
                query_string = "\nUNION\n".join(q[0] for q in queries)
                query_parameters = [p for q in queries for p in q[1]]
                cursor.execute(query_string, query_parameters)
                objs[obj_type.name] = list(
                    oo_query["schema"].unpack_iter(cursor, drop_null=False)
                )

        assert LIGHT_ASSERTS or kcidb.orm.SCHEMA.is_valid(objs)
        return objs

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current version of I/O schema.
        """
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
        with self.conn, self.conn.cursor() as cursor:
            for table_name, table_schema in schema.TABLES.items():
                if table_name in data:
                    psycopg2.extras.execute_batch(
                        cursor,
                        table_schema.format_insert(
                            table_name, self.load_prio_db
                        ),
                        table_schema.pack_iter(data[table_name])
                    )
        # Flip priority for the next load to maintain (rough)
        # parity with non-determinism of BigQuery's ANY_VALUE()
        self.load_prio_db = not self.load_prio_db
