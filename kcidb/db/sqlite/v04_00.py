"""Kernel CI report database - SQLite schema v4.0"""

import random
import textwrap
from functools import reduce
import datetime
import logging
import sqlite3
import kcidb.io as io
import kcidb.orm as orm
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.schematic import \
    Schema as AbstractSchema, \
    Connection as AbstractConnection
from kcidb.db.sqlite.schema import \
    Constraint, Column, BoolColumn, TextColumn, JSONColumn, TimestampColumn


# Module's logger
LOGGER = logging.getLogger(__name__)


class Connection(AbstractConnection):
    """
    Kernel CI SQLite report database connection.
    Exposes SQLite connection interface.
    """

    # Documentation of the connection parameters
    _PARAMS_DOC = textwrap.dedent("""\
        Parameters: <DATABASE>

        <DATABASE>      A path-like object giving the pathname (absolute or
                        relative to the current working directory) of the
                        database file to be opened. Use ":memory:" to create
                        and use an in-memory database.
    """)

    def __init__(self, params, load_prio_db=None):
        """
        Initialize an SQLite connection.

        Args:
            params:         A parameter string describing the database to
                            access. See Connection._PARAMS_DOC for
                            documentation. Cannot be None (must be specified).
            load_prio_db:   If True, prioritize the database values for the
                            first load. If False - prioritize the loaded data.
                            If None, pick the priority at random. Each further
                            load flips the priority.
        """
        assert params is None or isinstance(params, str)
        assert load_prio_db is None or isinstance(params, bool)
        if params is None:
            raise Exception("Parameters must be specified\n\n" +
                            self._PARAMS_DOC)
        super().__init__(params)

        # Create the connection
        self.conn = sqlite3.connect(params)
        self.conn.set_trace_callback(
            lambda s: LOGGER.debug("Executing:\n%s", s)
        )
        if load_prio_db is None:
            self.load_prio_db = bool(random.randint(0, 1))
        else:
            self.load_prio_db = load_prio_db

    def __getattr__(self, name):
        """Retrieve missing attributes from the SQLite connection object"""
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
        # Oh, but sqlite3 connection is, pylint: disable=not-context-manager
        with self:
            cursor = self.cursor()
            try:
                number = 0 if version is None \
                    else int(version[0] * 1000 + version[1] % 1000)
                cursor.execute(f"PRAGMA user_version = {number}")
            finally:
                cursor.close()

    def get_schema_version(self):
        """
        Retrieve the schema version of the connected database, in a separate
        transaction.

        Returns:
            The major and the minor version numbers of the database schema,
            or None, if not initialized.
        """
        # Oh, but sqlite3 connection is, pylint: disable=not-context-manager
        with self:
            cursor = self.cursor()
            try:
                cursor.execute("PRAGMA user_version")
                number = cursor.fetchone()[0]
                if number:
                    return int(number / 1000), int(number % 1000)
                return None
            finally:
                cursor.close()

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
    """SQLite database schema v4.0"""

    # The connection class to use for talking to the database.
    Connection = Connection
    # The schema's version.
    version = (4, 0)
    # The I/O schema the database schema supports
    io = io.schema.V4_0

    # A map of table names and descriptions
    TABLES = dict(
        checkouts=dict(
            columns={
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
                "log_excerpt": TextColumn(),
                "valid": BoolColumn(),
                "misc": JSONColumn(),
            }
        ),
        builds=dict(
            columns={
                "checkout_id": TextColumn(constraint=Constraint.NOT_NULL),
                "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
                "origin": TextColumn(constraint=Constraint.NOT_NULL),
                "comment": TextColumn(),
                "start_time": TimestampColumn(),
                "duration": Column("REAL"),
                "architecture": TextColumn(),
                "command": TextColumn(),
                "compiler": TextColumn(),
                "input_files": JSONColumn(),
                "output_files": JSONColumn(),
                "config_name": TextColumn(),
                "config_url": TextColumn(),
                "log_url": TextColumn(),
                "log_excerpt": TextColumn(),
                "valid": BoolColumn(),
                "misc": JSONColumn(),
            },
        ),
        tests=dict(
            columns={
                "build_id": TextColumn(constraint=Constraint.NOT_NULL),
                "id": TextColumn(constraint=Constraint.PRIMARY_KEY),
                "origin": TextColumn(constraint=Constraint.NOT_NULL),
                "environment.comment": TextColumn(),
                "environment.misc": JSONColumn(),
                "path": TextColumn(),
                "comment": TextColumn(),
                "log_url": TextColumn(),
                "log_excerpt": TextColumn(),
                "status": TextColumn(),
                "waived": BoolColumn(),
                "start_time": TimestampColumn(),
                "duration": Column("REAL"),
                "output_files": JSONColumn(),
                "misc": JSONColumn()
            },
        ),
    )

    # Queries and their columns for each type of raw object-oriented data.
    # Both should have columns in the same order.
    # NOTE: Relying on dictionaries preserving order in Python 3.6+
    OO_QUERIES = dict(
        revision=dict(
            statement="SELECT\n"
                      "   git_commit_hash,\n"
                      "   patchset_hash,\n"
                      "   patchset_files,\n"
                      "   git_commit_name,\n"
                      "   contacts\n"
                      "FROM checkouts\n"
                      "GROUP BY git_commit_hash, patchset_hash",
            columns=dict(
                git_commit_hash=TextColumn(),
                patchset_hash=TextColumn(),
                patchset_files=JSONColumn(),
                git_commit_name=TextColumn(),
                contacts=JSONColumn(),
            ),
        ),
        checkout=dict(
            statement="SELECT\n"
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
            columns=dict(
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
            ),
        ),
        build=dict(
            statement="SELECT\n"
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
            columns=dict(
                id=TextColumn(),
                checkout_id=TextColumn(),
                origin=TextColumn(),
                start_time=TimestampColumn(),
                duration=Column("REAL"),
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
            ),
        ),
        test=dict(
            statement="SELECT\n"
                      "   id,\n"
                      "   build_id,\n"
                      "   origin,\n"
                      "   path,\n"
                      "   \"environment.comment\" AS environment_comment,\n"
                      "   \"environment.misc\" AS environment_misc,\n"
                      "   log_url,\n"
                      "   log_excerpt,\n"
                      "   status,\n"
                      "   waived,\n"
                      "   start_time,\n"
                      "   duration,\n"
                      "   output_files,\n"
                      "   comment,\n"
                      "   misc\n"
                      "FROM tests",
            columns=dict(
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
                duration=Column("REAL"),
                output_files=JSONColumn(),
                comment=TextColumn(),
                misc=JSONColumn(),
            ),
        ),
    )

    def init(self):
        """
        Initialize the database. The database must be empty (uninitialized).
        """
        with self.conn:
            cursor = self.conn.cursor()
            try:
                for name, table in self.TABLES.items():
                    try:
                        cursor.execute(
                            "CREATE TABLE " + name + " (\n" +
                            ",\n".join(
                                '    "' + name + '" ' +
                                column.format_nameless_def()
                                for name, column in table["columns"].items()
                            ) +
                            "\n) WITHOUT ROWID"
                        )
                    except Exception as exc:
                        raise Exception(
                            f"Failed creating table {name!r}"
                        ) from exc
            finally:
                cursor.close()

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized.
        """
        with self.conn:
            cursor = self.conn.cursor()
            try:
                for name in self.TABLES:
                    cursor.execute(f"DROP TABLE {name}")
            finally:
                cursor.close()

    @staticmethod
    def _quote_column_name(name):
        """
        Quote a column name for use in an SQL statement.

        Args:
            name:   A column name to quote.

        Returns:
            The quoted column name.
        """
        return '"' + name + '"'

    @staticmethod
    def _quote_column_names(names):
        """
        Quote a list of column names for use in an SQL statement.

        Args:
            names:  A list of column names to quote.

        Returns:
            The list of quoted column names.
        """
        return [Schema._quote_column_name(n) for n in names]

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
        data = self.io.new()
        with self.conn:
            cursor = self.conn.cursor()
            try:
                for name, table in self.TABLES.items():
                    table_columns = table["columns"]
                    result = cursor.execute(
                        "SELECT " + ", ".join(
                            Schema._quote_column_names(table_columns)
                        ) +
                        " FROM " + name
                    )
                    obj_list = None
                    for columns in result:
                        if obj_list is None:
                            obj_list = []
                            data[name] = obj_list
                        obj_list.append(
                            Schema._unpack_fields(table_columns, columns)
                        )
                        obj_num += 1
                        if objects_per_report and \
                                obj_num >= objects_per_report:
                            assert LIGHT_ASSERTS or \
                                    self.io.is_valid_exactly(data)
                            yield data
                            obj_num = 0
                            data = self.io.new()
                            obj_list = None
            finally:
                cursor.close()

        if obj_num:
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
        for obj_list_name in self.io.tree:
            if not obj_list_name:
                continue
            table_ids = ids.get(obj_list_name, [])
            if table_ids:
                obj_list_queries[obj_list_name] = [
                    "WITH ids(id) AS (VALUES " +
                    ", ".join(["(?)"] * len(table_ids)) +
                    ") SELECT * FROM ids\n",
                    [*table_ids]
                ]
            else:
                obj_list_queries[obj_list_name] = [
                    "SELECT NULL as id WHERE 0\n",
                    []
                ]

        # Add referenced parents if requested
        if parents:
            def add_parents(obj_list_name):
                """Add parent IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in self.io.tree[obj_list_name]:
                    add_parents(child_list_name)
                    child_query = obj_list_queries[child_list_name]
                    query[0] += \
                        f"UNION\n" \
                        f"SELECT {child_list_name}.{obj_name}_id AS id " \
                        f"FROM {child_list_name} " + \
                        "INNER JOIN (\n" + \
                        textwrap.indent(child_query[0], " " * 4) + \
                        ") USING(id)\n"
                    query[1] += child_query[1]

            for obj_list_name in self.io.tree[""]:
                add_parents(obj_list_name)

        # Add referenced children if requested
        if children:
            def add_children(obj_list_name):
                """Add child IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in self.io.tree[obj_list_name]:
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

            for obj_list_name in self.io.tree[""]:
                add_children(obj_list_name)

        # Fetch the data
        obj_num = 0
        data = self.io.new()
        with self.conn:
            cursor = self.conn.cursor()
            try:
                for obj_list_name, query in obj_list_queries.items():
                    table_columns = self.TABLES[obj_list_name]["columns"]
                    query_parameters = query[1]
                    query_string = \
                        "SELECT " + ", ".join(
                            Schema._quote_column_names(table_columns)
                        ) + \
                        f" FROM {obj_list_name} INNER JOIN (\n" + \
                        textwrap.indent(query[0], " " * 4) + \
                        ") USING(id)\n"
                    obj_list = None
                    for columns in cursor.execute(query_string,
                                                  query_parameters):
                        if obj_list is None:
                            obj_list = []
                            data[obj_list_name] = obj_list
                        obj_list.append(
                            Schema._unpack_fields(table_columns, columns)
                        )
                        obj_num += 1
                        if objects_per_report and \
                                obj_num >= objects_per_report:
                            assert LIGHT_ASSERTS or \
                                    self.io.is_valid_exactly(data)
                            yield data
                            obj_num = 0
                            data = self.io.new()
                            obj_list = None
            finally:
                cursor.close()

        if obj_num:
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
            query_string = "SELECT obj.* FROM (\n" + \
                textwrap.indent(type_query_string, " " * 4) + "\n" + \
                ") AS obj INNER JOIN (\n" + \
                "    WITH ids(" + \
                ", ".join(obj_id_fields) + \
                ") AS (VALUES " + \
                ",\n".join(
                    ["    (" + ", ".join("?" * len(obj_id_fields)) + ")"] *
                    len(pattern.obj_id_set)
                ) + \
                ") SELECT * FROM ids\n" + \
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
                query_string += " WHERE 0"
            query_parameters = []

        if pattern.base:
            base_query_string, base_query_parameters = \
                Schema._oo_query_render(pattern.base)
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
                        append(Schema._oo_query_render(pattern))

        # Execute all the queries
        with self.conn:
            cursor = self.conn.cursor()
            try:
                objs = {}
                for obj_type, queries in obj_type_queries.items():
                    query_string = "SELECT obj.* FROM (\n" + \
                        textwrap.indent(
                            self.OO_QUERIES[obj_type.name]["statement"],
                            "    "
                        ) + "\n" + \
                        ") AS obj INNER JOIN (\n" + \
                        "    SELECT DISTINCT " + \
                        ", ".join(obj_type.id_fields) + \
                        " FROM (\n" + \
                        textwrap.indent(
                            "\nUNION ALL\n".join(q[0] for q in queries),
                            " " * 8
                        ) + "\n" + \
                        "    )\n" + \
                        ") AS ids USING(" + ", ".join(obj_type.id_fields) + ")"
                    query_parameters = reduce(lambda x, y: x + y,
                                              (q[1] for q in queries))
                    objs[obj_type.name] = [
                        Schema._unpack_fields(
                            self.OO_QUERIES[obj_type.name]["columns"],
                            columns, drop_null=False
                        )
                        for columns in
                        cursor.execute(query_string, query_parameters)
                    ]
            finally:
                cursor.close()

        assert LIGHT_ASSERTS or orm.SCHEMA.is_valid(objs)
        return objs

    @staticmethod
    def _pack_fields(column_vals, column_defs, pfx, fields):
        """
        Pack a dictionary of (sub-)fields into a dictionary of columns in
        SQLite-compatible representation for a particular table.

        Args:
            column_vals:    The dictionary to output the packed columns into.
            column_defs:    Dictionary of column names and definitions
                            (instances of kcidb.db.sqlite.schema.misc.Column).
            pfx:            The string prefix to add to field names to produce
                            the target column names.
            fields:         The dictionary of fields to pack into columns.

        Returns:
            The dictionary supplied in "column_vals" with newly-packed columns
            added.
        """
        assert isinstance(column_vals, dict)
        assert isinstance(column_defs, dict)
        assert LIGHT_ASSERTS or all(
            isinstance(name, str) and isinstance(column, Column)
            for name, column in column_defs.items()
        )
        assert isinstance(pfx, str)
        assert isinstance(fields, dict)

        for field_name, field_value in fields.items():
            column_name = pfx + field_name
            if column_name in column_defs:
                column_vals[column_name] = \
                    column_defs[column_name].pack(field_value)
            else:
                assert isinstance(field_value, dict), \
                    f"Field {pfx + field_name!r} value is not a dict: " \
                    f"{field_value!r}"
                Schema._pack_fields(column_vals, column_defs,
                                    pfx + field_name + ".", field_value)
        return column_vals

    @staticmethod
    def _unpack_fields(column_defs, column_vals, drop_null=True):
        """
        Unpack a tuple of SQLite table column values into a dictionary of JSON
        fields.

        Args:
            column_defs:    Dictionary of column names and definitions
                            (instances of kcidb.db.sqlite.schema.misc.Column).
            column_vals:    A tuple containing packed column values, in the
                            order the columns are listed in `column_defs`.
            drop_null:      Drop fields with NULL values, if true.
                            Keep them otherwise.

        Returns:
            The dictionary of unpacked fields.
        """
        assert isinstance(column_defs, dict)
        assert LIGHT_ASSERTS or all(
            isinstance(name, str) and isinstance(column, Column)
            for name, column in column_defs.items()
        )
        assert isinstance(column_vals, tuple)

        fields = {}
        for name, column, value in \
                zip(column_defs.keys(), column_defs.values(), column_vals):
            if value is None and drop_null:
                continue
            node = fields
            keys = name.split(".")
            for key in keys[:-1]:
                if key not in node:
                    node[key] = {}
                node = node[key]
            node[keys[-1]] = column.unpack(value)
        return fields

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current version of I/O schema.
        """
        assert LIGHT_ASSERTS or self.io.is_valid_exactly(data)
        with self.conn:
            cursor = self.conn.cursor()
            try:
                for name, table in self.TABLES.items():
                    if name not in data:
                        continue
                    for obj in data[name]:
                        columns = Schema._pack_fields(
                            {}, table["columns"], "", obj
                        )
                        quoted_columns = {}
                        quoted_key_columns = {}
                        quoted_data_columns = {}
                        for column_name, column_value in columns.items():
                            quoted_column_name = \
                                Schema._quote_column_name(column_name)
                            quoted_columns[quoted_column_name] = column_value
                            if table["columns"][column_name].constraint is \
                                    Constraint.PRIMARY_KEY:
                                quoted_key_columns[quoted_column_name] = \
                                    column_value
                            else:
                                quoted_data_columns[quoted_column_name] = \
                                    column_value

                        # TODO: Switch to executemany()
                        # TODO: Switch to UPSERT if we ever upgrade to Python
                        #       with SQLite 3.24.0 or later
                        try:
                            cursor.execute(
                                "INSERT INTO " + name + " " +
                                "(" + ', '.join(quoted_columns) + ')\n' +
                                "VALUES (" +
                                ", ".join("?" * len(columns)) +
                                ")",
                                list(quoted_columns.values())
                            )
                        except sqlite3.IntegrityError:
                            cursor.execute(
                                "UPDATE " + name + " SET\n" + ",\n".join(
                                    "    " + quoted_name +
                                    (' = coalesce(' + quoted_name + ', ?)'
                                     if self.conn.load_prio_db else
                                     ' = coalesce(?, ' + quoted_name + ')')
                                    for quoted_name in quoted_data_columns
                                ) + "\n" +
                                "WHERE " + " AND ".join(
                                    quoted_name + ' == ?'
                                    for quoted_name in quoted_key_columns
                                ),
                                list(quoted_data_columns.values()) +
                                list(quoted_key_columns.values())
                            )
            finally:
                cursor.close()
        # Flip priority for the next load to maintain (rough)
        # parity with non-determinism of BigQuery's ANY_VALUE()
        self.conn.load_prio_db = not self.conn.load_prio_db
