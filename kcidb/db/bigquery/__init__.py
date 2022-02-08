"""Kernel CI report database - BigQuery driver"""

import decimal
import json
import logging
import textwrap
from functools import reduce
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest as GoogleBadRequest
from google.api_core.exceptions import NotFound as GoogleNotFound
import kcidb.io as io
import kcidb.orm
from kcidb.db.bigquery import schema
from kcidb.misc import LIGHT_ASSERTS
from kcidb.db.misc import Driver as AbstractDriver, NotFound

# Module's logger
LOGGER = logging.getLogger(__name__)


class Driver(AbstractDriver):
    """Kernel CI BigQuery report database driver"""

    DOC = textwrap.dedent("""\
        The BigQuery driver allows connection to a Google Cloud project
        and corresponding BigQuery dataset.

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

    # Yes, it's an abstract class, pylint: disable=super-init-not-called
    def __init__(self, params):
        """
        Initialize the BigQuery driver.

        Args:
            params:         A parameter string describing the database to
                            access. See Driver.DOC for documentation.
                            Cannot be None (must be specified).

        Raises:
            NotFound        - the database does not exist,
        """
        assert params is None or isinstance(params, str)
        if params is None:
            raise Exception("Database parameters must be specified\n\n" +
                            Driver.DOC)
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
            self.dataset = self.client.get_dataset(self.dataset_ref)
        except GoogleNotFound as exc:
            raise NotFound(params) from exc

    def _get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.

        Returns:
            Major and minor version numbers,
            or (None, None) if the database is uninitialized.
        """
        if "version_major" in self.dataset.labels and \
           "version_minor" in self.dataset.labels:
            return int(self.dataset.labels["version_major"]), \
                int(self.dataset.labels["version_minor"])
        if set(tli.table_id for tli in
               self.client.list_tables(self.dataset_ref)) == \
           {"revisions", "builds", "tests"}:
            return io.schema.V1.major, io.schema.V1.minor
        return None, None

    def is_initialized(self):
        """
        Check if the database is initialized (not empty).

        Returns:
            True if the database is initialized, False otherwise.
        """
        major, minor = self._get_schema_version()
        return not (major is None and minor is None)

    def get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.
        Assumes the database is initialized.

        Returns:
            Major and minor version numbers.
        """
        major, minor = self._get_schema_version()
        assert not (major is None and minor is None)
        return major, minor

    def init(self):
        """
        Initialize the database.
        The database must be empty (uninitialized).
        """
        # Create tables and corresponding views
        for table_name, table_schema in schema.TABLE_MAP.items():
            # Create raw table with duplicate records
            table_ref = self.dataset_ref.table("_" + table_name)
            table = bigquery.table.Table(table_ref, schema=table_schema)
            self.client.create_table(table)
            # Create a view deduplicating the table records
            view_ref = self.dataset_ref.table(table_name)
            view = bigquery.table.Table(view_ref)
            view.view_query = \
                "SELECT " + \
                ", ".join(
                    f"ANY_VALUE(`{n}`) AS `{n}`" if n != "id" else f"`{n}`"
                    for n in (f.name for f in table_schema)
                ) + \
                f" FROM `{table_ref}` GROUP BY id"
            self.client.create_table(view)
        # Set dataset schema version
        self.dataset.labels["version_major"] = str(io.SCHEMA.major)
        self.dataset.labels["version_minor"] = str(io.SCHEMA.minor)
        self.client.update_dataset(self.dataset, ["labels"])

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        The database must be initialized (not empty).
        """
        for table_name, _ in schema.TABLE_MAP.items():
            view_ref = self.dataset_ref.table(table_name)
            try:
                self.client.delete_table(view_ref)
            except GoogleNotFound:
                pass
            table_ref = self.dataset_ref.table("_" + table_name)
            try:
                self.client.delete_table(table_ref)
            except GoogleNotFound:
                pass
        self.dataset.labels["version_major"] = None
        self.dataset.labels["version_minor"] = None
        self.client.update_dataset(self.dataset, ["labels"])

    def _query_create(self, query_string, query_parameters=None):
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

    def get_last_modified(self):
        """
        Get the time the data in the database was last modified.
        The database must be initialized (not empty).

        Returns:
            The datetime object representing the last modification time.
        """
        return next(iter(self._query_create(
            "SELECT TIMESTAMP_MILLIS(MAX(last_modified_time)) "
            "FROM __TABLES__"
        ).result()))[0]

    @staticmethod
    def _unpack_node(node, drop_null=True):
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
        elif isinstance(node, datetime):
            node = node.isoformat()
        elif isinstance(node, list):
            for index, value in enumerate(node):
                node[index] = Driver._unpack_node(value)
        elif isinstance(node, dict):
            for key, value in list(node.items()):
                if value is None:
                    if drop_null:
                        del node[key]
                elif key == "misc" or key.endswith("_misc"):
                    node[key] = json.loads(value)
                else:
                    node[key] = Driver._unpack_node(value)
        return node

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
        for obj_list_name in schema.TABLE_MAP:
            query_string = f"SELECT * FROM `{obj_list_name}`"
            query_job = self._query_create(query_string)
            obj_list = None
            for row in query_job:
                if obj_list is None:
                    obj_list = []
                    data[obj_list_name] = obj_list
                obj_list.append(Driver._unpack_node(dict(row.items())))
                obj_num += 1
                if objects_per_report and obj_num >= objects_per_report:
                    assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
                    yield data
                    obj_num = 0
                    data = io.SCHEMA.new()
                    obj_list = None

        if obj_num:
            assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
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
            An iterator returning report JSON data adhering to the current I/O
            schema version, each containing at most the specified number of
            objects.
        """
        # Calm down, we'll get to it,
        # pylint: disable=too-many-locals,too-many-statements
        assert isinstance(ids, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in ids.items())
        assert isinstance(objects_per_report, int)
        assert objects_per_report >= 0

        # A dictionary of object list names and three-element lists,
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
            for obj_list_name in io.SCHEMA.tree if obj_list_name
        }

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
                        f"UNION DISTINCT\n" \
                        f"SELECT {child_list_name}.{obj_name}_id AS id " \
                        f"FROM {child_list_name} " + \
                        "INNER JOIN (\n" + \
                        textwrap.indent(child_query[0], " " * 4) + \
                        ") USING(id)\n"
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

            for obj_list_name in io.SCHEMA.tree[""]:
                add_children(obj_list_name)

        # Fetch the data
        obj_num = 0
        data = io.SCHEMA.new()
        for obj_list_name, query in obj_list_queries.items():
            query_parameters = query[1]
            query_string = \
                f"SELECT * FROM `{obj_list_name}` INNER JOIN (\n" + \
                textwrap.indent(query[0], " " * 4) + \
                ") USING(id)\n"
            query_job = self._query_create(query_string, query_parameters)
            obj_list = None
            for row in query_job:
                if obj_list is None:
                    obj_list = []
                    data[obj_list_name] = obj_list
                obj_list.append(Driver._unpack_node(dict(row.items())))
                obj_num += 1
                if objects_per_report and obj_num >= objects_per_report:
                    assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)
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
        Render a pattern matching raw OO data into a query.

        Args:
            pattern:    The pattern (instance of kcidb.orm.Pattern) to
                        render.

        Returns:
            The SQL query string and the query parameters.
        """
        assert isinstance(pattern, kcidb.orm.Pattern)
        obj_type = pattern.obj_type
        type_query_string = schema.OO_QUERIES[obj_type.name]
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
                Driver._oo_query_render(pattern.base)
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
        assert all(isinstance(r, kcidb.orm.Pattern) for r in pattern_set)

        # Render all queries for each type
        obj_type_queries = {}
        for obj_type in kcidb.orm.SCHEMA.types.values():
            for pattern in pattern_set:
                # TODO: Avoid adding the same patterns multiple times
                if pattern.obj_type == obj_type:
                    if obj_type not in obj_type_queries:
                        obj_type_queries[obj_type] = []
                    obj_type_queries[obj_type]. \
                        append(Driver._oo_query_render(pattern))

        # Execute all the queries
        objs = {}
        for obj_type, queries in obj_type_queries.items():
            # Workaround lack of equality operation for array columns
            # required for "UNION DISTINCT"
            query_string = "SELECT obj.* FROM (\n" + \
                textwrap.indent(schema.OO_QUERIES[obj_type.name],
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
            job = self._query_create(query_string, query_parameters)
            objs[obj_type.name] = [
                Driver._unpack_node(dict(row.items()), drop_null=False)
                for row in job.result()
            ]

        assert LIGHT_ASSERTS or kcidb.orm.SCHEMA.is_valid(objs)
        return objs

    @staticmethod
    def _pack_node(node):
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
                node[index] = Driver._pack_node(value)
        elif isinstance(node, dict):
            node = node.copy()
            for key, value in list(node.items()):
                # Flatten the "misc" fields
                if key == "misc":
                    node[key] = json.dumps(value)
                else:
                    node[key] = Driver._pack_node(value)
        return node

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to the current version of I/O schema.
        """
        assert LIGHT_ASSERTS or io.SCHEMA.is_valid_exactly(data)

        # Load the data
        for obj_list_name, table_schema in schema.TABLE_MAP.items():
            if obj_list_name in data:
                obj_list = Driver._pack_node(data[obj_list_name])
                if not LIGHT_ASSERTS:
                    schema.validate_json_obj_list(table_schema, obj_list)
                job_config = bigquery.job.LoadJobConfig(autodetect=False,
                                                        schema=table_schema)
                job = self.client.load_table_from_json(
                    obj_list,
                    self.dataset_ref.table("_" + obj_list_name),
                    job_config=job_config)
                try:
                    job.result()
                except GoogleBadRequest as exc:
                    raise Exception("".join([
                        f"ERROR: {error['message']}\n" for error in job.errors
                    ])) from exc
