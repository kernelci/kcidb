"""Kernel CI report database"""

import argparse
import decimal
import json
import sys
import re
import textwrap
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from kcidb.db import schema
from kcidb import io


class IncompatibleSchema(Exception):
    """Database schema is incompatible with latest I/O schema"""

    def __init__(self, db_major, db_minor):
        """
        Initialize the exception.

        Args:
            db_major:   Database schema major version number
            db_minor:   Database schema minor version number
        """
        super().__init__(f"Database schema {db_major}.{db_minor} "
                         f"is incompatible with I/O schema "
                         f"{io.schema.LATEST.major}."
                         f"{io.schema.LATEST.minor}")


class Client:
    """Kernel CI report database client"""

    _LIKE_PATTERN_ESCAPE_RE = re.compile(r"([%_\\])")

    def __init__(self, dataset_name, project_id=None):
        """
        Initialize a Kernel CI report database client.

        Args:
            dataset_name:   The name of the Kernel CI dataset where data is
                            located. The dataset should be located within the
                            specified Google Cloud project.
            project_id:     ID of the Google Cloud project hosting the
                            dataset, or None to use the project from the
                            credentials file point to by
                            GOOGLE_APPLICATION_CREDENTIALS environment
                            variable.
        """
        assert isinstance(dataset_name, str)
        assert project_id is None or isinstance(project_id, str)
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_name)

    def get_schema_version(self):
        """
        Get the version of the I/O schema the dataset schema corresponds to.

        Returns:
            Major version number, minor version number.
        """
        dataset = self.client.get_dataset(self.dataset_ref)
        if "version_major" in dataset.labels and \
           "version_minor" in dataset.labels:
            return int(dataset.labels["version_major"]), \
                int(dataset.labels["version_minor"])
        return io.schema.V1.major, io.schema.V1.minor

    def init(self):
        """
        Initialize the database. The database must be empty.
        """
        for table_name, table_schema in schema.TABLE_MAP.items():
            table_ref = self.dataset_ref.table(table_name)
            table = bigquery.table.Table(table_ref, schema=table_schema)
            self.client.create_table(table)
        dataset = self.client.get_dataset(self.dataset_ref)
        dataset.labels["version_major"] = str(io.schema.LATEST.major)
        dataset.labels["version_minor"] = str(io.schema.LATEST.minor)
        self.client.update_dataset(dataset, ["labels"])

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        """
        for table_name, _ in schema.TABLE_MAP.items():
            table_ref = self.dataset_ref.table(table_name)
            try:
                self.client.delete_table(table_ref)
            except NotFound:
                pass
        dataset = self.client.get_dataset(self.dataset_ref)
        dataset.labels["version_major"] = None
        dataset.labels["version_minor"] = None
        self.client.update_dataset(dataset, ["labels"])

    @staticmethod
    def _unpack_node(node):
        """
        Unpack a retrieved data node (and all its children) to
        the JSON-compatible and schema-complying representation.

        Args:
            node:   The node to unpack.

        Returns:
            The unpacked node.
        """
        if isinstance(node, decimal.Decimal):
            node = float(node)
        elif isinstance(node, datetime):
            node = node.isoformat()
        elif isinstance(node, list):
            for index, value in enumerate(node):
                node[index] = Client._unpack_node(value)
        elif isinstance(node, dict):
            for key, value in list(node.items()):
                if value is None:
                    del node[key]
                elif key == "misc":
                    node[key] = json.loads(value)
                else:
                    node[key] = Client._unpack_node(value)
        return node

    def dump(self):
        """
        Dump all data from the database.

        Returns:
            The JSON data from the database adhering to the latest I/O schema
            version.

        Raises:
            `IncompatibleSchema` if the dataset schema is incompatible with
            the latest I/O schema.
        """
        major, minor = self.get_schema_version()
        if major != io.schema.LATEST.major:
            raise IncompatibleSchema(major, minor)

        data = io.new()
        for obj_list_name in schema.TABLE_MAP:
            job_config = bigquery.job.QueryJobConfig(
                default_dataset=self.dataset_ref)
            query_job = self.client.query(
                f"SELECT * FROM `{obj_list_name}`", job_config=job_config)
            data[obj_list_name] = [
                Client._unpack_node(dict(row.items())) for row in query_job
            ]

        assert io.schema.is_valid_latest(data)
        return data

    @staticmethod
    def escape_like_pattern(string):
        """
        Escape a string for use as a literal in a LIKE pattern.

        Args:
            string: The string to escape.

        Returns:
            The escaped string.
        """
        return Client._LIKE_PATTERN_ESCAPE_RE.sub(r"\\\1", string)

    def query(self, patterns, children=False, parents=False):
        """
        Match and fetch objects from the database.

        Args:
            patterns:   A dictionary of object list names, and lists of LIKE
                        patterns, for IDs of objects to match.
            children:   True if children of matched objects should be matched
                        as well.
            parents:    True if parents of matched objects should be matched
                        as well.

        Returns:
            The JSON data from the database adhering to the latest I/O schema
            version.

        Raises:
            `IncompatibleSchema` if the dataset schema is incompatible with
            the latest I/O schema.
        """
        assert isinstance(patterns, dict)
        assert all(isinstance(k, str) and isinstance(v, list) and
                   all(isinstance(e, str) for e in v)
                   for k, v in patterns.items())

        major, minor = self.get_schema_version()
        if major != io.schema.LATEST.major:
            raise IncompatibleSchema(major, minor)

        # A dictionary of object list names and tuples containing a SELECT
        # statement and the list of its parameters, returning IDs of the
        # objects to fetch.
        obj_list_queries = {
            # JOIN selecting objects with IDs matching the patterns
            obj_list_name: [
                f"SELECT {obj_list_name}.id AS id " \
                f"FROM {obj_list_name} " \
                f"INNER JOIN UNNEST(?) AS id_pattern " \
                f"ON {obj_list_name}.id LIKE id_pattern\n",
                [
                    bigquery.ArrayQueryParameter(
                        None, "STRING", patterns.get(obj_list_name, [])
                    )
                ]
            ]
            for obj_list_name in io.schema.LATEST.tree if obj_list_name
        }

        # Add referenced parents if requested
        if parents:
            def add_parents(obj_list_name):
                """Add parent IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in io.schema.LATEST.tree[obj_list_name]:
                    add_parents(child_list_name)
                    child_query = obj_list_queries[child_list_name]
                    query[0] += \
                        f"UNION DISTINCT\n" \
                        f"SELECT {child_list_name}.{obj_name}_id AS id " \
                        f"FROM {child_list_name} " + \
                        f"WHERE {child_list_name}.id IN (\n" + \
                        textwrap.indent(child_query[0], " " * 4) + \
                        f")\n"
                    query[1] += child_query[1]

            for obj_list_name in io.schema.LATEST.tree[""]:
                add_parents(obj_list_name)

        # Add referenced children if requested
        if children:
            def add_children(obj_list_name):
                """Add child IDs to query results"""
                obj_name = obj_list_name[:-1]
                query = obj_list_queries[obj_list_name]
                for child_list_name in io.schema.LATEST.tree[obj_list_name]:
                    child_query = obj_list_queries[child_list_name]
                    child_query[0] += \
                        f"UNION DISTINCT\n" \
                        f"SELECT {child_list_name}.id AS id " \
                        f"FROM {child_list_name} " + \
                        f"WHERE {child_list_name}.{obj_name}_id IN (\n" + \
                        textwrap.indent(query[0], " " * 4) + \
                        f")\n"
                    child_query[1] += query[1]
                    add_children(child_list_name)

            for obj_list_name in io.schema.LATEST.tree[""]:
                add_children(obj_list_name)

        # Fetch the data
        data = io.new()
        for obj_list_name, query in obj_list_queries.items():
            job_config = bigquery.job.QueryJobConfig(
                query_parameters=query[1],
                default_dataset=self.dataset_ref
            )
            query_job = self.client.query(
                f"SELECT * FROM {obj_list_name} WHERE id IN (\n" +
                query[0] +
                f")\n",
                job_config=job_config
            )
            data[obj_list_name] = [
                Client._unpack_node(dict(row.items())) for row in query_job
            ]

        assert io.schema.is_valid_latest(data)
        return data

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
                node[index] = Client._pack_node(value)
        elif isinstance(node, dict):
            node = node.copy()
            for key, value in list(node.items()):
                # Flatten the "misc" fields
                if key == "misc":
                    node[key] = json.dumps(value)
                else:
                    node[key] = Client._pack_node(value)
        return node

    def load(self, data):
        """
        Load data into the database.

        Args:
            data:   The JSON data to load into the database.
                    Must adhere to a version of I/O schema.

        Raises:
            `IncompatibleSchema` if the dataset schema is incompatible with
            the latest I/O schema.
        """
        assert io.schema.is_valid(data)
        data = io.schema.upgrade(data)

        major, minor = self.get_schema_version()
        if major != io.schema.LATEST.major:
            raise IncompatibleSchema(major, minor)

        for obj_list_name in schema.TABLE_MAP:
            if obj_list_name in data:
                obj_list = Client._pack_node(data[obj_list_name])
                job_config = bigquery.job.LoadJobConfig(
                    autodetect=False,
                    schema=schema.TABLE_MAP[obj_list_name])
                job = self.client.load_table_from_json(
                    obj_list,
                    self.dataset_ref.table(obj_list_name),
                    job_config=job_config)
                try:
                    job.result()
                except BadRequest:
                    raise Exception("".join([
                        f"ERROR: {error['message']}\n" for error in job.errors
                    ]))

    def complement(self, data):
        """
        Given I/O data, return its complement. I.e. the same data, but with
        all objects from the database it references. E.g. for each revision
        load all its builds, for each build load all its tests. And vice
        versa: for each test load its build, and for each build load its
        revision.

        Args:
            data:   The JSON data to complement from the database.
                    Must adhere to a version of I/O schema
                    Will not be modified.

        Returns:
            The complemented JSON data from the database adhering to the
            latest version of I/O schema.
        """
        assert io.schema.is_valid(data)
        data = io.schema.upgrade(data)

        # Generate patterns matching IDs of all supplied objects
        patterns = {}
        for obj_list_name in io.schema.LATEST.tree.keys():
            if obj_list_name:
                patterns[obj_list_name] = list({
                    Client.escape_like_pattern(obj["id"])
                    for obj in data.get(obj_list_name, [])
                })

        # Query the objects along with parents and children
        return self.query(patterns, children=True, parents=True)


def common_main_add_args(parser):
    """
    Add arguments common to all tools to an argparse.ArgumentParser object.

    Args:
        parser: The parser object to add arguments to.

    Returns:
        The parser with the arguments added.
    """
    assert isinstance(parser, argparse.ArgumentParser)
    parser.add_argument(
        '-p', '--project',
        help='ID of the Google Cloud project containing the dataset. '
             'Taken from credentials by default.',
        default=None,
        required=False
    )
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    return parser


def complement_main():
    """Execute the kcidb-db-complement command-line tool"""
    description = \
        'kcidb-db-complement - Complement reports from database'
    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    args = parser.parse_args()
    data = json.load(sys.stdin)
    data = io.schema.upgrade(data, copy=False)
    client = Client(args.dataset, project_id=args.project)
    json.dump(client.complement(data), sys.stdout, indent=4, sort_keys=True)


def dump_main():
    """Execute the kcidb-db-dump command-line tool"""
    description = \
        'kcidb-db-dump - Dump all data from Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    args = parser.parse_args()
    client = Client(args.dataset, project_id=args.project)
    json.dump(client.dump(), sys.stdout, indent=4, sort_keys=True)


def query_main_parse_args(description):
    """
    Parse arguments for a database-querying command-line tool

    Args:
        description:    The program description to use in online help
                        messages.

    Returns:
        An instance of argparse.Namespace containing the parsed arguments.
    """
    assert isinstance(description, str)

    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    parser.add_argument(
        '-r', '--revision-id-like',
        metavar="ID_PATTERN",
        default=[],
        help='LIKE pattern for IDs of revisions to match',
        dest="revision_id_patterns",
        action='append',
    )
    parser.add_argument(
        '-b', '--build-id-like',
        metavar="ID_PATTERN",
        default=[],
        help='LIKE pattern for IDs of builds to match',
        dest="build_id_patterns",
        action='append',
    )
    parser.add_argument(
        '-t', '--test-id-like',
        metavar="ID_PATTERN",
        default=[],
        help='LIKE pattern for IDs of tests to match',
        dest="test_id_patterns",
        action='append',
    )
    parser.add_argument(
        '--parents',
        help='Match parents of matching objects',
        action='store_true'
    )
    parser.add_argument(
        '-c', '--children',
        help='Match children of matching objects',
        action='store_true'
    )
    return parser.parse_args()


def query_main():
    """Execute the kcidb-db-query command-line tool"""
    args = query_main_parse_args(
        "kcidb-db-query - Query objects from Kernel CI report database"
    )
    client = Client(args.dataset, project_id=args.project)
    data = client.query(dict(revisions=args.revision_id_patterns,
                             builds=args.build_id_patterns,
                             tests=args.test_id_patterns),
                        parents=args.parents,
                        children=args.children)
    json.dump(data, sys.stdout, indent=4, sort_keys=True)


def load_main():
    """Execute the kcidb-db-load command-line tool"""
    description = \
        'kcidb-db-load - Load reports into Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    args = parser.parse_args()
    data = json.load(sys.stdin)
    data = io.schema.upgrade(data, copy=False)
    client = Client(args.dataset, project_id=args.project)
    client.load(data)


def init_main():
    """Execute the kcidb-db-init command-line tool"""
    description = 'kcidb-db-init - Initialize a Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    args = parser.parse_args()
    client = Client(args.dataset, project_id=args.project)
    client.init()


def cleanup_main():
    """Execute the kcidb-db-cleanup command-line tool"""
    description = 'kcidb-db-cleanup - Cleanup a Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    common_main_add_args(parser)
    args = parser.parse_args()
    client = Client(args.dataset, project_id=args.project)
    client.cleanup()
