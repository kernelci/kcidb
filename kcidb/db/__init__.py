"""Kernel CI report database"""

import argparse
import decimal
import json
import sys
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from kcidb.db import schema
from kcidb import io


class Client:
    """Kernel CI report database client"""

    def __init__(self, dataset_name):
        """
        Initialize a Kernel CI report database client.

        Args:
            dataset_name:   The name of the Kernel CI dataset. The dataset
                            should be located within the Google Cloud project
                            specified in the credentials file pointed to by
                            GOOGLE_APPLICATION_CREDENTIALS environment
                            variable.
        """
        assert isinstance(dataset_name, str)
        self.client = bigquery.Client()
        self.dataset_ref = self.client.dataset(dataset_name)

    def init(self):
        """
        Initialize the database. The database must be empty.
        """
        for table_name, table_schema in schema.TABLE_MAP.items():
            table_ref = self.dataset_ref.table(table_name)
            table = bigquery.table.Table(table_ref, schema=table_schema)
            self.client.create_table(table)

    def cleanup(self):
        """
        Cleanup (empty) the database, removing all data.
        """
        for table_name, _ in schema.TABLE_MAP.items():
            table_ref = self.dataset_ref.table(table_name)
            self.client.delete_table(table_ref)

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

    def query(self):
        """
        Query data from the database.

        Returns:
            The JSON data from the database adhering to the I/O schema
            (kcidb.io.schema.JSON).
        """

        data = dict(version=dict(major=io.schema.JSON_VERSION_MAJOR,
                                 minor=io.schema.JSON_VERSION_MINOR))
        for obj_list_name in schema.TABLE_MAP:
            job_config = bigquery.job.QueryJobConfig(
                default_dataset=self.dataset_ref)
            query_job = self.client.query(
                f"SELECT * FROM `{obj_list_name}`", job_config=job_config)
            data[obj_list_name] = [
                Client._unpack_node(dict(row.items())) for row in query_job
            ]

        io.schema.validate(data)

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
                    Must adhere to the I/O schema (kcidb.io.schema.JSON).
        """
        io.schema.validate(data)
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

    @staticmethod
    def _get_ids_query(data, obj_list_name):
        """
        Generate a query string and parameters retrieving IDs of specific
        objects referenced by the supplied data, both directly and as parents.

        Args:
            data:           The data to get objects from.
            obj_list_name:  Plural name of the object type (table name) to
                            query IDs for.

        Returns:
            The query string and a list of (positional) parameters for it, or
            an empty string and an empty list if there were no object IDs
            referenced in the data.
            If the query string is returned, the query would produce two
            columns: "origin" and "origin_id".
        """
        io.schema.validate(data)
        assert isinstance(obj_list_name, str)
        assert obj_list_name.endswith("s")
        obj_name = obj_list_name[:-1]

        query_string = ""
        query_params = []

        for child_list_name in schema.TABLE_CHILDREN_MAP[obj_list_name]:
            child_query_string, child_query_params = \
                Client._get_ids_query(data, child_list_name)
            assert bool(child_query_string) == bool(child_query_params)
            if child_query_string:
                if query_string:
                    query_string += "UNION DISTINCT\n"
                query_string += \
                    f"SELECT " \
                    f"table.{obj_name}_origin as origin, " \
                    f"table.{obj_name}_origin_id as origin_id " \
                    f"FROM {child_list_name} as table " \
                    f"INNER JOIN ({child_query_string}) as ids " \
                    f"ON table.origin = ids.origin AND " \
                    f"   table.origin_id = ids.origin_id\n"
                query_params += child_query_params
        # Workaround client library choking on empty array parameters
        if data.get(obj_list_name, []):
            if query_string:
                query_string += "UNION DISTINCT\n"
            query_string += \
                "SELECT * FROM UNNEST(?)\n"
            query_params += [
                bigquery.ArrayQueryParameter(
                    None,
                    "STRUCT",
                    [
                        bigquery.StructQueryParameter(
                            None,
                            bigquery.ScalarQueryParameter(
                                "origin", "STRING", obj["origin"]),
                            bigquery.ScalarQueryParameter(
                                "origin_id", "STRING", obj["origin_id"])
                        )
                        for obj in data.get(obj_list_name, [])
                    ]
                )
            ]
        assert bool(query_string) == bool(query_params)
        return query_string, query_params

    def _query_tree(self, data, obj_list_name, join_string, join_params):
        """
        Retrieve objects of particular type selected by the specified JOIN
        clause string and parameters, as well as their children.

        Args:
            data:           The I/O-schema data to put retrieved objects
                            into.
            obj_list_name:  Plural name of the type of object (table name)
                            to retrieve
            join_string:    The JOIN clause string to limit the query with.
            join_params:    The parameters for the JOIN clause.
        """
        io.schema.validate(data)

        assert isinstance(obj_list_name, str)
        assert obj_list_name.endswith("s")
        assert isinstance(join_string, str)
        assert isinstance(join_params, list)

        obj_name = obj_list_name[:-1]

        query_string = \
            f"SELECT {obj_list_name}.* FROM {obj_list_name}\n{join_string}"
        job_config = bigquery.job.QueryJobConfig(
            query_parameters=join_params,
            default_dataset=self.dataset_ref
        )
        query_job = self.client.query(query_string, job_config=job_config)
        data[obj_list_name] = [
            Client._unpack_node(dict(row.items())) for row in query_job
        ]

        for child_list_name in schema.TABLE_CHILDREN_MAP[obj_list_name]:
            child_join_string = \
               f"INNER JOIN {obj_list_name} " \
               f"ON {child_list_name}.{obj_name}_origin = " \
               f"   {obj_list_name}.origin AND " \
               f"   {child_list_name}.{obj_name}_origin_id = " \
               f"   {obj_list_name}.origin_id\n" \
               f"{join_string}"
            self._query_tree(data, child_list_name,
                             child_join_string, join_params)

        io.schema.validate(data)

    def complement(self, data):
        """
        Given I/O data, return its complement. I.e. the same data, but with
        all objects from the database it references. E.g. for each revision
        load all its builds, for each build load all its tests. And vice
        versa: for each test load its build, and for each build load its
        revision.

        Args:
            data:   The JSON data to complement from the database.
                    Must adhere to the I/O schema (kcidb.io.schema.JSON).
                    Will not be modified.

        Returns:
            The complemented JSON data from the database adhering to the I/O
            schema (kcidb.io.schema.JSON).
        """
        io.schema.validate(data)

        complement = dict(version=dict(major=io.schema.JSON_VERSION_MAJOR,
                                       minor=io.schema.JSON_VERSION_MINOR))
        # For each top-level table
        for obj_list_name in schema.TABLE_CHILDREN_MAP[""]:
            # Get complement IDs
            query_string, query_params = \
                Client._get_ids_query(data, obj_list_name)
            assert bool(query_string) == bool(query_params)
            if not query_string:
                continue
            job_config = bigquery.job.QueryJobConfig(
                query_parameters=query_params,
                default_dataset=self.dataset_ref
            )
            query_job = self.client.query(query_string, job_config=job_config)
            result = query_job.result()

            # Workaround client library choking on empty array parameters
            if result.total_rows:
                # Get object tree starting with complement IDs
                join_string = \
                    f"INNER JOIN UNNEST(?) as ids " \
                    f"ON {obj_list_name}.origin = ids.origin AND " \
                    f"   {obj_list_name}.origin_id = ids.origin_id\n"
                join_params = [
                    bigquery.ArrayQueryParameter(
                        None,
                        "STRUCT",
                        [
                            bigquery.StructQueryParameter(
                                None,
                                bigquery.ScalarQueryParameter(
                                    "origin", "STRING", row.origin),
                                bigquery.ScalarQueryParameter(
                                    "origin_id", "STRING", row.origin_id)
                            )
                            for row in result
                        ]
                    )
                ]
                self._query_tree(complement, obj_list_name,
                                 join_string, join_params)

        return complement


def complement_main():
    """Execute the kcidb-db-complement command-line tool"""
    description = \
        'kcidb-db-complement - Complement reports from database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    data = json.load(sys.stdin)
    io.schema.validate(data)
    client = Client(args.dataset)
    json.dump(client.complement(data), sys.stdout, indent=4, sort_keys=True)


def query_main():
    """Execute the kcidb-db-query command-line tool"""
    description = \
        'kcidb-db-query - Query reports from Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    client = Client(args.dataset)
    json.dump(client.query(), sys.stdout, indent=4, sort_keys=True)


def load_main():
    """Execute the kcidb-db-load command-line tool"""
    description = \
        'kcidb-db-load - Load reports into Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    data = json.load(sys.stdin)
    io.schema.validate(data)
    client = Client(args.dataset)
    client.load(data)


def init_main():
    """Execute the kcidb-db-init command-line tool"""
    description = 'kcidb-db-init - Initialize a Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    client = Client(args.dataset)
    client.init()


def cleanup_main():
    """Execute the kcidb-db-cleanup command-line tool"""
    description = 'kcidb-db-cleanup - Cleanup a Kernel CI report database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()
    client = Client(args.dataset)
    client.cleanup()
