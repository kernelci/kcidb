"""kcidb-init command-line executable"""

import argparse
from google.cloud import bigquery
from kcidb import db_schema


def main():
    """Run the executable"""
    description = 'kcidb-init - Initialize a kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()

    client = bigquery.Client()
    dataset_ref = client.dataset(args.dataset)

    for table_name, table_schema in db_schema.table_map.items():
        table_ref = dataset_ref.table(table_name)
        table = bigquery.table.Table(table_ref, schema=table_schema)
        client.create_table(table)
