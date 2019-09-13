"""kcidb-cleanup command-line executable"""

import argparse
from google.cloud import bigquery
from kcidb import schema

def main():
    """Run the executable"""
    description = 'kcidb-cleanup - Cleanup a kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()

    client = bigquery.Client()
    dataset_ref = client.dataset(args.dataset)

    for table_name, _ in schema.table_map.items():
        table_ref = dataset_ref.table(table_name)
        client.delete_table(table_ref)
