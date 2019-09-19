"""kcidb-submit command-line executable"""

import argparse
import time
import sys
import json
from google.cloud import bigquery
from kcidb import db_schema
from kcidb import io_schema


def generate_id():
    """
    Generate a pseudo-unique increasing ID.

    Returns:
        A pseudo-unique integer ID.
    """
    return int(time.time()) * 1000000000 + time.time_ns()


def main():
    """Run the executable"""
    description = 'kcidb-submit - Submit test results to kernelci.org database'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        '-d', '--dataset',
        help='Dataset name',
        required=True
    )
    args = parser.parse_args()

    data = json.load(sys.stdin)
    io_schema.validate(data)

    client = bigquery.Client()
    dataset_ref = client.dataset(args.dataset)
    for obj_name in ("revision", "build", "test"):
        if obj_name + "_list" in data:
            for obj in data[obj_name + "_list"]:
                obj["id"] = generate_id()
            table_name = obj_name + "s"
            property_name = obj_name + "_list"
            job_config = bigquery.job.LoadJobConfig(
                autodetect=False, schema=db_schema.TABLE_MAP[table_name])
            table_ref = dataset_ref.table(table_name)
            job = client.load_table_from_json(data[property_name],
                                              table_ref, job_config=job_config)
            job.result()
