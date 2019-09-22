"""kcidb-submit command-line executable"""

import argparse
import sys
import json
from google.cloud import bigquery
from kcidb import db_schema
from kcidb import io_schema


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
    for obj_list_name in db_schema.TABLE_MAP:
        if obj_list_name in data:
            obj_list = data[obj_list_name]
            # Flatten the "misc" fields
            for obj in obj_list:
                if "misc" in obj:
                    obj["misc"] = json.dumps(obj["misc"])
            # Store
            job_config = bigquery.job.LoadJobConfig(
                autodetect=False, schema=db_schema.TABLE_MAP[obj_list_name])
            job = client.load_table_from_json(obj_list,
                                              dataset_ref.table(obj_list_name),
                                              job_config=job_config)
            job.result()
