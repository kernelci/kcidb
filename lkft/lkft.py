#!/usr/bin/env python3

import argparse
from google.cloud import bigquery
import datetime
import sys
import squad_client


def transform_lkft_to_kci(test, build_version):
    """
        transform an lkft test record into a kernelci test record

        kci schema: https://api.kernelci.org/schema-test-case.html#post

        IN:
            test: {
                'has_known_issues': False,
                'id': 216221035,
                'known_issues': [],
                'log': '- {"dt": "2019-09-09T17:46:48.674914", "lvl": "target", "msg": '
                       '"<LAVA_SIGNAL_TESTCASE TEST_CASE_ID=fcntl10_64 RESULT=pass>"}\n',
                'metadata': 16029,
                'name': 'ltp-syscalls-tests/fcntl10_64',
                'result': True,
                'short_name': 'fcntl10_64',
                'status': 'pass',
                'suite': 142
            }
            build_version: 'v5.3-rc8'

        OUT:
            {
                'name': 'ltp-syscalls-tests/fcntl10_64',
                'status': 'PASS'
                'vcs_commit': build_vers
            }
    """
    #return {
    #    "name": test["name"],
    #    "status": test[
    #        "status"
    #    ].upper(),  # XXX do all squad statuses map to kci status?
    #    "vcs_commit": build_version,
    #}
    return {
        "name": test["name"],
        "result": test[
            "status"
        ].upper(),  # XXX do all squad statuses map to kci status?
    }

def valid_date_type(arg_date_str):
    """custom argparse *date* type for user dates values given from the command line"""
    try:
        return datetime.datetime.strptime(arg_date_str, "%Y-%m-%d")
    except:
        print(
            "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(
                arg_date_str
            )
        )
        sys.exit(1)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate report of branches tested recently"
    )
    parser.add_argument(
        dest="date",
        type=valid_date_type,
        help='Report on builds that occured since date given (inclusive) "YYYY-MM-DD"',
    )
    args = parser.parse_args()
    date = args.date

    client = bigquery.Client()
    dataset_ref = client.dataset('kernelci01')
    table_ref = dataset_ref.table("tests")

    branches = squad_client.get_projects_by_branch()
    for branch, branch_url in branches.items():
        builds_url = branch_url+"builds/"
        for build in squad_client.Builds(builds_url):
            if date > datetime.datetime.strptime(
                build["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ):
                break
            build_version = build["version"]
            for testrun in squad_client.get_objects(build.get("testruns")):
                data = []
                for index, test in enumerate(squad_client.get_objects(testrun.get("tests"))):
                    data.append(transform_lkft_to_kci(test, build_version))
                    print(transform_lkft_to_kci(test, build_version))

                    if index > 10:
                        print("Bailing out after 10 test results")
                        break
                job = client.load_table_from_json(data, table_ref)
                job.result()



    #build_url = "https://qa-aep rts.linaro.org/api/builds/22006/"  # v5.3-rc8
    #build = squad_client.Build(build_url)
    #build_version = build.build["version"]

