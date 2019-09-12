#!/usr/bin/env python3

import argparse
import datetime
import os
import sys
from urllib.parse import urljoin

import squad_client  # noqa: E402


def transform_lkft_to_kci(test, build_version):
    '''
        transform an lkft test record into a kernelci test record

        kci schema: https://api.kernelci.org/schema-test-case.html#post

        IN:
            {
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

        OUT:
            {
                'name': 'ltp-syscalls-tests/fcntl10_64',
                'status': 'PASS'
            }
    '''
    return {
        'name': test['name'],
        'status': test['status'].upper(), # XXX do all squad statuses map to kci status?
        'vcs_commit': build_version,
    }


if __name__ == "__main__":

    build_url = "https://qa-reports.linaro.org/api/builds/22006/" # v5.3-rc8
    build = squad_client.Build(build_url)
    build_version = build.build['version']

    for testrun in squad_client.get_objects(build.build.get('testruns')):
        for test in squad_client.get_objects(testrun.get('tests')):
            print(transform_lkft_to_kci(test, build_version))
            # {'id': 216221035, 'name': 'ltp-syscalls-tests/fcntl10_64', 'short_name': 'fcntl10_64', 'status': 'pass', 'result': True, 'log': '- {"dt": "2019-09-09T17:46:48.674914", "lvl": "target", "msg": "<LAVA_SIGNAL_TESTCASE TEST_CASE_ID=fcntl10_64 RESULT=pass>"}\n', 'has_known_issues': False, 'suite': 142, 'metadata': 16029, 'known_issues': []}

    #status = squad_client.get_objects(build["status"], expect_one=True)
    #test_run_count += status.get("test_runs_total", 0)
