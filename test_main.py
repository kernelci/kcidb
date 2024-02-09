"""main.py tests"""

import os
import subprocess
import unittest
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from importlib import import_module
from urllib.parse import quote
import time
import yaml
import requests
import kcidb


@unittest.skipIf(os.environ.get("KCIDB_DEPLOYMENT"), "local-only")
def test_google_credentials_are_not_specified():
    """Check Google Application credentials are not specified"""
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") is None, \
        "Local tests must run without " \
        "GOOGLE_APPLICATION_CREDENTIALS " \
        "environment variable"


def test_import():
    """Check main.py can be loaded"""
    # Load deployment environment variables
    file_dir = os.path.dirname(os.path.abspath(__file__))
    cloud_path = os.path.join(file_dir, "cloud")
    env = yaml.safe_load(
        subprocess.check_output([
            cloud_path,
            "env", "kernelci-production", "",
            "--log-level=DEBUG"
        ])
    )
    env["GCP_PROJECT"] = "TEST_PROJECT"

    orig_env = dict(os.environ)
    try:
        os.environ.update(env)
        import_module("main")
    finally:
        os.environ.clear()
        os.environ.update(orig_env)


def check_url_in_cache(url):
    """Check whether the URL is sourced from storage or not."""
    url_encoded = quote(url)
    cache_redirector_url = os.environ["KCIDB_CACHE_REDIRECTOR_URL"]
    try:
        response = requests.get(
            f"{cache_redirector_url}?{url_encoded}",
            timeout=10,   # Time in secs
            allow_redirects=False
        )
    except requests.exceptions.Timeout:
        return False
    if response.status_code == 302:
        # Check if the redirect URL matches the blob storage URL pattern
        if (
            response.headers.get("Location", "")
            .startswith('https://storage.googleapis.com/')
        ):
            return True
    return False


def test_url_caching(empty_deployment):
    """kcidb cache client workflow test"""

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    client = kcidb.Client(
        project_id=os.environ["GCP_PROJECT"],
        topic_name=os.environ["KCIDB_LOAD_QUEUE_TOPIC"]
    )

    data = {
        "version": {
            "major": 4,
            "minor": 0
        },
        "checkouts": [
            {
                "id": "_:1",
                "origin": "_",
                "log_url":
                    "https://github.com/kernelci/kcidb/blob/main"
                    "/.github/workflows/deploy.yml?padding=4821",
                "patchset_files": [
                    {
                        "name": "file",
                        "url":
                            "https://github.com/kernelci/kcidb/"
                            "blob/main/doc/installation.md?padding=1505"
                    }
                ],
            },
        ],
        "builds": [
            {
                "id": "_:1",
                "origin": "_",
                "input_files": [
                    {
                        "name": "kernel_image1",
                        "url":
                            "https://github.com/kernelci/kcidb/blob/"
                            "main/doc/_index.md?padding=7230"
                    }
                ],
                "log_url":
                    "https://github.com/kernelci/kcidb/blob/main"
                    "/.pylintrc?padding=247",
                "config_url":
                    "https://github.com/kernelci/kcidb/blob/main"
                    "/.gitignore?padding=547",
                "output_files": [
                    {
                        "name": "kernel_image",
                        "url":
                            "https://github.com/kernelci/kcidb/"
                            "blob/main/Dockerfile"
                    },
                    {
                        "name": "kernel",
                        "url": "https://kernelcdn.kernel.org/pub/linux/"
                    }
                ],
                "checkout_id": "_:1",
            },
        ],
        "tests": [
            {
                "build_id": "kernelci:kernelci.org:64147283e6021132258c86c0",
                "id": "_:1",
                "origin": "_",
                "log_url":
                    "https://cdn.kernel.org/pub/linux/"
                    "kernel/v6.x/linux-6.4.11.tar.xz",
            },
            {
                "build_id": "kernelci:kernelci.org:64147283e6021132258c86c0",
                "id": "_:1",
                "origin": "_",
                "output_files": [
                    {
                        "name": "x86_64_4_console.log",
                        "url":
                            "https://github.com/kernelci/kcidb/"
                            "blob/main/setup.py?padding=4673"
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                        "url":
                            "https://github.com/kernelci/kcidb/blob"
                            "/main/requirements.txt?padding=1649"
                    },
                ],
                "environment": {
                    "comment": "meson-s905d-p230 in lab-baylibre",
                    "misc": {
                        "rootfs_url":
                            "https://github.com/kernelci/kcidb/tree/main/"
                            "kcidb/?padding=4165"
                    }
                },
            },
        ],
    }

    # Submit data to submission queue
    client.submit(data)

    # Trigger a submission queue pull
    kcidb.mq.JSONPublisher(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_LOAD_QUEUE_TRIGGER_TOPIC"]
    ).publish({})

    current_time = time.time()
    deadline_time = current_time + 300  # 5 minutes
    retry_interval = 5  # seconds

    # URLs to check, as they should be cached
    urls_expected = {
        "https://github.com/kernelci/kcidb/blob/main/setup.py?padding=4673",
        "https://github.com/kernelci/kcidb/blob/main/requirements.txt?"
        "padding=1649",
        "https://github.com/kernelci/kcidb/blob/main/.gitignore?"
        "padding=547",
        "https://github.com/kernelci/kcidb/blob/main/.pylintrc?padding=247",
        "https://github.com/kernelci/kcidb/blob/main/doc/_index.md?"
        "padding=7230",
        "https://github.com/kernelci/kcidb/blob/main/.github/workflows/"
        "deploy.yml?padding=4821",
        "https://github.com/kernelci/kcidb/blob/main/doc/installation.md?"
        "padding=1505"
    }

    while urls_expected and time.time() < deadline_time:
        time.sleep(retry_interval)
        urls_expected = {
            url for url in urls_expected
            if not check_url_in_cache(url)
        }
    assert not urls_expected, \
        f"Expected URLs '{urls_expected}' not found in the cache"

    current_time = time.time()
    if current_time < deadline_time:
        time.sleep(deadline_time - current_time)

    # URL cases not to be cached
    urls_not_expected = {
        url for url in (
            # Invalid url
            'https://non-existing-name.kernel.org/pub/linux/',
            # Larger-than-maximum size URL
            "https://cdn.kernel.org/pub/linux/kernel/v6.x/"
            "linux-6.4.11.tar.xz",
            # Wrong hash URL
            "https://github.com/kernelci/kcidb/blob/main/Dockerfile",
            # URL from a wrong field
            "https://github.com/kernelci/kcidb/tree/main/kcidb/?padding=4165"
        ) if check_url_in_cache(url)
    }
    assert not urls_not_expected, \
        f"Unexpected URLs {urls_not_expected} found in the cache"


def test_purge_db(empty_deployment):
    """Check kcidb_purge_db() works correctly"""

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    # Use the current time to avoid deployment purge trigger
    timestamp_before = datetime.now(timezone.utc)
    str_before = timestamp_before.isoformat(timespec="microseconds")

    data_before = dict(
        version=dict(
            major=kcidb.io.SCHEMA.major, minor=kcidb.io.SCHEMA.minor
        ),
        checkouts=[dict(
            id="origin:1", origin="origin",
            _timestamp=str_before
        )],
        builds=[dict(
            id="origin:1", origin="origin", checkout_id="origin:1",
            _timestamp=str_before
        )],
        tests=[dict(
            id="origin:1", origin="origin", build_id="origin:1",
            _timestamp=str_before
        )],
        issues=[dict(
            id="origin:1", origin="origin", version=1,
            _timestamp=str_before
        )],
        incidents=[dict(
            id="origin:1", origin="origin",
            issue_id="origin:1", issue_version=1,
            _timestamp=str_before
        )],
    )

    timestamp_after = timestamp_before + timedelta(microseconds=1)
    str_after = timestamp_after.isoformat(timespec="microseconds")

    data_after = dict(
        version=dict(
            major=kcidb.io.SCHEMA.major, minor=kcidb.io.SCHEMA.minor
        ),
        checkouts=[dict(
            id="origin:2", origin="origin",
            _timestamp=str_after
        )],
        builds=[dict(
            id="origin:2", origin="origin", checkout_id="origin:2",
            _timestamp=str_after
        )],
        tests=[dict(
            id="origin:2", origin="origin", build_id="origin:2",
            _timestamp=str_after
        )],
        issues=[dict(
            id="origin:2", origin="origin", version=1,
            _timestamp=str_after
        )],
        incidents=[dict(
            id="origin:2", origin="origin",
            issue_id="origin:2", issue_version=1,
            _timestamp=str_after
        )],
    )

    def filter_test_data(data):
        """Filter objects created by this test from I/O data"""
        return {
            key: [
                deepcopy(obj) for obj in value
                if obj.get("_timestamp") in (str_before, str_after)
            ] if key and key in kcidb.io.SCHEMA.graph
            else deepcopy(value)
            for key, value in data.items()
        }

    # Merge the before and after data
    data = kcidb.io.SCHEMA.merge(data_before, [data_after])

    # Load the merged data into the database
    client = kcidb.db.Client(os.environ["KCIDB_DATABASE"])
    client.load(data, with_metadata=True)
    dump = filter_test_data(client.dump())
    for obj_list_name in kcidb.io.SCHEMA.graph:
        if obj_list_name:
            assert len(dump.get(obj_list_name, [])) == 2, \
                f"Invalid number of {obj_list_name}"

    # Trigger the purge at the boundary
    kcidb.mq.JSONPublisher(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_PURGE_DB_TRIGGER_TOPIC"]
    ).publish(dict(database="op", timedelta=dict(stamp=str_after)))

    # Wait and check for the purge
    deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
    while datetime.now(timezone.utc) < deadline:
        time.sleep(5)
        dump = filter_test_data(client.dump())
        # If everything was purged
        # NOTE: For some reason we're hitting incomplete purges sometimes
        if all(
            len(dump.get(n, [])) == 1
            for n in kcidb.io.SCHEMA.graph
            if n
        ):
            break
    assert dump == data_after

    # Make sure we were getting the operational DB dump
    op_client = kcidb.db.Client(os.environ["KCIDB_OPERATIONAL_DATABASE"])
    assert dump == filter_test_data(op_client.dump())

    # Make sure the archive database is still intact
    ar_client = kcidb.db.Client(os.environ["KCIDB_ARCHIVE_DATABASE"])
    assert filter_test_data(ar_client.dump()) == data
