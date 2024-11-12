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


def url_is_in_cache(url, content):
    """Check whether the URL is in the cache or not."""
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
        location = response.headers.get("Location", "")
        if location.startswith('https://storage.googleapis.com/'):
            if content is not None:
                response = requests.get(
                    location, timeout=10, allow_redirects=True
                )
                response.raise_for_status()
                assert response.content == content
            return True
    return False


def test_url_caching(empty_deployment):
    """kcidb cache client workflow test"""

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    urls_expected = [
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        "setup.py?padding=2366",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        "requirements.txt?padding=11277",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        ".gitignore?padding=1557",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        ".pylintrc?padding=1013",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        "doc/_index.md?padding=1435",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        ".github/workflows/deploy.yml?padding=307",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/"
        "doc/installation.md?padding=9761",
    ]
    urls_unexpected = [
        # Invalid url
        'https://non-existing-name.kernel.org/pub/linux/',
        # Larger-than-maximum size URL
        "https://cdn.kernel.org/pub/linux/kernel/v6.x/"
        "linux-6.4.11.tar.xz",
        # Wrong hash URL
        "https://github.com/kernelci/kcidb/blob/main/Dockerfile",
        # URL from a wrong field
        "https://github.com/kernelci/kcidb/tree/main/kcidb/?padding=4165",
    ]

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
                "log_url": urls_expected[0],
                "patchset_files": [
                    {
                        "name": "file",
                        "url": urls_expected[1]
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
                        "url": urls_expected[2]
                    }
                ],
                "log_url": urls_expected[3],
                "config_url": urls_expected[4],
                "output_files": [
                    {
                        "name": "kernel_image",
                        "url": urls_unexpected[0]
                    },
                    {
                        "name": "kernel",
                        "url": urls_unexpected[1]
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
                "log_url": urls_unexpected[2]
            },
            {
                "build_id": "kernelci:kernelci.org:64147283e6021132258c86c0",
                "id": "_:1",
                "origin": "_",
                "output_files": [
                    {
                        "name": "x86_64_4_console.log",
                        "url": urls_expected[5]
                    },
                    {
                        "name": "x86_64_4_IOMMU_boot_test_dmesg.log",
                        "url": urls_expected[6]
                    },
                ],
                "environment": {
                    "comment": "meson-s905d-p230 in lab-baylibre",
                    "misc": {
                        "rootfs_url": urls_unexpected[3]
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

    # URLs and their contents to check, as they should be cached
    urls_contents_expected = {}
    for url in urls_expected:
        response = requests.get(url, timeout=10, allow_redirects=True)
        response.raise_for_status()
        urls_contents_expected[url] = response.content

    # Wait until either URLs are cached or we hit the deadline
    while urls_contents_expected and time.time() < deadline_time:
        time.sleep(retry_interval)
        urls_contents_expected = {
            url: content
            for url, content in urls_contents_expected.items()
            if not url_is_in_cache(url, content)
        }
    assert not set(urls_contents_expected), \
        f"Expected URLs '{set(urls_contents_expected)}' " \
        f"not found in the cache"

    current_time = time.time()
    if current_time < deadline_time:
        time.sleep(deadline_time - current_time)

    # URL cases not to be cached
    urls_unexpected_but_found = set(filter(
        lambda url: url_is_in_cache(url, None), urls_unexpected
    ))
    assert not urls_unexpected_but_found, \
        f"Unexpected URLs {urls_unexpected_but_found} found in the cache"


def test_purge_db(empty_deployment):
    """Check kcidb_purge_db() works correctly"""
    # It's OK, pylint: disable=too-many-locals

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    # Each type of database, purging expectation, and client
    clients = dict(
        op=(True, kcidb.db.Client(os.environ["KCIDB_OPERATIONAL_DATABASE"])),
        sm=(True, kcidb.db.Client(os.environ["KCIDB_SAMPLE_DATABASE"])),
        ar=(False, kcidb.db.Client(os.environ["KCIDB_ARCHIVE_DATABASE"])),
    )

    # Determine the minimum supported I/O version
    min_io_version = min(c.get_schema()[1] for _, c in clients.values())

    # Use the current time to avoid deployment purge trigger
    timestamp_before = datetime.now(timezone.utc)
    str_before = timestamp_before.isoformat(timespec="microseconds")
    timestamp_cutoff = timestamp_before + timedelta(seconds=1)
    str_cutoff = timestamp_cutoff.isoformat(timespec="microseconds")

    data_before = dict(
        version=dict(
            major=min_io_version.major, minor=min_io_version.minor
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

    timestamp_after = timestamp_cutoff + timedelta(seconds=1)
    str_after = timestamp_after.isoformat(timespec="microseconds")

    data_after = dict(
        version=dict(
            major=min_io_version.major, minor=min_io_version.minor
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
            ] if key and key in min_io_version.graph
            else deepcopy(value)
            for key, value in data.items()
        }

    # Merge the before and after data
    data = min_io_version.merge(data_before, [data_after])

    # For each type of database, purging expectation, and client
    publisher = kcidb.mq.JSONPublisher(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_PURGE_DB_TRIGGER_TOPIC"]
    )
    for database, (purging, client) in clients.items():
        client.load(data, with_metadata=True)
        dump = filter_test_data(client.dump())
        for obj_list_name in min_io_version.graph:
            if obj_list_name:
                assert len(dump.get(obj_list_name, [])) == 2, \
                    f"Invalid number of {obj_list_name}"

        # Trigger the purge at the boundary
        publisher.publish(
            dict(database=database, timedelta=dict(stamp=str_cutoff))
        )

        # Wait and check for the purge
        deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
        while datetime.now(timezone.utc) < deadline:
            time.sleep(5)
            dump = filter_test_data(client.dump())
            # If everything was purged
            # NOTE: For some reason we're hitting incomplete purges sometimes
            if all(
                len(dump.get(n, [])) == 1
                for n in min_io_version.graph
                if n
            ):
                break
        assert dump == client.get_schema()[1].upgrade(
            data_after if purging else data
        )


def test_archive(empty_deployment):
    """Check kcidb_archive() works correctly"""
    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    op_client = kcidb.db.Client(os.environ["KCIDB_OPERATIONAL_DATABASE"])
    op_schema = op_client.get_schema()[1]
    ar_client = kcidb.db.Client(os.environ["KCIDB_ARCHIVE_DATABASE"])
    ar_schema = ar_client.get_schema()[1]
    publisher = kcidb.mq.JSONPublisher(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_ARCHIVE_TRIGGER_TOPIC"]
    )

    # Empty the archive
    ar_client.empty()

    # Generate timestamps
    ts_now = op_client.get_current_time()
    ts_3w = ts_now - timedelta(days=7 * 3)
    ts_4w = ts_now - timedelta(days=7 * 4)

    def gen_data(id, ts):
        """
        Generate a dataset with one object per type, all using the specified
        timestamp, ID, and origin extracted from the ID.
        """
        assert isinstance(id, str)
        assert isinstance(ts, datetime) and ts.tzinfo
        origin = id.split(":")[0]
        assert origin
        assert origin != id
        base = dict(id=id, origin=origin,
                    _timestamp=ts.isoformat(timespec='microseconds'))
        return dict(
            checkouts=[base | dict()],
            builds=[base | dict(checkout_id=id)],
            tests=[base | dict(build_id=id)],
            issues=[base | dict(version=1)],
            incidents=[base | dict(issue_id=id, issue_version=1)],
            **op_schema.new(),
        )

    # Generate datasets
    data_now = gen_data("archive:now", ts_now)
    data_3w = gen_data("archive:3w", ts_3w)
    data_4w = gen_data("archive:4w", ts_4w)

    # Load data_now into the operational DB
    op_client.load(data_now, with_metadata=True)
    # Trigger and wait for archival (ignore possibility of actual trigger)
    publisher.publish({})
    time.sleep(60)
    # Check data_now doesn't end up in the archive DB
    assert ar_schema.count(ar_client.dump()) == 0

    # Load data_3w and data_4w
    op_client.load(op_schema.merge(data_3w, [data_4w]), with_metadata=True)
    # Trigger and wait for archival (ignore possibility of actual trigger)
    publisher.publish({})
    time.sleep(60)
    # Check data_4w is in the archive database
    dump = ar_client.dump()
    assert all(
        any(obj["id"] == "archive:4w"
            for obj in dump.get(obj_list_name, []))
        for obj_list_name in op_schema.id_fields
    ), "No complete four-week old data in the archive"
    # Check data_3w is not in the archive database
    assert not any(
        any(obj["id"] == "archive:3w"
            for obj in dump.get(obj_list_name, []))
        for obj_list_name in op_schema.id_fields
    ), "Some three-week old data in the archive"
    # Trigger and wait for another archival (ignore chance of actual trigger)
    publisher.publish({})
    time.sleep(60)
    # Check data_3w is now in the archive database
    dump = ar_client.dump()
    assert all(
        any(obj["id"] == "archive:3w"
            for obj in dump.get(obj_list_name, []))
        for obj_list_name in op_schema.id_fields
    ), "No complete three-week old data in the archive"
