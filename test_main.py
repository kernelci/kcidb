"""main.py tests"""

import os
import subprocess
import unittest
from importlib import import_module
import yaml
import kcidb
import time


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
            "env", "kernelci-production", "", "0",
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

def test_url_caching(empty_deployment):
    publisher = kcidb.mq.URLListPublisher(
        os.environ["GCP_PROJECT"],
        os.environ["KCIDB_UPDATED_URLS_TOPIC"]
    )
    publisher.publish([
        "https://raw.githubusercontent.com/kernelci/kcidb/main/test_kcidb.py",
        "https://raw.githubusercontent.com/kernelci/kcidb/main/conftest.py"
    ])

    cache = kcidb.cache.Client(os.environ["KCIDB_CACHE_BUCKET_NAME"], 5 * 1024 * 1024)

    for i in range(12):
        # Check if the URLs are in the cache
        url1_stored = cache.is_stored("https://raw.githubusercontent.com/kernelci/kcidb/main/test_kcidb.py")
        url2_stored = cache.is_stored("https://raw.githubusercontent.com/kernelci/kcidb/main/conftest.py")

        # If both URLs are in the cache, the test passes
        if url1_stored and url2_stored:
            break

        # Wait for five seconds before retrying
        time.sleep(5)
    else:
        # If the loop finishes without breaking, it means the URLs were not found in the cache.
        # The test fails in this case.
        assert False, "URLs not found in the cache"