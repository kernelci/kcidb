"""main.py tests"""

import os
import subprocess
import unittest
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


def check_url_in_cache(url):
    """Check whether the URL is sourced from storage or not."""
    url_encoded = quote(url)
    cache_redirector_url = os.environ["KCIDB_CACHE_REDIRECTOR_URL"]
    response = requests.get(
        f"{cache_redirector_url}?{url_encoded}",
        timeout=10,   # Time in secs
        allow_redirects=True
    )
    redirect_url = response.url
    if response.status_code == 200:
        # Check if the redirect URL matches the blob storage URL pattern
        if redirect_url.startswith('https://storage.googleapis.com/'):
            return True
    return False


def test_url_caching(empty_deployment):
    """kcidb cache client workflow test"""

    # Make empty_deployment appear used to silence pylint warning
    assert empty_deployment is None

    pub_client = kcidb.mq.URLListPublisher(
        project_id=os.environ["GCP_PROJECT"],
        topic_name=os.environ["KCIDB_UPDATED_URLS_TOPIC"]
    )

    # Submit messages with different URLs
    urls_messages = [
        ["https://github.com/kernelci/kcidb/blob/main/setup.py"],
        ["https://github.com/kernelci/kcidb/blob/main/requirements.txt",
         "https://github.com/kernelci/kcidb/blob/main/README.md"]
    ]

    for urls in urls_messages:
        pub_client.publish(urls)

    # Retry checking URLs in the cache for a minute
    retry_interval = 5  # seconds
    max_retries = 12  # 60 seconds / 5 seconds

    for urls in urls_messages:
        for url in urls:
            for _ in range(max_retries):
                if check_url_in_cache(url):
                    break
                time.sleep(retry_interval)
            else:
                raise AssertionError(f"URL '{url}' not found in the cache")
