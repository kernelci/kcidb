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
        allow_redirects=False
    )
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

    current_time = time.time()
    # The time when caching system should be done with all our data
    deadline_time = current_time + 180

    # URLs to be check, as they should be cached
    urls_expected = [
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
    ]

    # Retry checking URLs in the cache for a minute
    retry_interval = 5  # seconds

    for url in urls_expected:
        while True:
            if check_url_in_cache(url):
                break
            if time.time() > deadline_time:
                raise AssertionError(f"URL '{url}' not found in the cache")
            time.sleep(retry_interval)

    current_time = time.time()
    if current_time < deadline_time:
        time.sleep(deadline_time - current_time)

    # URL cases not to be cached
    urls_not_expected = [
        # Invalid url
        'https://non-existing-name.kernel.org/pub/linux/',
        # Larger-than-maximum size URL
        "https://cdn.kernel.org/pub/linux/kernel/v6.x/"
        "linux-6.4.11.tar.xz",
        # Wrong hash URL
        "https://github.com/kernelci/kcidb/blob/main/Dockerfile",
        # URL from a wrong field
        "https://github.com/kernelci/kcidb/tree/main/kcidb/?padding=4165"
    ]

    for url_not_expected in urls_not_expected:
        if check_url_in_cache(url_not_expected):
            raise AssertionError(f"Unexpected URL '{url_not_expected}' \
                found in the cache")
