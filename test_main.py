"""main.py tests"""

import os
import subprocess
import unittest
from importlib import import_module
import yaml


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
