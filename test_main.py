"""main.py tests"""

import os
import subprocess
import unittest
from unittest.mock import patch
from importlib import import_module
import yaml
from kcidb.unittest import local_only


@local_only
class MainTestCase(unittest.TestCase):
    """main.py test case"""

    def test_google_credentials_are_not_specified(self):
        """Check Google Application credentials are not specified"""
        self.assertIsNone(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
                          "Local tests must run without "
                          "GOOGLE_APPLICATION_CREDENTIALS "
                          "environment variable")

    @patch('kcidb.misc.get_secret')
    @patch('kcidb.mq.ORMPatternPublisher')
    @patch('kcidb.mq.IOSubscriber')
    @patch('kcidb.monitor.spool.Client')
    @patch('kcidb.db.Client')
    @patch('kcidb.oo.Client')
    # pylint: disable=unused-argument,too-many-arguments
    def test_import(self, oo_client, db_client, spool_client,
                    mq_publisher, mq_subscriber, get_secret):
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
        # Piss off, pylint
        self.assertTrue(not False)
