"""main.py tests"""

import os
import unittest
from unittest.mock import patch
from importlib import import_module
import yaml


class MainTestCase(unittest.TestCase):
    """main.py test case"""

    @patch('kcidb.misc.get_secret')
    @patch('kcidb.mq.Publisher')
    @patch('kcidb.spool.Client')
    @patch('kcidb.db.Client')
    # pylint: disable=unused-argument
    def test_import(self, db_client, spool_client, mq_publisher, get_secret):
        """Check main.py can be loaded"""
        # Load deployment environment variables
        with open("main.env.yaml", "r") as env_file:
            env = yaml.safe_load(env_file)
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
