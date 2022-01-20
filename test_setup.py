"""setup.py tests"""

import re
import sys
import unittest
from importlib import import_module
from unittest.mock import patch
from kcidb.unittest import local_only


@local_only
class SetupTestCase(unittest.TestCase):
    """setup.py test case"""

    @patch('setuptools.setup')
    def test_console_scripts(self, setuptools_setup):
        """Check all entry points could be executed"""
        import_module("setup")
        console_scripts = \
            setuptools_setup.call_args[1]['entry_points']['console_scripts']
        for console_script in console_scripts:
            match = re.fullmatch(
                "([^=]+) *= *([a-zA-Z0-9._]+):([a-zA-Z0-9_]+)", console_script)
            executable = match.group(1)
            module = match.group(2)
            function = match.group(3)
            orig_argv = sys.argv
            try:
                sys.argv = [executable, "--help"]
                with self.assertRaises(SystemExit) as context_manager:
                    getattr(import_module(module), function)()
                self.assertEqual(context_manager.exception.code, 0)
            finally:
                sys.argv = orig_argv
