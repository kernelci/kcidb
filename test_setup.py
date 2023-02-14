"""setup.py tests"""

import re
import sys
from importlib import import_module
from unittest.mock import patch
import pytest


@patch('setuptools.setup')
def test_console_scripts(setuptools_setup):
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
            with pytest.raises(SystemExit) as excinfo:
                getattr(import_module(module), function)()
            assert excinfo.value.code == 0
        finally:
            sys.argv = orig_argv
