"""Kernel CI reporting - unittest extensions"""

import os
import re
import sys
import unittest
import textwrap
import subprocess


def local_only(subject):
    """Decorate a test function or a test case class as local-only"""
    return unittest.skipIf(
        os.environ.get("KCIDB_DEPLOYED", ""), "local-only"
    )(subject)


def deployment_only(subject):
    """Decorate a test function or a test case class as deployment-only"""
    return unittest.skipUnless(
        os.environ.get("KCIDB_DEPLOYED", ""), "deployment-only"
    )(subject)


class TestCase(unittest.TestCase):
    """KCIDB test case"""

    @staticmethod
    def execute(stdin, name, *args, driver_source="return function()"):
        """
        Run a KCIDB executable with the specified name, by executing its main
        function.

        Args:
            stdin:          A string to pass to the standard input of the
                            executable.
            name:           The fully-qualified name of the executable's main
                            function. Must end with "_main".
            args:           Command-line arguments to the executable.
            driver_source:  The Python source code for the body of the
                            function's driver (setup/teardown) function. To be
                            interpreted within the executable. Must use
                            four-space indent. Will have the function to call
                            in a local variable called "function". Must return
                            what the function returns.

        Returns:
            An instance of subprocess.CompletedProcess representing the
            execution results.
        """
        assert name.endswith("_main")
        executable = name[:-5].replace(".", "-").replace("_", "-")
        # It's really OK, pylint: disable=subprocess-run-check
        return subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, kcidb\n"
                "\n"
                "def run_function(function):\n" +
                textwrap.indent(driver_source, "    ") +
                "\n"
                f"sys.argv[0] = {repr(executable)}\n"
                f"sys.exit(run_function({name}))\n",
                *args
            ],
            encoding="utf8",
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    # Gotta conform to unittest conventions, pylint: disable=invalid-name
    def assertExecutes(self, stdin, name, *args,
                       driver_source="return function()",
                       stdout_re="", stderr_re="", status=0):
        """
        Assert a KCIDB executable produces certain stdout, stderr, and exit
        status, provided the specified standard input and arguments.

        Args:
            stdin:          A string to pass to the standard input of the
                            executable.
            name:           The fully-qualified name of the executable's main
                            function. Must end with "_main".
            args:           Command-line arguments to the executable.
            driver_source:  The Python source code for the function's driver
                            (execution setup/teardown) to be interpreted
                            within the executable. Must use four-space indent.
                            Will have the function to call in a local variable
                            called "function".
            stdout_re:      A regular expression the executable's stdout must
                            match.
            stderr_re:      A regular expression the executable's stderr must
                            match.
            status:         The exit status the executable should produce.
        """
        result = self.execute(stdin, name, *args, driver_source=driver_source)
        errors = []
        if result.returncode != status:
            errors.append(f"Expected exit status {status}, "
                          f"got {result.returncode}")
        if not re.fullmatch(stdout_re, result.stdout, re.DOTALL):
            errors.append(f"Stdout doesn't match regex {stdout_re!r}:\n"
                          f"{textwrap.indent(result.stdout, '    ')}")
        if not re.fullmatch(stderr_re, result.stderr, re.DOTALL):
            errors.append(f"Stderr doesn't match regex {stderr_re!r}:\n"
                          f"{textwrap.indent(result.stderr, '    ')}")
        if errors:
            raise AssertionError("\n".join(errors))
