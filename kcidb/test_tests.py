"""kcdib.tests module tests"""

import textwrap
from kcidb.unittest import assert_executes


def test_validate_main():
    """Check kcidb-tests-validate works"""
    assert_executes('', "kcidb.tests.validate_main",
                    status=1, stderr_re=".*ValidationError.*")
    assert_executes('test: "', "kcidb.tests.validate_main",
                    status=1, stderr_re=".*ScannerError.*")
    assert_executes(textwrap.dedent("""
                        te/st:
                            title: title
                            home: https://example.com
                    """),
                    "kcidb.tests.validate_main",
                    status=1, stderr_re=".*ValidationError.*")
    assert_executes(textwrap.dedent("""
                        test:
                            title: title
                            home: https://example.com
                    """),
                    "kcidb.tests.validate_main")
    assert_executes(textwrap.dedent("""
                        test:
                            title: title
                            description: title
                            home: https://example.com
                            description: |
                                desc
                                ription
                    """),
                    "kcidb.tests.validate_main")
