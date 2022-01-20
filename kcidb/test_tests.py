"""kcdib.tests module tests"""

import textwrap
import kcidb
from kcidb.unittest import local_only


@local_only
class KCIDBTestsMainFunctionsTestCase(kcidb.unittest.TestCase):
    """Test case for main functions"""

    def test_validate_main(self):
        """Check kcidb-tests-validate works"""
        self.assertExecutes('', "kcidb.tests.validate_main",
                            status=1, stderr_re=".*ValidationError.*")
        self.assertExecutes('test: "', "kcidb.tests.validate_main",
                            status=1, stderr_re=".*ScannerError.*")
        self.assertExecutes(textwrap.dedent("""
                                te/st:
                                    title: title
                                    home: https://example.com
                            """),
                            "kcidb.tests.validate_main",
                            status=1, stderr_re=".*ValidationError.*")
        self.assertExecutes(textwrap.dedent("""
                                test:
                                    title: title
                                    home: https://example.com
                            """),
                            "kcidb.tests.validate_main")
        self.assertExecutes(textwrap.dedent("""
                                test:
                                    title: title
                                    description: title
                                    home: https://example.com
                                    description: |
                                        desc
                                        ription
                            """),
                            "kcidb.tests.validate_main")
