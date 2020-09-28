"""kcdib.db module tests"""

import re
import textwrap
import json
import kcidb


class KCIDBDBMainFunctionsTestCase(kcidb.unittest.TestCase):
    """Test case for main functions"""

    def test_init_main(self):
        """Check kcidb-db-init works"""
        argv = ["kcidb.db.init_main", "-p", "project", "-d", "dataset"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            client = Mock()
            client.init = Mock()
            with patch("kcidb.db.Client", return_value=client) as \
                    Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.init.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_cleanup_main(self):
        """Check kcidb-db-cleanup works"""
        argv = ["kcidb.db.cleanup_main", "-p", "project", "-d", "dataset"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            client = Mock()
            client.cleanup = Mock()
            with patch("kcidb.db.Client", return_value=client) as \
                    Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.cleanup.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_dump_main(self):
        """Check kcidb-db-dump works"""
        empty = kcidb.io.new()
        argv = ["kcidb.db.dump_main", "-p", "project", "-d", "dataset",
                "--indent=0"]

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.dump_iter = Mock(return_value=iter(({repr(empty)},)))
            with patch("kcidb.db.Client", return_value=client) as \
                    Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.dump_iter.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source,
                            stdout_re=re.escape(json.dumps(empty) + "\n"))

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.dump_iter = Mock(return_value=iter({repr((empty, empty))}))
            with patch("kcidb.db.Client", return_value=client) as \
                    Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.dump_iter.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source,
                            stdout_re=re.escape(json.dumps(empty) + "\n" +
                                                json.dumps(empty) + "\n"))

    def test_query_main(self):
        """Check kcidb-db-query works"""
        driver_source = textwrap.dedent("""
            from unittest.mock import patch
            with patch("kcidb.db.Client"):
                return function()
        """)
        argv = ["kcidb.db.query_main", "-p", "project", "-d", "dataset"]
        self.assertExecutes("", *argv, driver_source=driver_source)

        git_commit_hash = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
        argv = [
            "kcidb.db.query_main", "-p", "project", "-d", "dataset",
            "-r", git_commit_hash, "-b", "test:build:1", "-t", "test:test:1",
            "-R", "4ff6%", "-B", "test:build:%", "-T", "test:test:%",
            "--parents", "--children", "--objects-per-report", "10",
            "--indent=0",
        ]
        empty = kcidb.io.new()
        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.query_iter = Mock(return_value=iter((
                {repr(empty)}, {repr(empty)},
            )))
            with patch("kcidb.db.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.query_iter.assert_called_once_with(
                ids=dict(revisions=[{repr(git_commit_hash)}],
                         builds=["test:build:1"],
                         tests=["test:test:1"]),
                patterns=dict(revisions=["4ff6%"],
                              builds=["test:build:%"],
                              tests=["test:test:%"]),
                parents=True,
                children=True,
                objects_per_report=10
            )
            return status
        """)
        self.assertExecutes(
            json.dumps(empty), *argv,
            driver_source=driver_source,
            stdout_re=re.escape(
                json.dumps(empty) + "\n" +
                json.dumps(empty) + "\n"
            )
        )

    def test_load_main(self):
        """Check kcidb-db-load works"""
        driver_source = textwrap.dedent("""
            from unittest.mock import patch
            with patch("kcidb.db.Client"):
                return function()
        """)
        argv = ["kcidb.db.load_main", "-p", "project", "-d", "dataset"]

        self.assertExecutes("", *argv, driver_source=driver_source)
        self.assertExecutes('{', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*ValidationError.*")

        empty = kcidb.io.new()

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.load = Mock()
            with patch("kcidb.db.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.load.assert_called_once_with({repr(empty)})
            return status
        """)
        self.assertExecutes(json.dumps(empty), *argv,
                            driver_source=driver_source)

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock, call
            client = Mock()
            client.load = Mock()
            with patch("kcidb.db.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            assert client.load.call_count == 2
            client.load.assert_has_calls([call({repr(empty)}),
                                          call({repr(empty)})])
            return status
        """)
        self.assertExecutes(json.dumps(empty) + json.dumps(empty), *argv,
                            driver_source=driver_source)

    def test_complement_main(self):
        """Check kcidb-db-complement works"""
        driver_source = textwrap.dedent("""
            from unittest.mock import patch
            with patch("kcidb.db.Client"):
                return function()
        """)
        argv = ["kcidb.db.complement_main", "-p", "project", "-d", "dataset",
                "--indent=0"]

        self.assertExecutes("", *argv, driver_source=driver_source)
        self.assertExecutes('{', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*ValidationError.*")

        git_commit_hash = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
        report_a = {**kcidb.io.new(),
                    "revisions": [dict(id=git_commit_hash, origin="test")]}
        git_commit_hash = "8dc8588e6fd405df996d5a83fcc6c5af6284f72d"
        report_b = {**kcidb.io.new(),
                    "revisions": [dict(id=git_commit_hash, origin="test")]}

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.complement = Mock(return_value={repr(report_b)})
            with patch("kcidb.db.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            client.complement.assert_called_once_with({repr(report_a)})
            return status
        """)
        self.assertExecutes(json.dumps(report_a), *argv,
                            driver_source=driver_source,
                            stdout_re=re.escape(json.dumps(report_b) + "\n"))

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock, call
            client = Mock()
            client.complement = Mock(side_effect={repr((report_b, report_a))})
            with patch("kcidb.db.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("dataset", project_id="project")
            assert client.complement.call_count == 2
            client.complement.assert_has_calls([call({repr(report_a)}),
                                                call({repr(report_b)})])
            return status
        """)
        self.assertExecutes(json.dumps(report_a) + json.dumps(report_b), *argv,
                            driver_source=driver_source,
                            stdout_re=re.escape(json.dumps(report_b) + "\n" +
                                                json.dumps(report_a) + "\n"))
