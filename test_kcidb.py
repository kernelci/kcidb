"""kcidb namespace tests"""

import re
import json
import textwrap
import kcidb


class KCIDBMainFunctionsTestCase(kcidb.unittest.TestCase):
    """Test case for main functions"""

    def test_json_output_options(self):
        """Check JSON output options work"""
        self.assertExecutes("", "kcidb.schema_main",
                            stdout_re="\\{.*")
        self.assertExecutes("", "kcidb.schema_main", "--seq",
                            stdout_re="\x1e\\{.*")
        self.assertExecutes("", "kcidb.schema_main", "--indent=0",
                            stdout_re="\\{\".*")
        self.assertExecutes("", "kcidb.schema_main", "--indent=4",
                            stdout_re="\\{\n    \".*")
        self.assertExecutes("", "kcidb.schema_main", "--indent=2",
                            stdout_re="\\{\n  \".*")

    def test_submit_main(self):
        """Check kcidb-submit works"""
        driver_source = textwrap.dedent("""
            from unittest.mock import patch
            with patch("kcidb.Client"):
                return function()
        """)
        argv = ["kcidb.submit_main", "-p", "project", "-t", "topic"]

        self.assertExecutes("", *argv, driver_source=driver_source)
        self.assertExecutes('{', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*ValidationError.*")

        empty = kcidb.io.new()

        empty_driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            client = Mock()
            client.submit = Mock()
            with patch("kcidb.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with(project_id="project",
                                           topic_name="topic")
            client.submit.assert_called_once_with({repr(empty)})
            return status
        """)
        self.assertExecutes(json.dumps(empty), *argv,
                            driver_source=empty_driver_source)

        twice_empty_driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock, call
            client = Mock()
            client.submit = Mock()
            with patch("kcidb.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with(project_id="project",
                                           topic_name="topic")
            assert client.submit.call_count == 2
            client.submit.assert_has_calls([call({repr(empty)}),
                                            call({repr(empty)})])
            return status
        """)
        self.assertExecutes(json.dumps(empty) + json.dumps(empty), *argv,
                            driver_source=twice_empty_driver_source)

    def test_query_main(self):
        """Check kcidb-query works"""
        driver_source = textwrap.dedent("""
            from unittest.mock import patch
            with patch("kcidb.Client"):
                return function()
        """)
        argv = ["kcidb.query_main", "-p", "project", "-d", "dataset"]
        self.assertExecutes("", *argv, driver_source=driver_source)

        git_commit_hash = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
        argv = [
            "kcidb.query_main", "-p", "project", "-d", "dataset",
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
            with patch("kcidb.Client", return_value=client) as Client:
                status = function()
            Client.assert_called_once_with(project_id="project",
                                           dataset_name="dataset")
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

    def test_schema_main(self):
        """Check kcidb-schema works"""
        self.assertExecutes("", "kcidb.schema_main", stdout_re="\\{.*")

    def test_validate_main(self):
        """Check kcidb-validate works"""
        self.assertExecutes('', "kcidb.validate_main")
        self.assertExecutes('{"version":{"major":3,"minor":0}}',
                            "kcidb.validate_main")
        self.assertExecutes('{', "kcidb.validate_main",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.validate_main",
                            status=1, stderr_re=".*ValidationError.*")

    def test_upgrade_main(self):
        """Check kcidb-upgrade works"""
        major = kcidb.io.schema.LATEST.major
        minor = kcidb.io.schema.LATEST.minor

        prev_version = \
            json.dumps(dict(version=dict(major=major - 1, minor=minor))) + "\n"
        latest_version = \
            json.dumps(dict(version=dict(major=major, minor=minor))) + "\n"

        self.assertExecutes('', "kcidb.upgrade_main")
        self.assertExecutes('{', "kcidb.upgrade_main",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.upgrade_main",
                            status=1, stderr_re=".*ValidationError.*")
        self.assertExecutes(latest_version, "kcidb.upgrade_main", "--indent=0",
                            stdout_re=re.escape(latest_version))
        self.assertExecutes(latest_version + latest_version,
                            "kcidb.upgrade_main", "--indent=0",
                            stdout_re=re.escape(latest_version +
                                                latest_version))
        self.assertExecutes(prev_version, "kcidb.upgrade_main", "--indent=0",
                            stdout_re=re.escape(latest_version))
        self.assertExecutes(prev_version + prev_version,
                            "kcidb.upgrade_main", "--indent=0",
                            stdout_re=re.escape(latest_version +
                                                latest_version))

    def test_count_main(self):
        """Check kcidb-count works"""
        self.assertExecutes('', "kcidb.count_main")
        self.assertExecutes('{', "kcidb.count_main",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.count_main",
                            status=1, stderr_re=".*ValidationError.*")

        empty = json.dumps(dict(version=dict(major=3, minor=0)))
        self.assertExecutes(empty, "kcidb.count_main", stdout_re="0\n")
        self.assertExecutes(empty + empty, "kcidb.count_main",
                            stdout_re="0\n0\n")

        one_revision = json.dumps(dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id="c003f145ae96b769858ee5501189c582a97c6742",
                     origin="test")
            ]
        ))
        self.assertExecutes(one_revision, "kcidb.count_main", stdout_re="1\n")
        self.assertExecutes(one_revision + one_revision,
                            "kcidb.count_main", stdout_re="1\n1\n")

    def test_summarize_main(self):
        """Check kcidb-summarize works"""
        self.assertExecutes('', "kcidb.summarize_main", "revisions")
        self.assertExecutes('{', "kcidb.summarize_main", "revisions",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.summarize_main", "revisions",
                            status=1, stderr_re=".*ValidationError.*")

        empty = json.dumps(dict(version=dict(major=3, minor=0)))
        self.assertExecutes(empty, "kcidb.summarize_main", "revisions")
        self.assertExecutes(empty + empty, "kcidb.summarize_main", "revisions")

        git_commit_hash = "c003f145ae96b769858ee5501189c582a97c6742"
        one_revision = json.dumps(dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id=git_commit_hash,
                     git_commit_hash=git_commit_hash,
                     origin="test")
            ]
        ))
        self.assertExecutes(one_revision, "kcidb.summarize_main", "revisions",
                            stdout_re="c003f145ae96\n")
        self.assertExecutes(one_revision + one_revision,
                            "kcidb.summarize_main", "revisions",
                            stdout_re="c003f145ae96\nc003f145ae96\n")

    def test_describe_main(self):
        """Check kcidb-describe works"""
        self.assertExecutes('', "kcidb.describe_main", "revisions")
        self.assertExecutes('{', "kcidb.describe_main", "revisions",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.describe_main", "revisions",
                            status=1, stderr_re=".*ValidationError.*")

        empty = json.dumps(dict(version=dict(major=3, minor=0)))
        self.assertExecutes(empty, "kcidb.describe_main", "revisions")
        self.assertExecutes(empty + empty, "kcidb.describe_main", "revisions")

        git_commit_hash = "c003f145ae96b769858ee5501189c582a97c6742"
        one_revision = json.dumps(dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id=git_commit_hash,
                     git_commit_hash=git_commit_hash,
                     origin="test")
            ]
        ))
        self.assertExecutes(one_revision, "kcidb.describe_main", "revisions",
                            stdout_re="Below is the summary.*\x00")
        self.assertExecutes(one_revision + one_revision,
                            "kcidb.describe_main", "revisions",
                            stdout_re="Below .*\x00Below .*\x00")

    def test_merge_main(self):
        """Check kcidb-merge works"""
        empty = json.dumps(dict(version=dict(major=3, minor=0))) + "\n"

        self.assertExecutes('', "kcidb.merge_main", "--indent=0",
                            stdout_re=re.escape(empty))
        self.assertExecutes('{', "kcidb.merge_main",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.merge_main",
                            status=1, stderr_re=".*ValidationError.*")
        self.assertExecutes(empty, "kcidb.merge_main", "--indent=0",
                            stdout_re=re.escape(empty))
        self.assertExecutes(empty + empty, "kcidb.merge_main", "--indent=0",
                            stdout_re=re.escape(empty))

        hash_a = "c003f145ae96b769858ee5501189c582a97c6742"
        report_a = dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id=hash_a, origin="test")
            ],
            builds=[
                dict(id="test:build:1", origin="text", revision_id=hash_a)
            ],
            tests=[
                dict(id="test:test:1", origin="text", build_id="test:build:1")
            ]
        )

        hash_b = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
        report_b = dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id=hash_b, origin="test")
            ],
            builds=[
                dict(id="test:build:2", origin="text", revision_id=hash_b)
            ],
            tests=[
                dict(id="test:test:2", origin="text", build_id="test:build:2")
            ]
        )
        merged_report = dict(
            version=dict(major=3, minor=0),
            revisions=report_a["revisions"] + report_b["revisions"],
            builds=report_a["builds"] + report_b["builds"],
            tests=report_a["tests"] + report_b["tests"],
        )
        self.assertExecutes(json.dumps(report_a) + json.dumps(report_b),
                            "kcidb.merge_main", "--indent=0",
                            stdout_re=re.escape(
                                json.dumps(merged_report) + "\n"
                            ))

    def test_notify_main(self):
        """Check kcidb-notify works"""
        self.assertExecutes('', "kcidb.notify_main")
        self.assertExecutes('{', "kcidb.notify_main",
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', "kcidb.notify_main",
                            status=1, stderr_re=".*ValidationError.*")

        empty = json.dumps(dict(version=dict(major=3, minor=0)))
        self.assertExecutes(empty, "kcidb.notify_main")
        self.assertExecutes(empty + empty, "kcidb.notify_main")

        git_commit_hash = "4ff6a2469104218a044ff595a0c1eb469ca7ea01"
        build_fail = json.dumps(dict(
            version=dict(major=3, minor=0),
            revisions=[
                dict(id=git_commit_hash,
                     git_commit_hash=git_commit_hash,
                     origin="test")
            ],
            builds=[
                dict(id="test:build:1",
                     origin="test",
                     revision_id=git_commit_hash,
                     valid=False)
            ]
        ))

        self.assertExecutes(build_fail, "kcidb.notify_main",
                            stdout_re="Subject: Test revision: .*\x00"
                                      "Subject: Test build: .*\x00")
        self.assertExecutes(build_fail + build_fail, "kcidb.notify_main",
                            stdout_re="Subject: Test revision: .*\x00"
                                      "Subject: Test build: .*\x00"
                                      "Subject: Test revision: .*\x00"
                                      "Subject: Test build: .*\x00")
