"""kcdib.mq module tests"""

import re
import textwrap
import json
import kcidb_io
import kcidb


class KCIDBMQMainFunctionsTestCase(kcidb.unittest.TestCase):
    """Test case for main functions"""

    def test_publisher_init_main(self):
        """Check kcidb-mq-publisher-init works"""
        argv = ["kcidb.mq.publisher_init_main", "-p", "project", "-t", "topic"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            publisher = Mock()
            publisher.init = Mock()
            with patch("kcidb.mq.Publisher", return_value=publisher) as \
                    Publisher:
                status = function()
            Publisher.assert_called_once_with("project", "topic")
            publisher.init.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_publisher_cleanup_main(self):
        """Check kcidb-mq-publisher-cleanup works"""
        argv = ["kcidb.mq.publisher_cleanup_main",
                "-p", "project", "-t", "topic"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            publisher = Mock()
            publisher.cleanup = Mock()
            with patch("kcidb.mq.Publisher", return_value=publisher) as \
                    Publisher:
                status = function()
            Publisher.assert_called_once_with("project", "topic")
            publisher.cleanup.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_publisher_publish_main(self):
        """Check kcidb-mq-publisher-publish works"""
        argv = ["kcidb.mq.publisher_publish_main",
                "-p", "project", "-t", "topic"]
        empty = kcidb_io.new()

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            publisher = Mock()
            future = Mock()
            future.result = Mock(return_value="id")
            publisher.publish = Mock(return_value=future)
            with patch("kcidb.mq.Publisher", return_value=publisher) as \
                    Publisher:
                status = function()
            Publisher.assert_called_once_with("project", "topic")
            publisher.publish.assert_called_once_with({repr(empty)})
            return status
        """)
        self.assertExecutes('{', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*JSONParseError.*")
        self.assertExecutes('{}', *argv, driver_source=driver_source,
                            status=1, stderr_re=".*ValidationError.*")
        self.assertExecutes(json.dumps(empty), *argv,
                            driver_source=driver_source,
                            stdout_re="id\n")

        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock, call
            publisher = Mock()
            future = Mock()
            future.result = Mock(return_value="id")
            publisher.publish = Mock(return_value=future)
            with patch("kcidb.mq.Publisher", return_value=publisher) as \
                    Publisher:
                status = function()
            Publisher.assert_called_once_with("project", "topic")
            assert publisher.publish.call_count == 2
            publisher.publish.assert_has_calls([call({repr(empty)}),
                                                call({repr(empty)})])
            return status
        """)
        self.assertExecutes(json.dumps(empty) + json.dumps(empty), *argv,
                            driver_source=driver_source,
                            stdout_re="id\nid\n")

    def test_subscriber_init_main(self):
        """Check kcidb-mq-subscriber-init works"""
        argv = ["kcidb.mq.subscriber_init_main",
                "-p", "project", "-t", "topic", "-s", "subscription"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            subscriber = Mock()
            subscriber.init = Mock()
            with patch("kcidb.mq.Subscriber", return_value=subscriber) as \
                    Subscriber:
                status = function()
            Subscriber.assert_called_once_with("project", "topic",
                                               "subscription")
            subscriber.init.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_subscriber_cleanup_main(self):
        """Check kcidb-mq-subscriber-cleanup works"""
        argv = ["kcidb.mq.subscriber_cleanup_main",
                "-p", "project", "-t", "topic", "-s", "subscription"]
        driver_source = textwrap.dedent("""
            from unittest.mock import patch, Mock
            subscriber = Mock()
            subscriber.cleanup = Mock()
            with patch("kcidb.mq.Subscriber", return_value=subscriber) as \
                    Subscriber:
                status = function()
            Subscriber.assert_called_once_with("project", "topic",
                                               "subscription")
            subscriber.cleanup.assert_called_once()
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source)

    def test_subscriber_pull_main(self):
        """Check kcidb-mq-subscriber-pull works"""
        argv = ["kcidb.mq.subscriber_pull_main",
                "-p", "project", "-t", "topic", "-s", "subscription",
                "--timeout", "123", "--indent=0"]
        empty = kcidb_io.new()
        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            subscriber = Mock()
            subscriber.pull = Mock(return_value=[("ID", {repr(empty)})])
            subscriber.ack = Mock()
            with patch("kcidb.mq.Subscriber", return_value=subscriber) as \
                    Subscriber:
                status = function()
            Subscriber.assert_called_once_with("project", "topic",
                                               "subscription")
            subscriber.pull.assert_called_once_with(1, timeout=123)
            subscriber.ack.assert_called_once_with("ID")
            return status
        """)
        self.assertExecutes("", *argv, driver_source=driver_source,
                            stdout_re=re.escape(json.dumps(empty) + "\n"))
