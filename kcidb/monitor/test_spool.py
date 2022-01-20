"""kcdib.spool module tests"""

import textwrap
import dateutil.parser
import kcidb
from kcidb.unittest import local_only


@local_only
class SpoolMainFunctionsTestCase(kcidb.unittest.TestCase):
    """Test case for main functions"""

    def test_wipe_main(self):
        """Check kcidb-spool-wipe works"""
        datetime_str = "2020-09-28 15:42:18.170439+03:00"
        datetime = dateutil.parser.isoparse(datetime_str)
        argv = [
            "kcidb.monitor.spool.wipe_main",
            "-p", "project", "-c", "collection",
            datetime_str
        ]
        driver_source = textwrap.dedent(f"""
            from unittest.mock import patch, Mock
            import datetime
            from dateutil.tz import tzoffset
            client = Mock()
            client.wipe = Mock()
            with patch("kcidb.monitor.spool.Client",
                       return_value=client) as Client:
                status = function()
            Client.assert_called_once_with("collection", project="project")
            client.wipe.assert_called_once_with(until={repr(datetime)})
            return status
        """)
        self.assertExecutes("", *argv,
                            driver_source=driver_source)
