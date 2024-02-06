"""kcdib.misc module tests"""

import datetime
from pytest import raises
from jsonschema.exceptions import ValidationError
import kcidb.misc


def test_timedelta_json_parse():
    """Check kcidb-tests-validate works"""
    f = kcidb.misc.timedelta_json_parse
    min_stamp = datetime.datetime(1, 1, 1, 0, 0, 0, 0,
                                  tzinfo=datetime.timezone.utc)
    stamp = datetime.datetime(1, 2, 3, 4, 5, 6, 7,
                              tzinfo=datetime.timezone.utc)
    stamp_str = stamp.isoformat(timespec="microseconds")
    with raises(ValidationError):
        f([], stamp)
    with raises(ValidationError):
        f({}, stamp)
    with raises(ValidationError):
        f(dict(stamp=10), stamp)
    with raises(ValidationError):
        f(dict(stamp=""), stamp)
    with raises(ValidationError):
        f(dict(delta=10), stamp)
    with raises(ValidationError):
        f(dict(delta=""), stamp)
    with raises(ValidationError):
        f(dict(delta={}), stamp)
    with raises(ValidationError):
        f(dict(delta=dict(seconds=-1)), stamp)

    assert f(dict(stamp=stamp_str), min_stamp) == stamp
    assert f(dict(delta=dict(microseconds=0), stamp=stamp_str),
             min_stamp) == stamp

    assert f(dict(delta=dict(microseconds=0)), stamp) == stamp
    assert f(dict(delta=dict(seconds=0, microseconds=0)), stamp) == stamp
    assert f(dict(delta=dict(minutes=0, seconds=0, microseconds=0)),
             stamp) == stamp
    assert f(dict(delta=dict(hours=0, minutes=0, seconds=0, microseconds=0)),
             stamp) == stamp
    assert f(dict(delta=dict(days=0,
                             hours=0, minutes=0, seconds=0, microseconds=0)),
             stamp) == stamp
    assert f(dict(delta=dict(months=0, days=0,
                             hours=0, minutes=0, seconds=0, microseconds=0)),
             stamp) == stamp
    assert f(dict(delta=dict(years=0, months=0, days=0,
                             hours=0, minutes=0, seconds=0, microseconds=0)),
             stamp) == stamp
    assert f(dict(delta=dict(seconds=0)), stamp) == \
        datetime.datetime(1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc)
    assert f(dict(delta=dict(minutes=0)), stamp) == \
        datetime.datetime(1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    assert f(dict(delta=dict(hours=0)), stamp) == \
        datetime.datetime(1, 2, 3, 4, tzinfo=datetime.timezone.utc)
    assert f(dict(delta=dict(days=0)), stamp) == \
        datetime.datetime(1, 2, 3, tzinfo=datetime.timezone.utc)
    assert f(dict(delta=dict(months=0)), stamp) == \
        datetime.datetime(1, 2, 1, tzinfo=datetime.timezone.utc)
    assert f(dict(delta=dict(years=0)), stamp) == min_stamp

    assert f(
        dict(delta=dict(years=0), stamp=stamp_str),
        min_stamp
    ) == min_stamp
    assert f(
        dict(delta=dict(years=0, months=1, days=2), stamp=stamp_str),
        min_stamp
    ) == min_stamp

    assert f(
        dict(delta=dict(months=6)),
        datetime.datetime(2023, 12, 15, 15, 52, 24, 204547,
                          tzinfo=datetime.timezone.utc)
    ) == datetime.datetime(2023, 6, 1, tzinfo=datetime.timezone.utc)

    assert f(
        dict(delta=dict(months=2)),
        datetime.datetime(2023, 3, 31, 21, 11, 1, tzinfo=datetime.timezone.utc)
    ) == datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)

    assert f(
        dict(delta=dict(months=3)),
        datetime.datetime(2023, 4, 3, 2, 1, tzinfo=datetime.timezone.utc)
    ) == datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
