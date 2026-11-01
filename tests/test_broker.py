"""The dev-shell broker tool, over the in-memory broker."""
import pytest
from fakes import FakeConsumer

from citibike_contract import TOPIC_DLQ, TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER
from citibike_producer import broker

ALL = {TOPIC_STATUS: 3, TOPIC_INFO: 1, TOPIC_WEATHER: 1, TOPIC_POLL_LOG: 1, TOPIC_DLQ: 1}


def test_required_topics_match_what_setup_creates():
    assert sorted(broker.REQUIRED_TOPICS) == sorted(ALL)


def test_check_passes_when_every_topic_exists(capsys):
    fc = FakeConsumer(partitions=ALL)
    assert broker.main(["check"], consumer=fc) == 0
    assert fc.closed
    assert "ok: all 5 topics exist" in capsys.readouterr().out


def test_check_fails_and_names_missing_topics(capsys):
    fc = FakeConsumer(partitions={TOPIC_STATUS: 3})
    assert broker.main(["check"], consumer=fc) == 1
    out = capsys.readouterr().out
    assert "MISSING" in out and TOPIC_POLL_LOG in out and "citibike_setup" in out


def test_check_reports_an_unreachable_broker_without_a_traceback(capsys):
    class Down(FakeConsumer):
        def list_topics(self, topic=None, timeout=0):
            raise RuntimeError("Failed to get metadata: Local: Broker transport failure")

    fc = Down()
    assert broker.main(["check"], consumer=fc) == 1
    assert fc.closed
    assert "UNREACHABLE" in capsys.readouterr().out


def _with_messages(n_per_partition, partitions=3):
    fc = FakeConsumer(partitions={TOPIC_STATUS: partitions})
    for p in range(partitions):
        for i in range(n_per_partition):
            fc.append(TOPIC_STATUS, p, "s%d" % i, b'{"i": %d}' % i)
    return fc


def test_peek_oldest_reads_count_per_partition():
    msgs = broker.peek(_with_messages(5), TOPIC_STATUS, count=2)
    assert [(p, o) for p, o, _k, _v in msgs] == [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)]
    assert msgs[0][2:] == ("s0", '{"i": 0}')


def test_peek_newest_and_short_partitions():
    fc = _with_messages(5, partitions=1)
    assert [o for _p, o, _k, _v in broker.peek(fc, TOPIC_STATUS, count=2, newest=True)] == [3, 4]
    assert len(broker.peek(_with_messages(1, partitions=1), TOPIC_STATUS, count=3)) == 1
    assert broker.peek(FakeConsumer(partitions={TOPIC_STATUS: 2}), TOPIC_STATUS, count=3) == []


def test_peek_missing_topic_says_how_to_fix():
    with pytest.raises(RuntimeError, match="citibike_setup"):
        broker.peek(FakeConsumer(), TOPIC_STATUS, count=1)


def test_peek_cli_prints_one_line_per_message(capsys):
    fc = _with_messages(2, partitions=1)
    assert broker.main(["peek", TOPIC_STATUS, "--count", "1", "--newest"], consumer=fc) == 0
    assert capsys.readouterr().out.strip() == 'partition=0 offset=1 key=s1 {"i": 1}'
