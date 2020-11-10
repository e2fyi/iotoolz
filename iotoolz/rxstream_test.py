import json

import rx

from iotoolz.rxstream import rxstream
from iotoolz.streams import open_stream


def test_rxstream(tmpdir):

    sink = open_stream(tmpdir / "data.ndjson", "w")
    data = [{"a": 1}, {"a": 2}]

    rx.from_iterable(data).subscribe(
        on_next=lambda line: sink.write(json.dumps(line)), on_completed=sink.close
    )

    rxstream(tmpdir / "data.ndjson").subscribe(print)
    assert False
