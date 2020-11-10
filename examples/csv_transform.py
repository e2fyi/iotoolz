from iotoolz.transforms import csv_transform
from iotoolz.streams import open_stream
from iotoolz.rxstream import rxstream_from

with open_stream(
    "https://ghcdn.rawgit.org/curran/data/gh-pages/bokeh/AAPL.csv", "rU"
) as stream:
    rxstream_from(stream).pipe(csv_transform()).subscribe(
        on_next=print,
        on_error=print,
        on_completed=print
    )
