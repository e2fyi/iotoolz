from iotoolz.http import HttpStream
from iotoolz.temp import TempStream

buffer = TempStream("tmp://google.com", mode="w")

with HttpStream("https://google.com") as source, open("tmp.html", "w") as sink:
    buffer.pipe(sink, text_mode=True)
    source.pipe(buffer)
