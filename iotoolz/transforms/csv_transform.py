import csv
import functools
from typing import List, Optional, Sequence

import rx
import rx.operators as ops
from rx.core.typing import Disposable, Observer, Scheduler


def csv_transform(
    num_samples: int = 10,
    headers: Sequence[str] = None,
    delimiter=",",
    doublequote: bool = True,
    escapechar: str = "\\",
    quotechar: str = '"',
    quoting: int = csv.QUOTE_MINIMAL,
    skipinitialspace: bool = False,
    strict: bool = True,
):
    """
    [summary]

    See https://docs.python.org/3.6/library/csv.html#csv-fmt-params

    Args:
        num_samples (int, optional): [description]. Defaults to 10.
        headers (Sequence[str], optional): [description]. Defaults to None.
        delimiter (str, optional): [description]. Defaults to ",".
        doublequote (bool, optional): [description]. Defaults to True.
        escapechar (str, optional): [description]. Defaults to None.
        quotechar (str, optional): [description]. Defaults to '"'.
        quoting (int, optional): [description]. Defaults to csv.QUOTE_MINIMAL.
        skipinitialspace (bool, optional): [description]. Defaults to False.
        strict (bool, optional): [description]. Defaults to True.
    """

    def csv_stream(source: rx.core.observable.observable.Observable):
        reader = functools.partial(
            csv.reader,
            delimiter=delimiter,
            doublequote=doublequote,
            escapechar=escapechar,
            quoting=quoting,
            strict=strict,
            skipinitialspace=skipinitialspace,
            quotechar=quotechar,
        )
        drop_first_line = False

        def sniff(items: List[str]):
            nonlocal reader
            nonlocal headers
            nonlocal drop_first_line
            sniffer = csv.Sniffer()
            samples = "\n".join(items)
            dialect = sniffer.sniff(samples)
            drop_first_line = sniffer.has_header(samples)
            headers = tuple(
                next(
                    iter(csv.DictReader(items, fieldnames=headers, dialect=dialect))
                ).keys()
            )

        if drop_first_line:
            source = source.pipe(ops.skip(1))

        source.pipe(ops.buffer_with_count(num_samples), ops.take(1)).subscribe(
            on_next=sniff
        ).dispose()

        def subscribe(observer: Observer, scheduler: Optional[Scheduler]) -> Disposable:
            def on_next(items: List[str]):
                for record in reader(items):  # pylint: disable=not-callable
                    observer.on_next(dict(zip(headers, record)))

            return source.pipe(ops.buffer_with_count(100)).subscribe(
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                on_next=on_next,
                scheduler=scheduler,
            )

        return rx.create(subscribe)

    return csv_stream
