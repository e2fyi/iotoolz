"""Wrapper over cytoolz and toolz."""
try:
    import cytoolz as toolz  # pylint: disable=unused-import
except ImportError:
    import toolz  # noqa pylint: disable=unused-import
