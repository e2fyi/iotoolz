import importlib.util

PANDAS_EXISTS = importlib.util.find_spec("pandas") is not None
