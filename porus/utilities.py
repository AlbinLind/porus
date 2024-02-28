"""Utility functions."""
from typing import Any


def _get_type(field: Any) -> str:
    """Returns the SQLite affinity for the given field, if it cannot be inferred,
    it will be returned as BLOB, which will store it exactly as it is passed, i.e. it's __repr__ or
    __str__ function.
    """
    if field.__name__ == "int":
        return "INTEGER"
    if field.__name__ == "str":
        return "TEXT"
    if field.__name__ == "float":
        return "REAL"
    if field.__name__ == "bool":
        return "INTEGER"
    return "BLOB"


def _convert_values(values: list[Any]) -> list[Any]:
    """Convert the values to a string type if SQLite3 cannot automatically interpret the type.
    SQLite 3 can interpret the following types: None, int, float, str, bytes.
    If the type is not one of these types, it will be converted to a string.
    The exception is bool, which will be converted to an int.
    """
    for i, value in enumerate(values):
        if isinstance(value, (str, int, float, bytes, type(None))):
            continue
        if isinstance(value, bool):
            values[i] = int(value)
            continue
        values[i] = str(value)
    return values
