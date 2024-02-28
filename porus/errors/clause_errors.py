"""Errors for the clause module."""


class InvalidOperationError(Exception):
    """Raised when an invalid operation is performed. For example, using ORDER BY in a DELETE."""
