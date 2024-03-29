"""Porus is a simple ORM for SQLite3."""
import os
from typing import Any

from porus.column import ColumnField
from porus.engine import Engine
from porus.table import Table


class User(Table):
    """Test table."""

    id: int = ColumnField(primary_key=True)
    name: str


def remove_database() -> None:
    """Helper function to remove an existing database."""
    os.remove("test.db")


if __name__ == "__main__":
    remove_database()
    engine = Engine("test.db")
    engine.push(User)
    usr1 = User(name="smt")
    engine.insert([usr1])
    res: list[Any] = engine.query(User.c.name).all()
    usr1.name = "smt2"
