"""Porus is a simple ORM for SQLite3."""
import os
import sqlite3
from typing import Any

from porus.column import ColumnField
from porus.engine import Engine
from porus.table import Table


class User(Table):
    """Test table."""

    id: int = ColumnField(primary_key=True)
    name: str

class Comment(Table):
    """Test table."""

    id: int = ColumnField(primary_key=True)
    comment: str
    user_id: int = ColumnField(foreign_key=User.c.id)

def remove_database() -> None:
    """Helper function to remove an existing database."""
    if os.path.exists("test.db"):
        os.remove("test.db")


if __name__ == "__main__":
    remove_database()
    engine = Engine("test.db")
    engine.conn.execute("PRAGMA foreign_keys = ON;").fetchall()
    engine.push(User)
    engine.push(Comment)

    usr1 = User(name="smt")
    engine.insert([usr1])
    comment2 = Comment(comment="ehllo", user_id=usr1.id)
    engine.insert([ comment2])
    print(engine.query(User).all())
    print(engine.query(Comment).all())