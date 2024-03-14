"""Test the relationship between tables."""
import sqlite3

import pytest

from porus import ColumnField, Engine, Table


class Artist(Table):
    id: int = ColumnField(primary_key=True)
    name: str

class Album(Table):
    id: int = ColumnField(primary_key=True)
    title: str
    artist_id: int = ColumnField(foreign_key=Artist.c.id)

def test_relationship_creation():
    """Test that a foreign key constraint is created when a table is pushed to the database."""
    engine = Engine(":memory:")
    engine.push(Artist)
    engine.push(Album)

    artist1 = Artist(name="Foo")
    artist2 = Artist(name="Bar")
    engine.insert([artist1, artist2])

    album1 = Album(title="Fizz", artist_id=artist1.id)
    engine.insert([album1])

    res: list[Album] = engine.query(Album).where(Album.c.artist_id == artist1.id).all()

    assert len(res) == 1
    assert res[0].title == "Fizz"

def test_relationship_not_exists():
    """Test that a foreign key constraint is enforced."""
    engine = Engine(":memory:")
    engine.push(Artist)
    engine.push(Album)
    engine.conn.execute("PRAGMA foreign_keys = ON;").fetchall()

    artist1 = Artist(name="Foo")
    engine.insert([artist1])

    album1 = Album(title="Fizz", artist_id=artist1.id + 1)

    with pytest.raises(sqlite3.IntegrityError):
        engine.insert([album1])

