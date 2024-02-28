"""Tests the delete functionality of the porus ORM."""
from porus.column import ColumnField
from porus.engine import Engine
from porus.table import Table


class A(Table):
    """Test table for the delete tests."""
    id: int = ColumnField(primary_key=True)
    num: int
    string: str


def test_delete_one():
    """Tests deleting one row from the table, and checks that it was deleted."""
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where(A.c.id == a.id).all()
    res = engine.query(A.c.id).all()
    assert res == [(b.id,)]


def test_delete_all():
    """Tests deleting all rows from the table."""
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).all()
    res = engine.query(A.c.id).all()
    assert res == []


def test_delete_returning():
    """Tests that the returning clause works when returning a column."""
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where(A.c.id == a.id).returning(A.c.id).all()
    assert res == [(a.id,)]
    res = engine.query(A.c.id).all()
    assert res == [(b.id,)]

def test_delete_returning_object():
    """Tests deleting a row and returning the object that was deleted."""
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where(A.c.id == a.id).returning().all()
    assert res == [a]
    res = engine.query(A).all()
    assert res == [b]

def test_delete_complex_where():
    """Tests deleting with a more complex where clause."""
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where((A.c.id == a.id) | (A.c.num == 2)).all()
    res = engine.query(A.c.id).all()
    assert res == []
