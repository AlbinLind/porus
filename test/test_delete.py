from porus.column import ColumnField
from porus.table import Table
from porus.engine import Engine


class A(Table):
    id: int = ColumnField(primary_key=True)
    num: int
    string: str


def test_delete_one():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where(A.c.id == a.id).all()
    res = engine.query(A.c.id).all()
    assert res == [(b.id,)]


def test_delete_all():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).all()
    res = engine.query(A.c.id).all()
    assert res == []


def test_delete_returning():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where(A.c.id == a.id).returning(A.c.id).all()
    assert res == [(a.id,)]
    res = engine.query(A.c.id).all()
    assert res == [(b.id,)]


def test_delete_complex_where():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.delete(A).where((A.c.id == a.id) | (A.c.num == 2)).all()
    res = engine.query(A.c.id).all()
    assert res == []
