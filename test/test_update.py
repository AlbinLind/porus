from porus.column import ColumnField
from porus.table import Table
from porus.engine import Engine

class A(Table):
    id: int = ColumnField(primary_key=True)
    num: int
    string: str

# def test_update_literal():
#     engine = Engine(":memory:")
#     engine.push(A)
#     a = A(num=1, string="test")
#     b = A(num=2, string="test2")
#     a, b = engine.insert([a, b])

#     res = engine.update(A.c.num = 2).where(A.c.id == a.id).returning(A.c.num).all()
#     assert res == [(2,)]
#     res = engine.query(A.c.num).all()
#     assert res[0][0] == 2
#     assert res[1][0] == 2

def test_update_addition():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.update(A.c.num + 1).where(A.c.id == a.id).returning(A.c.num).all()
    assert res == [(2,)]
    res = engine.query(A.c.num).all()
    assert res[0][0] == 2
    assert res[1][0] == 2

def test_update_subtraction():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.update(A.c.num - 1).where(A.c.id == a.id).returning(A.c.num).all()
    assert res == [(0,)]
    res = engine.query(A.c.num).all()
    assert res[0][0] == 0
    assert res[1][0] == 2

def test_update_multiplication():
    engine = Engine(":memory:")
    engine.push(A)
    a = A(num=1, string="test")
    b = A(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.update(A.c.num * 2).where(A.c.id == a.id).returning(A.c.num).all()
    assert res == [(2,)]
    res = engine.query(A.c.num).all()
    assert res[0][0] == 2
    assert res[1][0] == 2

def test_update_division():
    engine = Engine(":memory:")
    class B(Table):
        id: int = ColumnField(primary_key=True)
        num: float
        string: str
    engine.push(B)
    a = B(num=1, string="test")
    b = B(num=2, string="test2")
    a, b = engine.insert([a, b])

    res = engine.update(B.c.num / 2.0).where(B.c.id == a.id).returning(B.c.num).all()
    assert res == [(0.5,)]
    res = engine.query(B.c.num).all()
    assert res[0][0] == 0.5
    assert res[1][0] == 2

# def test_update_user_function():
#     def my_func(a: A) -> A:
#         a.num = 2 * a.num - 3
#         return a
#     engine = Engine(":memory:")
#     engine.push(A)
#     a = A(num=1, string="test")
#     b = A(num=2, string="test2")
#     a, b = engine.insert([a, b])

#     res = engine.update(A.c, my_func).where(A.c.id == a.id).returning(A.c.num).all()
#     assert res == [(-1,)]