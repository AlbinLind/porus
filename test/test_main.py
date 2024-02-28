"""Tests the main functionality of the library, including select statement
and creating and inserting table.
"""
import pytest
from pydantic import ValidationError

from porus.column import ColumnField
from porus.engine import Engine
from porus.main import User
from porus.table import Table


def test_create_table():
    """Tests creating a table with the porus ORM."""

    class A(Table):
        id: int
        something: str

    _ = A(id=1, something="hello")

    with pytest.raises(ValidationError):
        _ = A()

    _ = A(id="1", something="sdfo")


def test_create_engine():
    """Tests setting up the engine."""
    engine = Engine(":memory:")
    engine.conn.execute("SELECT * FROM sqlite_master")


def test_add_table():
    """Tests adding a table to the database."""
    engine = Engine(":memory:")
    engine.push(User)
    res = engine.conn.execute("SELECT * FROM sqlite_master").fetchone()
    assert res is not None


def test_add_single_row():
    """Tests adding a single row to the database."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    engine.insert([usr])
    res = engine.conn.execute("SELECT count(*) FROM User").fetchone()
    assert res[0] == 1


def test_add_multiple_rows():
    """Tests adding multiple rows to the database."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    engine.insert([usr, usr2])
    res = engine.conn.execute("SELECT count(*) FROM User").fetchone()
    assert res[0] == 2


def test_inserting_returns_same_object():
    """Tests that the insert method returns the same object that was inserted."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    res = engine.insert([usr])
    assert id(res[0]) == id(usr)


def test_query_single_row():
    """Tests querying a single row from the database."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    engine.insert([usr])
    res = engine.query(User).all()
    assert len(res) == 1
    assert isinstance(res[0], User)


def test_query_with_limit():
    """Tests querying with a limit clause."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    engine.insert([usr, usr2])
    res = engine.query(User).limit(1).all()
    assert len(res) == 1
    assert isinstance(res[0], User)
    assert res[0].name == usr.name


def test_query_with_offset():
    """Tests querying with an offset clause."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    engine.insert([usr, usr2])
    res = engine.query(User).offset(1).all()
    assert len(res) == 1
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name


def test_query_with_limit_and_offset():
    """Tests querying with a limit and offset clause."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res = engine.query(User).limit(1).offset(1).all()
    assert len(res) == 1
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name


def test_query_with_where():
    """Tests querying with a where clause."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res = engine.query(User).where(User.c.name == "Name number 2").all()
    assert len(res) == 1
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name


def test_query_with_multiple_where():
    """Tests querying with a where clause."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = (
        engine.query(User)
        .where((User.c.name == "Name number 2") | (User.c.name == "Name number 3"))
        .all()
    )
    assert len(res) == 2
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name
    assert res[1].name == usr3.name


def test_column_query_function_sum():
    """Tests the sum function in a query."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine = Engine(":memory:")
    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.sum()).all()
    assert res[0][0] == 6


def test_column_query_function_avg():
    """Tests the avg function in a query."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine = Engine(":memory:")
    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.avg()).all()
    assert res[0][0] == 1.5


def test_column_query_function_max():
    """Tests the max function in a query."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine = Engine(":memory:")
    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.max()).all()
    assert res[0][0] == 3


def test_column_query_function_min():
    """Tests the min function in a query."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine = Engine(":memory:")
    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.min()).all()
    assert res[0][0] == 0


def test_column_query_function_count():
    """Tests the count function in a query."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine = Engine(":memory:")
    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.count()).all()
    assert res[0][0] == 4


def test_order_by_query():
    """Tests the order by clause in a query."""
    engine = Engine(":memory:")

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine.push(A)
    a = A(number=1)
    a2 = A(number=2)
    a3 = A(number=3)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res: list[A] = engine.query(A).order_by(A.c.number).all()
    assert len(res) == 4
    assert isinstance(res[0], A)
    assert res[0].number == a4.number
    assert res[1].number == a.number
    assert res[2].number == a2.number
    assert res[3].number == a3.number


def test_order_by_query_desc():
    """Tests the order by clause in a query with descending order."""
    engine = Engine(":memory:")

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int

    engine.push(A)
    a = A(number=1)
    a2 = A(number=3)
    a3 = A(number=2)
    a4 = A(number=0)
    engine.insert([a, a2, a3, a4])
    res: list[A] = engine.query(A).order_by(A.c.number, ascending=False).all()
    assert len(res) == 4
    assert isinstance(res[0], A)
    assert res[0].number == a2.number
    assert res[1].number == a3.number
    assert res[2].number == a.number
    assert res[3].number == a4.number


def test_group_by_query():
    """Tests the group by clause in a query."""
    engine = Engine(":memory:")

    class A(Table):
        id: int = ColumnField(primary_key=True)
        number: int
        name: str

    engine.push(A)
    a = A(number=1, name="A")
    a2 = A(number=2, name="A")
    a3 = A(number=3, name="B")
    a4 = A(number=0, name="B")
    engine.insert([a, a2, a3, a4])
    res = engine.query(A.c.number.count(), A.c.name).group_by(A.c.name).all()
    assert len(res) == 2
    assert res[0][0] == 2
    assert res[0][1] == "A"
    assert res[1][0] == 2
    assert res[1][1] == "B"


def test_ne_query():
    """Tests the not equal operator in a query."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = engine.query(User).where(User.c.name != "Name number 2").all()
    assert len(res) == 2
    assert isinstance(res[0], User)
    assert res[0].name == usr.name
    assert res[1].name == usr3.name


def test_IN_SQL_command():  # noqa: N802
    """Tests the IN SQL command."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = engine.query(User).where(User.c.name["Name number 2", "Name number 3"]).all()
    assert len(res) == 2
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name
    assert res[1].name == usr3.name


def test_COMPLEX_SQL_command():  # noqa: N802
    """Tests the complex SQL command with IN and NOT IN."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = (
        engine.query(User)
        .where(User.c.name["Name number 2", "Name number 3"] & (User.c.name != "Name number 2"))
        .all()
    )
    assert len(res) == 1
    assert isinstance(res[0], User)
    assert res[0].name == usr3.name


def test_COMPLEX_SQL_command_2():  # noqa: N802
    """Tests the complex SQL command with IN and NOT IN."""

    class A(Table):
        id: int = ColumnField(primary_key=True)
        name: str
        boolean: bool
        somefloat: float

    engine = Engine(":memory:")
    engine.push(A)
    a = A(name="Hello", boolean=True, somefloat=1.0)
    a2 = A(name="Name number 2", boolean=False, somefloat=2.0)
    a3 = A(name="Name number 3", boolean=True, somefloat=3.0)
    a4 = A(name="Name number 4", boolean=False, somefloat=4.0)
    a5 = A(name="Name number 3", boolean=False, somefloat=5.0)
    engine.insert([a, a2, a3, a4, a5])
    res: list[A] = (
        engine.query(A)
        .where(
            A.c.name["Name number 2", "Name number 3"]
            & (A.c.name != "Name number 2")
            & (A.c.boolean == True)  # noqa: E712
        )  # noqa: E712
        .all()
    )
    assert len(res) == 1
    assert res[0].name == a3.name
    assert res[0].boolean == a3.boolean
    assert res[0].somefloat == a3.somefloat


def test_query_with_only_one_column():
    """Tests querying with only one column."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[tuple[str]] = engine.query(User.c.name).all()
    assert len(res) == 3
    assert isinstance(res[0][0], str)
    assert res[0][0] == usr.name


def test_query_with_two_columns():
    """Tests querying with two columns."""
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res = engine.query(User.c.name, User.c.id).all()
    assert len(res) == 3
    assert isinstance(res[0], tuple)
    assert res[0] == (usr.name, usr.id)
