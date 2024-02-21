from pydantic import ValidationError
import pytest
from database.main import ColumnField, Table, User, Engine


def test_create_table():
    class A(Table):
        id: int
        something: str

    _ = A(id=1, something="hello")

    with pytest.raises(ValidationError):
        _ = A()

    _ = A(id="1", something="sdfo")


def test_add_database():
    engine = Engine(":memory:")
    engine.conn.execute("SELECT * FROM sqlite_master")


def test_add_table():
    engine = Engine(":memory:")
    engine.push(User)
    res = engine.conn.execute("SELECT * FROM sqlite_master").fetchone()
    assert res is not None


def test_add_single_row():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    engine.insert([usr])
    res = engine.conn.execute("SELECT count(*) FROM User").fetchone()
    assert res[0] == 1


def test_add_multiple_rows():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    engine.insert([usr, usr2])
    res = engine.conn.execute("SELECT count(*) FROM User").fetchone()
    assert res[0] == 2


def test_inserting_returns_same_object():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    res = engine.insert([usr])
    assert id(res[0]) == id(usr)


def test_query_single_row():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    engine.insert([usr])
    res = engine.query(User).all()
    assert len(res) == 1
    assert isinstance(res[0], User)


def test_query_with_limit():
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

def test_ne_query():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = (
        engine.query(User)
        .where(User.c.name != "Name number 2")
        .all()
    )
    assert len(res) == 2
    assert isinstance(res[0], User)
    assert res[0].name == usr.name
    assert res[1].name == usr3.name

def test_IN_SQL_command():
    engine = Engine(":memory:")
    engine.push(User)
    usr = User(name="Hello")
    usr2 = User(name="Name number 2")
    usr3 = User(name="Name number 3")
    engine.insert([usr, usr2, usr3])
    res: list[User] = (
        engine.query(User)
        .where(User.c.name["Name number 2", "Name number 3"])
        .all()
    )
    assert len(res) == 2
    assert isinstance(res[0], User)
    assert res[0].name == usr2.name
    assert res[1].name == usr3.name

def test_COMPLEX_SQL_command():
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

def test_COMPLEX_SQL_command_2():
    class A(Table):
        id: int = ColumnField(primary_key=True)
        name: str
        boolean: bool
        somefloat: float

    engine = Engine(":memory:")
    engine.push(A)
    a = A( name="Hello", boolean=True, somefloat=1.0)
    a2 = A(name="Name number 2", boolean=False, somefloat=2.0)
    a3 = A(name="Name number 3", boolean=True, somefloat=3.0)
    a4 = A(name="Name number 4", boolean=False, somefloat=4.0)
    a5 = A(name="Name number 3", boolean=False, somefloat=5.0)
    engine.insert([a, a2, a3, a4, a5])
    res: list[A] = (
        engine.query(A)
        .where(A.c.name["Name number 2", "Name number 3"] & (A.c.name != "Name number 2") & (A.c.boolean == True))  # noqa: E712
        .all()
    )
    assert len(res) == 1
    assert res[0].name == a3.name
    assert res[0].boolean == a3.boolean
    assert res[0].somefloat == a3.somefloat


def test_query_with_only_one_column():
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
