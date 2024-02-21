from pydantic import ValidationError
import pytest
from database.main import Table, remove_database, User, Engine


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
