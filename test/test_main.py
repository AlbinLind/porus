import pytest
from database.main import Table, Column, remove_database, User, Engine

def test_create_table():
    class A(Table):
        id: Column[int]
        something: Column[str]

    _ = A(id=1, something="hello")        
    
    with pytest.raises(TypeError):
        _ = A()

    with pytest.raises(TypeError):
        _ = A(id="smt")

    _ = A(id="1", something="sdfo")

def test_add_database():
    engine = Engine(":memory:")
    engine.conn.execute("SELECT * FROM sqlite_master")
