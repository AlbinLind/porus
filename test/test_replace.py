"""Tests for the replace method on the engine."""
from porus.engine import Engine
from porus.table import Table
from porus.column import ColumnField

class TestTable(Table):
    """A test table."""
    id: int = ColumnField(primary_key=True)
    column_one: str
    column_two: int

def test_replacing_one():
    """Tests replacing one row in the table."""
    engine = Engine(":memory:")
    engine.push(TestTable)

    obj_a = TestTable(column_one="Hello World!", column_two=42)
    obj_b = TestTable(column_one="Goodbye World!", column_two=-123)

    obj_a, obj_b = engine.insert([obj_a, obj_b])
    obj_a_inital_id = obj_a.id
    obj_a.column_one = "Once upon a time"

    obj_a = engine.replace(obj_a)
    in_db_obj_a = engine.query(TestTable).where(TestTable.c.id == obj_a_inital_id).all()
    assert in_db_obj_a[0].column_one == "Once upon a time"
    assert in_db_obj_a[0].column_two == 42
    assert in_db_obj_a[0].id == obj_a_inital_id

def test_replacing_many():
    """Tests replacing many rows in the table."""
    engine = Engine(":memory:")
    engine.push(TestTable)

    obj_a = TestTable(column_one="Hello World!", column_two=42)
    obj_b = TestTable(column_one="Goodbye World!", column_two=-123)

    obj_a, obj_b = engine.insert([obj_a, obj_b])
    obj_a_inital_id = obj_a.id
    obj_b_inital_id = obj_b.id
    obj_a.column_one = "Once upon a time"
    obj_b.column_two = 123

    obj_a, obj_b = engine.replace(obj_a, obj_b)
    in_db_obj_a = engine.query(TestTable).where(TestTable.c.id == obj_a_inital_id).all()
    in_db_obj_b = engine.query(TestTable).where(TestTable.c.id == obj_b_inital_id).all()
    assert in_db_obj_a[0].column_one == "Once upon a time"
    assert in_db_obj_a[0].column_two == 42
    assert in_db_obj_a[0].id == obj_a_inital_id
    assert in_db_obj_b[0].column_one == "Goodbye World!"
    assert in_db_obj_b[0].column_two == 123
    assert in_db_obj_b[0].id == obj_b_inital_id