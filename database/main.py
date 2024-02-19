import sqlite3
from abc import ABC
from typing import Any

from pydantic import BaseModel, Field

def column_field(primary_key: bool = False, **kwargs):
    return Field(json_schema_extra={"primary_key": primary_key},**kwargs)

def _get_type(field: Any) -> str:
    """Returns the SQLite affinity for the given field, if it cannot be inferred,
    it will be returned as BLOB, which will store it exactly as it is passed, i.e. it's __repr__ or __str__ function.
    """
    print(field)
    if field.__name__ == "int":
        return "INTEGER"
    if field.__name__ == "str":
        return "TEXT"
    if field.__name__ == "float":
        return "REAL"
    if field.__name__ == "bool":
        return "INTEGER"
    return "BLOB"

class Engine:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self._tables: list[type["Table"]] = []

    def _check_if_table_exists(self, table: type["Table"]) -> bool:
        if table in self._tables:
            return True
        statement = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table.__name__.lower()}';"
        result = self.conn.execute(statement).fetchone()
        if result:
            return True
        return False

    @property
    def tables(self) -> list[type["Table"]]:
        return self._tables
    
    def _create_table(self, table: type["Table"]):
        statement = f"CREATE TABLE {table.table_name}"
        fields = []
        for field in table.model_fields:
            field_statement = f"{field} {_get_type(table.model_fields[field].annotation)}"
            if json_schema := table.model_fields[field].json_schema_extra:
                if json_schema.get("primary_key"): # type: ignore
                    field_statement += " PRIMARY KEY"
            fields.append(field_statement)
        statement += f"({', '.join(fields)});"
        print(statement)
        self.conn.execute(statement)
        self.conn.commit()

    def push(self, table: type["Table"]):
        if not self._check_if_table_exists(table):
            self._tables.append(table)
            self._create_table(table)



class Table(BaseModel, ABC):
    
    def __class_getitem__(cls, item):
        return item

    @property
    def table_name(self) -> str:
        return self.__class__.__name__.lower()
    
    def _get_insert_value_statement(self) -> str:
        values = self.model_dump()
        fields = [values[field] for field in self.model_fields]
        return str(tuple(fields))

    def _get_insert_prefix(self) -> str:
        return f"INSERT INTO {self.table_name} VALUES"


class User(Table):
    id: int = column_field(primary_key=True)
    name: str


def remove_database():
    import os

    os.remove("test.db")


if __name__ == "__main__":
    remove_database()
    engine = Engine("test.db")
    engine.push(User)
    usr1 = User(id=1, name="John")