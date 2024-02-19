import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar, get_args

from pydantic import BaseModel, Field, create_model, validate_call


def column_field(primary_key: bool = False, **kwargs):
    return Field(json_schema_extra={"primary_key": primary_key}, **kwargs)


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
            field_statement = (
                f"{field} {_get_type(table.model_fields[field].annotation)}"
            )
            if json_schema := table.model_fields[field].json_schema_extra:
                if json_schema.get("primary_key"):  # type: ignore
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


T = TypeVar("T")


class Column:
    def __init__(self, type_: T):
        self.type = type_

    def __call__(self, name: str, val: T) -> "Column":
        self.name = name
        self.val = val
        return self

    def __str__(self) -> str:
        return f"Column({self.name}, {self.val}, {self.type})"  # type: ignore

    def __eq__(self, other: "Column") -> "Column": # type: ignore
        raise NotImplementedError("EQ is not yet implemented, but will be")

    def __or__(self, other: "Column") -> "Column":
        raise NotImplementedError("OR is not yet implemented, but will be")

    def __and__(self, other: "Column") -> "Column":
        raise NotImplementedError("AND is not yet implemented, but will be")

    def __class_getitem__(cls, item: T) -> "Column":
        return cls(item)


class Table(ABC):
    def __init_subclass__(cls, *args, **kwargs):
        for ann, val in cls.__annotations__.items():
            setattr(cls, ann, val)
        return super().__new__(cls)

    def __init__(self, **kwargs):
        if (
            len(kwargs) != len(self.__annotations__)
            and len(kwargs) != len(self.__annotations__) - 1
        ):
            raise TypeError(
                f"Expected {len(self.__annotations__)} arguments, got {len(kwargs)}"
            )
        for key, value in kwargs.items():
            col = self.__annotations__[key](name=key, val=value)


class User(Table):
    id: Column[int]
    name: Column[str]


def remove_database():
    import os

    os.remove("test.db")


if __name__ == "__main__":
    # remove_database()
    # engine = Engine("test.db")
    # engine.push(User)
    usr1 = User(id="som", name="smt")
    print(User.name & User.name )
