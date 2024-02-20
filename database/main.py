import sqlite3
from typing import Any

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from pydantic._internal._model_construction import ModelMetaclass


def ColumnField(default: Any = None, *, primary_key: bool = False, **kwargs) -> Any:
    """A wrapper function for pydantic's Field, which adds a primary_key parameter to the json_schema_extra parameter."""
    return Field(
        default=default, json_schema_extra={"primary_key": primary_key}, **kwargs
    )


def _get_type(field: Any) -> str:
    """Returns the SQLite affinity for the given field, if it cannot be inferred,
    it will be returned as BLOB, which will store it exactly as it is passed, i.e. it's __repr__ or __str__ function.
    """
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

    def _check_if_table_exists(self, table: type["Table"]) -> bool:
        statement = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table.__name__.lower()}';"
        result = self.conn.execute(statement).fetchone()
        if result:
            return True
        return False

    def _create_table(self, table: type["Table"]):
        statement = f"CREATE TABLE {table.table_name}"
        columns = []
        for field in table.model_fields:
            column_statement = (
                f"{field} {_get_type(table.model_fields[field].annotation)}"
            )
            if table.model_fields[field].json_schema_extra:
                if table.model_fields[field].json_schema_extra.get("primary_key"): # type: ignore
                    column_statement += " PRIMARY KEY"
            columns.append(column_statement)
        statement += f"({', '.join(columns)})"
        self.conn.execute(statement)
        self.conn.commit()

    def push(self, table: type["Table"]):
        if not self._check_if_table_exists(table):
            self._create_table(table)

    def _convert_row_to_object(self, table: "Table", row: tuple[Any]) -> "Table":
        fields = table.model_fields
        if len(row) != len(fields):
            raise ValueError(f"Number of columns in the row ({len(row)}) does not match the number of fields in the table ({len(fields)}).")
        for i, field in enumerate(fields):
            if getattr(table, field) != row[i]:
                setattr(table, field, row[i])
        return table

    def insert(self, objs: list["Table"]) -> list["Table"]:
        """Add the objects to the database and return the objects.
        It automatically commits the changes."""
        row_list = []
        for obj in objs:
            keys = []
            values = []
            for key, value in obj.model_dump().items():
                if obj.model_fields[key].json_schema_extra:
                    if obj.model_fields[key].json_schema_extra.get("primary_key") and not value:  # type: ignore
                        continue
                keys.append(str(key))
                values.append(str(value))

            statement = f"INSERT INTO {obj.table_name} ({', '.join(keys)}) VALUES ({', '.join(['?' for _ in range(len(values))])}) RETURNING *;"
            result = self.conn.execute(statement, values).fetchone()
            row_list.append(self._convert_row_to_object(obj, result))
        self.conn.commit()
        return row_list


class Column:
    def __init__(self, field: FieldInfo):
        self.field: FieldInfo = field

    def __repr__(self):
        return f"Column(type={self.field.annotation})"


class GenericColumn:
    def __init__(self, table: type["Table"]):
        self.table = table

    def __getattr__(self, name: str):
        if name not in self.table.model_fields:
            raise AttributeError(f"Column {name} not found in {self.table.__name__}")
        return Column(field=self.table.model_fields[name])


class TableMeta(ModelMetaclass):
    def __getattr__(self, name):  # type: ignore
        if name == "_" or name == "c":
            return GenericColumn(self)
        return super().__getattr__(name)  # type: ignore


class Table(BaseModel, metaclass=TableMeta):
    def __init_subclass__(cls, **kwargs):
        setattr(cls, "table_name", cls.__name__.lower())

    @property
    def table_name(self):
        return self.__class__.__name__.lower()


class User(Table):
    id: int = ColumnField(primary_key=True)
    name: str


def remove_database():
    import os

    os.remove("test.db")


if __name__ == "__main__":
    remove_database()
    engine = Engine("test.db")
    engine.push(User)
    usr1 = User(name="smt")
    engine.insert([usr1])
