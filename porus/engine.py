from porus.column import Column, SetStatement
from porus.statement.delete import Delete
from porus.statement.query import Query
from porus.statement.update import Update
from porus.utilities import _get_type
from porus.utilities import _convert_values
from porus.table import Table


import sqlite3
from typing import  Any, Union


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
                if table.model_fields[field].json_schema_extra.get("primary_key"):  # type: ignore
                    column_statement += " PRIMARY KEY"
            columns.append(column_statement)
        statement += f"({', '.join(columns)})"
        self.conn.execute(statement)
        self.conn.commit()

    def push(self, table: type["Table"]):
        if not self._check_if_table_exists(table):
            self._create_table(table)

    def _convert_row_to_object(
        self, table: Union["Table", type["Table"]], row: tuple[Any]
    ) -> "Table":
        fields = table.model_fields
        if len(row) != len(fields):
            raise ValueError(
                f"Number of columns in the row ({len(row)}) does not match the number of fields in the table ({len(fields)})."
            )
        if isinstance(table, Table):
            for i, field in enumerate(fields):
                if getattr(table, field) != row[i]:
                    setattr(table, field, row[i])
            return table
        if isinstance(table, type):
            return table(**{field: row[i] for i, field in enumerate(fields)})
        raise ValueError(
            f"Table is neither a Table nor a type, it is {type(table)}, which is not supported."
        )

    def insert(self, objs: list["Table"]) -> list[Any]:
        """Add the objects to the database and return the objects.
        It automatically commits the changes."""
        row_list = []
        for obj in objs:
            keys = []
            values = []
            for key, value in obj.model_dump().items():
                if obj.model_fields[key].json_schema_extra:
                    if (
                        obj.model_fields[key].json_schema_extra.get("primary_key")  # type: ignore
                        and not value
                    ):
                        continue
                keys.append(str(key))
                values.append(value)

            statement = f"INSERT INTO {obj.table_name} ({', '.join(keys)}) VALUES ({', '.join(['?' for _ in range(len(values))])}) RETURNING *;"
            values = _convert_values(values)
            result = self.conn.execute(statement, values).fetchone()
            row_list.append(self._convert_row_to_object(obj, result))
        self.conn.commit()
        return row_list

    def query(self, *select: Union["Column", type["Table"]]) -> Query:
        if len(select) == 1:
            if isinstance(select[0], Column):
                return Query(table_or_subquery=[select[0]], engine=self)
            if issubclass(select[0], Table):
                return Query(table_or_subquery=select[0], engine=self)
            raise ValueError("The select parameter must be a Column or a Table.")
        if not all(isinstance(x, Column) for x in select):
            raise ValueError("All elements in the select list must be of type Column.")
        return Query(table_or_subquery=list(select), engine=self)  # type: ignore

    def update(self, *update: SetStatement) -> Update:
        if not all(isinstance(x, SetStatement) for x in update):
            raise ValueError("All elements in the update list must be of type SetStatement, if you want to replace objects, use engine.replace().")
        return Update(table_or_subquery=list(update), engine=self)

    def delete(self, table: type["Table"]) -> "Delete":
        return Delete(table_or_subquery=table, engine=self)