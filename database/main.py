from enum import Enum
import sqlite3
from typing import Any, Union

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


class QueryClause(Enum):
    SELECT = 0
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    ORDER_BY = 4
    LIMIT = 5
    OFFSET = 6


class Query:
    def __init__(self, select: list["Column"] | type["Table"], engine: "Engine"):
        self.statements: list[tuple[str, QueryClause, list[Any] | None]] = []
        self.engine = engine
        self.select = select
        # We initially assume we can return an object if we are selecting from a table
        # Otherwise, we will return a tuple of size equal to the size of the select list
        # For example, if we add a group by clause we can no longer return an object,
        # since we cannot know how it will look, and no model is defined for that.
        self.result_column = select
        self._can_return_table = isinstance(select, TableMeta)
        if isinstance(select, TableMeta):
            self._select_table(str(select.table_name))
        else:
            self._select_columns()

    def _select_table(self, table_name: str):
        self.statements.append(("SELECT *", QueryClause.SELECT, None))
        self.statements.append((f"FROM {table_name}", QueryClause.FROM, None))
        return self

    def _select_columns(self):
        raise NotImplementedError(
            "You may only select all columns from a table at this moment..."
        )

    def limit(self, limit: int):
        self.statements.append(("LIMIT ?", QueryClause.LIMIT, [limit]))
        return self

    def offset(self, offset: int):
        self.statements.append(("OFFSET ?", QueryClause.OFFSET, [offset]))
        return self

    def where(self, clause: "WhereStatement"):
        self.statements.append(
            (f"WHERE {clause.statement}", QueryClause.WHERE, clause.values)
        )
        return self

    def _validate_query(self):
        """Make sure that the query are valid, and that there are no conflicting clauses.
        The function will try and fix the query if possible.
        For example if the query contains an OFFSET clause, but no LIMIT clause, it will set the LIMIT as -1"""
        clauses = [x[1] for x in self.statements]
        if QueryClause.SELECT not in clauses:
            raise ValueError(
                "You must provide a SELECT clause. If this error is raised, something has "
                "gone wrong when creating the query, and it is an internal error."
            )
        if QueryClause.FROM not in clauses:
            raise ValueError(
                "You must provide a FROM clause. If this error is raised, something has "
                "gone wrong when creating the query, and it is an internal error."
            )
        if QueryClause.OFFSET in clauses and QueryClause.LIMIT not in clauses:
            self.limit(-1)

    def _build_statement(self) -> tuple[str, list[Any]]:
        statement = ""
        values = []
        clauses_added = set()
        self._validate_query()
        self.statements.sort(key=lambda x: x[1].value)
        for clause, clause_type, value in self.statements:
            if clause_type in clauses_added:
                raise ValueError(
                    f"Clause {clause_type} already added, you cannot provide a clause multiple times."
                )
            statement += " " + clause
            if value:
                values.extend(value)
            clauses_added.add(clause_type)
        return statement, values

    def all(self) -> list[tuple[Any]] | list["Table"]:
        """
        Retrieve all rows from the database table.

        Returns:
            list[tuple[Any]] | list[Table]: A list of objects representing the rows from the table, or a list of tuples if the return type is not a table.
        """
        statement, values = self._build_statement()
        result = self.engine.conn.execute(statement, values).fetchall()
        if self._can_return_table and isinstance(self.result_column, TableMeta):
            return [
                self.engine._convert_row_to_object(self.result_column, row)
                for row in result
            ]
        return result


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

    def query(self, select: list["Column"] | type["Table"]) -> Query:
        return Query(select, self)

    def insert(self, objs: list["Table"]) -> list["Table"]:
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
                values.append(str(value))

            statement = f"INSERT INTO {obj.table_name} ({', '.join(keys)}) VALUES ({', '.join(['?' for _ in range(len(values))])}) RETURNING *;"
            result = self.conn.execute(statement, values).fetchone()
            row_list.append(self._convert_row_to_object(obj, result))
        self.conn.commit()
        return row_list


class WhereStatement:
    def __init__(self, statement: str, values: list[Any] = []):
        self.statement = f"{statement}"
        self.values = values

    def __and__(self, other: "WhereStatement"):
        self.statement = f"({self.statement} AND {other.statement})"
        self.values.extend(other.values)
        return self

    def __or__(self, other: "WhereStatement"):
        self.statement = f"({self.statement} OR {other.statement})"
        self.values.extend(other.values)
        return self

    def __repr__(self):
        return f"WHERE {self.statement}, ({', '.join([str(value) for value in self.values])})"


class Column:
    def __init__(self, field: FieldInfo, column_name: str):
        self.field: FieldInfo = field
        self.column_name = column_name

    def __repr__(self):
        return f"Column(type={self.field.annotation})"

    def __eq__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} = ?", [other])

    def __ne__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} != ?", [other])

    def __lt__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} < ?", [other])

    def __le__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} <= ?", [other])

    def __gt__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} > ?", [other])

    def __ge__(self, other: Any) -> WhereStatement:  # type: ignore
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} >= ?", [other])

    def __getitem__(self, other: list[Any]) -> WhereStatement:
        # This is not final, if there is a better use case for getitem, we will change it.
        # Alternatives for SQL IN clause are:
        # Right shift: User.c.id >> [1,2,3]
        # Matrix Multiplication: User.c.id @ [1,2,3]
        # Power: User.c.id ** [1,2,3]
        if not isinstance(other, list):
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        return WhereStatement(
            f"{self.column_name} IN ({', '.join(['?' for _ in range(len(other))])})",
            other,
        )


class GenericColumn:
    def __init__(self, table: type["Table"]):
        self.table = table

    def __getattr__(self, name: str):
        if name not in self.table.model_fields:
            raise AttributeError(f"Column {name} not found in {self.table.__name__}")
        return Column(field=self.table.model_fields[name], column_name=name)


class TableMeta(ModelMetaclass):
    def __getattr__(self, name):  
        if name == "_" or name == "c":
            return GenericColumn(self) 
        return super().__getattr__(name)  # type: ignore


class Table(BaseModel, metaclass=TableMeta):
    def __init_subclass__(cls, **kwargs):
        setattr(cls, "table_name", cls.__name__.lower())

    @property
    def table_name(cls) -> str:
        return cls.__class__.__name__.lower()


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
    print(engine.query(User).all())
