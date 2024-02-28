from enum import Enum
from pydantic import Field


from pydantic.fields import FieldInfo


from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from porus.table import Table


class ColumnFunction(Enum):
    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    SUM = "SUM"
    AVG = "AVG"


def ColumnField(default: Any = None, *, primary_key: bool = False, **kwargs) -> Any:
    """A wrapper function for pydantic's Field, which adds a primary_key parameter to the json_schema_extra parameter."""
    return Field(
        default=default, json_schema_extra={"primary_key": primary_key}, **kwargs
    )


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

class SetStatement:
    def __init__(self, statement: str, column: "Column", values: list[Any] = []):
        self.statement = statement
        self.column = column
        self.values = values

    def __repr__(self):
        return f"(mock:) SET {self.statement}, {self.column.column_name} = {self.values[0]}"

class Column:
    def __init__(self, field: FieldInfo, column_name: str, table_name: str):
        self.field: FieldInfo = field
        self.column_name = column_name
        self.table_name = table_name
        self._function_applied = None

    def get_query(self) -> str:
        if self._function_applied:
            return f"{self._function_applied.value}({self.column_name})"
        return f"{self.column_name}"

    def max(self):
        self._function_applied = ColumnFunction.MAX
        return self

    def min(self):
        self._function_applied = ColumnFunction.MIN
        return self

    def sum(self):
        self._function_applied = ColumnFunction.SUM
        return self

    def count(self):
        self._function_applied = ColumnFunction.COUNT
        return self

    def avg(self):
        self._function_applied = ColumnFunction.AVG
        return self

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

    def __getitem__(self, other: Any) -> WhereStatement:
        # This is not final, if there is a better use case for getitem, we will change it.
        # Alternatives for SQL IN clause are:
        # Right shift: User.c.id >> [1,2,3]
        # Matrix Multiplication: User.c.id @ [1,2,3]
        # Power: User.c.id ** [1,2,3]
        if not isinstance(other, (list, tuple)):
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a {type(other)}"
            )
        if len(other) == 0:
            raise ValueError("You cannot compare a column to an empty list.")
        if not all(isinstance(x, self.field.annotation) for x in other):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are comparing it to a list of {type(other)}"
            )
        return WhereStatement(
            f"{self.column_name} IN ({', '.join(['?' for _ in range(len(other))])})",
            list(other),
        )

    def __add__(self, other: Any) -> SetStatement:
        if not isinstance(other, self.field.annotation): # type: ignore
                raise ValueError(
                    f"Column has type {self.field.annotation}, but you are trying to compare it with {type(other)}"
                )
        return SetStatement(f"{self.column_name} = {self.column_name} + ?", self, [other])

    def __sub__(self, other: Any) -> SetStatement:
        if not isinstance(other, self.field.annotation): # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} - ?", self, [other])
    
    def __mul__(self, other: Any) -> SetStatement:
        if not isinstance(other, self.field.annotation): # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} * ?", self, [other])
    
    def __truediv__(self, other: Any) -> SetStatement:
        if not isinstance(other, self.field.annotation): # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation}, but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} / ?", self, [other])
    

class GenericColumn:
    def __init__(self, table: type["Table"]):
        self.table = table

    def __getattr__(self, name: str):
        if name not in self.table.model_fields:
            raise AttributeError(f"Column {name} not found in {self.table.__name__}")
        return Column(
            field=self.table.model_fields[name],
            column_name=name,
            table_name=str(self.table.table_name),
        )
