"""Module for the Column class and the ColumnField function."""
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field
from pydantic.fields import FieldInfo

if TYPE_CHECKING:
    from porus.table import Table


class ColumnFunction(Enum):
    """Enum for the different functions that can be applied to a column."""

    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    SUM = "SUM"
    AVG = "AVG"


def ColumnField(default: Any = None, *, primary_key: bool = False, **kwargs) -> Any:  # noqa: ANN003, N802
    """A wrapper function for pydantic's Field, which adds a primary_key parameter to the
    json_schema_extra parameter.
    """
    return Field(default=default, json_schema_extra={"primary_key": primary_key}, **kwargs)


class WhereStatement:
    """Represents a where statement in SQL. This class is used to create where statements."""

    def __init__(self, statement: str, values: Optional[list[Any]] = None) -> None:
        """Create a new WhereStatement object."""
        if values is None:
            values = []
        self.statement = f"{statement}"
        self.values = values

    def __and__(self, other: "WhereStatement") -> "WhereStatement":
        """Combines the current WhereStatement with another WhereStatement using the
        logical AND operator.
        """
        self.statement = f"({self.statement} AND {other.statement})"
        self.values.extend(other.values)
        return self

    def __or__(self, other: "WhereStatement") -> "WhereStatement":
        """Combines the current WhereStatement with another WhereStatement using the
        logical OR operator.
        """
        self.statement = f"({self.statement} OR {other.statement})"
        self.values.extend(other.values)
        return self

    def __repr__(self) -> str:
        """Representation of the WhereStatement object. This is used for debugging."""
        return f"WHERE {self.statement}, ({', '.join([str(value) for value in self.values])})"


class SetStatement:
    """Represents a set statement in SQL. This class is used to create set statements."""

    def __init__(self, statement: str, column: "Column", values: list[Any] | None = None) -> None:
        """Create a new SetStatement object."""
        if values is None:
            values = []
        self.statement = statement
        self.column = column
        self.values = values

    def __repr__(self) -> str:
        """Representation of the SetStatement object. This is used for debugging."""
        return f"(mock:) SET {self.statement}, {self.column.column_name} = {self.values[0]}"


class Column:
    """Column of a table. This class is used to create SQL statements with the Table.c.column name
    syntax.
    """

    def __init__(self, field: FieldInfo, column_name: str, table_name: str) -> None:
        """Initialize a new Column object."""
        self.field: FieldInfo = field
        self.column_name = column_name
        self.table_name = table_name
        self._function_applied = None

    def get_query(self) -> str:
        """Get the query representation of the column. This is used to create SQL statements."""
        if self._function_applied:
            return f"{self._function_applied.value}({self.column_name})"
        return f"{self.column_name}"

    def max(self) -> "Column":
        """Apply the MAX function to the column."""
        self._function_applied = ColumnFunction.MAX
        return self

    def min(self) -> "Column":
        """Apply the MIN function on the column."""
        self._function_applied = ColumnFunction.MIN
        return self

    def sum(self) -> "Column":
        """Apply the SUM function on the column."""
        self._function_applied = ColumnFunction.SUM
        return self

    def count(self) -> "Column":
        """Apply the COUNT function on the column."""
        self._function_applied = ColumnFunction.COUNT
        return self

    def avg(self) -> "Column":
        """Apply the AVG function on the column."""
        self._function_applied = ColumnFunction.AVG
        return self

    def __repr__(self) -> str:
        """Representation of the Column object. This is used for debugging."""
        return f"Column(type={self.field.annotation})"

    def __eq__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the equal operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} = ?", [other])

    def __hash__(self) -> int:
        """Hash the column."""
        return hash(self.column_name)

    def __ne__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the not equal operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} != ?", [other])

    def __lt__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the less than operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} < ?", [other])

    def __le__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the less than or equal operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} <= ?", [other])

    def __gt__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the greater than operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} > ?", [other])

    def __ge__(self, other: Any) -> WhereStatement:  # type: ignore
        """Create a where statement for the greater than or equal operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        return WhereStatement(f"{self.column_name} >= ?", [other])

    def __getitem__(self, other: Any) -> WhereStatement:
        """Create a where statement for the in operator. This is used to create SQL IN clauses."""
        # This is not final, if there is a better use case for getitem, we will change it.
        # Alternatives for SQL IN clause are:
        # Right shift: User.c.id >> [1,2,3]
        # Matrix Multiplication: User.c.id @ [1,2,3]
        # Power: User.c.id ** [1,2,3]
        if not isinstance(other, (list, tuple)):
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a {type(other)}"
            )
        if len(other) == 0:
            raise ValueError("You cannot compare a column to an empty list.")
        if not all(isinstance(x, self.field.annotation) for x in other):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are comparing it to a list of {type(other)}"
            )
        return WhereStatement(
            f"{self.column_name} IN ({', '.join(['?' for _ in range(len(other))])})",
            list(other),
        )

    def __add__(self, other: Any) -> SetStatement:
        """Create a set statement for the addition operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} + ?", self, [other])

    def __sub__(self, other: Any) -> SetStatement:
        """Create a set statement for the subtraction operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} - ?", self, [other])

    def __mul__(self, other: Any) -> SetStatement:
        """Create a set statement for the multiplication operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} * ?", self, [other])

    def __truediv__(self, other: Any) -> SetStatement:
        """Create a set statement for the division operator."""
        if not isinstance(other, self.field.annotation):  # type: ignore
            raise ValueError(
                f"Column has type {self.field.annotation},"
                f"but you are trying to compare it with {type(other)}"
            )
        return SetStatement(f"{self.column_name} = {self.column_name} / ?", self, [other])


class GenericColumn:
    """Generic column class. This class is used as the "intermediary" between the Table and its
    actually column.
    This class represents the .c (or ._) attribute of the Table class. Once you access a column
    from the Table.c, you will get the true Column object that you should use to create queries etc.
    """

    def __init__(self, table: type["Table"]) -> None:
        """Create a new GenericColumn object."""
        self.table = table

    def __getattr__(self, name: str) -> Column:
        """Get a column attribute from the table. Here we insert our code to create the Column
        object.
        """
        if name not in self.table.model_fields:
            raise AttributeError(f"Column {name} not found in {self.table.__name__}")
        return Column(
            field=self.table.model_fields[name],
            column_name=name,
            table_name=str(self.table.table_name),
        )
