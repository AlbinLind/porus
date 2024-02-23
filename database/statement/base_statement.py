from enum import Enum
from database.column import Column, WhereStatement
from database.statement.clause_enums import QueryClause
from database.table import Table, TableMeta
from database.utilities import _convert_values


from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from database.engine import Engine

class BaseStatement(ABC):
    def __init__(
        self,
        *,
        table_or_subquery: Union[list["Column"], type["Table"]],
        engine: "Engine",
    ):
        self.statements: list[tuple[str, Enum, list[Any] | None]] = []
        self.engine = engine
        self.select = table_or_subquery
        # We initially assume we can return an object if we are selecting from a table
        # Otherwise, we will return a tuple of size equal to the size of the select list
        # For example, if we add a group by clause we can no longer return an object,
        # since we cannot know how it will look, and no model is defined for that.
        self.result_column = table_or_subquery
        self._can_return_table = isinstance(table_or_subquery, TableMeta)

    @abstractmethod
    def _validate_query(self):
        pass

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

    def group_by(self, *columns: "Column"):
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError(
                "All elements in the group by list must be of type Column."
            )
        self.statements.append(
            (
                f"GROUP BY {', '.join([x.column_name for x in columns])}",
                QueryClause.GROUP_BY,
                None,
            )
        )
        self._can_return_table = False
        return self

    def order_by(self, *columns: "Column", ascending: bool = True):
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError(
                "All elements in the order by list must be of type Column."
            )
        self.statements.append(
            (
                f"ORDER BY {', '.join([x.column_name for x in columns])} {'ASC' if ascending else 'DESC'}",
                QueryClause.ORDER_BY,
                None,
            )
        )
        return self

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
        values = _convert_values(values)
        return statement, values

    def all(self) -> Any:
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