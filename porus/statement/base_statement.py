"""Base class for all statements in porus ORM."""
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Union

from porus.column import Column, WhereStatement
from porus.statement.clause_enums import QueryClause
from porus.table import Table, TableMeta
from porus.utilities import _convert_values

if TYPE_CHECKING:
    from porus.engine import Engine


class BaseStatement(ABC):
    """Base class from which all statements inherit from. This is an abstract class, so it cannot be instantiated directly, nor should it.
    Note that a statement will need to implement the _validate_query method, which will be called before the statement is executed.
    """
    def __init__(
        self,
        *,
        table_or_subquery: Union[list["Column"], type["Table"]],
        engine: "Engine",
    ) -> None:
        """Create a new BaseStatement object."""
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
    def _validate_query(self) -> None:
        """Make sure that the query are valid, and that there are no conflicting clauses."""
        pass

    def limit(self, limit: int) -> "BaseStatement":
        """Limit the number of rows returned from the database.
        Setting this to -1 will return all rows.
        """
        self.statements.append(("LIMIT ?", QueryClause.LIMIT, [limit]))
        return self

    def offset(self, offset: int) -> "BaseStatement":
        """Offset the number of rows returned from the database."""
        self.statements.append(("OFFSET ?", QueryClause.OFFSET, [offset]))
        return self

    def where(self, clause: "WhereStatement") -> "BaseStatement":
        """Add a where clause to the query. This will filter the rows returned from the database.
        Please note that you will need to surround your column expressions with parenhesis if 
        you are using the AND or OR operators.
        
        Example:
        >>> engine.query(User).where((User.c.id == 1) & (User.c.age > 25)).first()
        """
        self.statements.append((f"WHERE {clause.statement}", QueryClause.WHERE, clause.values))
        return self

    def group_by(self, *columns: "Column") -> "BaseStatement":
        """Group the rows returned from the database by the provided columns.
        If you are querying a table, you will receive a list of tuple instead of a list of objects.
        """
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError("All elements in the group by list must be of type Column.")
        self.statements.append((
            f"GROUP BY {', '.join([x.column_name for x in columns])}",
            QueryClause.GROUP_BY,
            None,
        ))
        self._can_return_table = False
        return self

    def order_by(self, *columns: "Column", ascending: bool = True) -> "BaseStatement":
        """Order the rows returned from the database by the provided columns. You can provide
        multiple columns to order by.
        """
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError("All elements in the order by list must be of type Column.")
        self.statements.append((
            f"ORDER BY {', '.join([x.column_name for x in columns])} {'ASC' if ascending else 'DESC'}",
            QueryClause.ORDER_BY,
            None,
        ))
        return self

    def _build_statement(self) -> tuple[str, list[Any]]:
        """Build the statement and the values to be used in the execute method."""
        statement = ""
        values = []
        clauses_added = set()
        self._validate_query()
        self.statements.sort(key=lambda x: x[1].value)
        for clause, clause_type, value in self.statements:
            if clause_type in clauses_added:
                raise ValueError(
                    f"Clause {clause_type} already added, you cannot provide"
                    " a clause multiple times."
                )
            statement += " " + clause
            if value:
                values.extend(value)
            clauses_added.add(clause_type)
        values = _convert_values(values)
        return statement, values

    def all(self, debug: bool = False) -> Any:
        """Retrieve all rows from the database table.

        Args:
            debug (bool, optional): If True, the statement and values will be printed to the 
            console. Defaults to False.

        Returns:
            list[tuple[Any]] | list[Table]: A list of objects representing the rows from the table,
            or a list of tuples if the return type is not a table.
        """
        statement, values = self._build_statement()
        if debug:
            print(statement, values)  # noqa: T201
        result = self.engine.conn.execute(statement, values).fetchall()
        if self._can_return_table and isinstance(self.result_column, TableMeta):
            return [self.engine._convert_row_to_object(self.result_column, row) for row in result]
        return result

    def first(self, debug: bool = False) -> Any:
        """Retrive the first result from the database table.

        Args:
            debug (bool, optional): If True, the statement and values will be printed to the 
            console. Defaults to False.

        Returns:
            Any: An object representing the first row from the table, or a tuple if the return type
            is not a table.
        """
        statement, values = self._build_statement()
        if debug:
            print(statement, values)  # noqa: T201
        result = self.engine.conn.execute(statement, values).fetchone()
        if self._can_return_table and isinstance(self.result_column, TableMeta):
            return self.engine._convert_row_to_object(self.result_column, result)
        return result
