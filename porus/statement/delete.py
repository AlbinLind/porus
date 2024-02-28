"""This module contains the Delete class, which is used to create a DELETE statement."""
from typing import TYPE_CHECKING

from porus.column import Column
from porus.errors.clause_errors import InvalidOperationError
from porus.statement.base_statement import BaseStatement
from porus.statement.clause_enums import DeleteClause
from porus.table import Table, TableMeta

if TYPE_CHECKING:
    from porus.column import Column
    from porus.engine import Engine


class Delete(BaseStatement):
    """Delete statement, used to delete rows from a table, just as you would with an SQL DELETE."""
    def __init__(
        self,
        *,
        table_or_subquery: type["Table"],
        engine: "Engine",
    ) -> None:
        """Create a new Delete object."""
        super().__init__(table_or_subquery=table_or_subquery, engine=engine)
        self._delete_table()

    def _delete_table(self) -> "Delete":
        """Create the DELETE clause."""
        if not isinstance(self.select, TableMeta):
            raise ValueError("You cannot delete from a subquery. You must provide a table.")
        self.statements.append((f"DELETE FROM {self.select.table_name}", DeleteClause.DELETE, None))
        return self

    def _validate_query(self) -> None:
        """Validates that the query is valid."""
        if DeleteClause.DELETE not in [x[1] for x in self.statements]:
            raise ValueError("You must provide a table to delete from.")

    def returning(self, *columns: Column) -> "Delete":
        """Return the columns after the delete. If no columns are provided, returns all columns."""
        if len(columns) == 0:
            self.statements.append(("RETURNING *", DeleteClause.RETURNING, None))
            return self
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError("All elements in the returning list must be of type Column.")
        if not all(x.table_name == self.select.table_name for x in columns):  # type: ignore
            raise ValueError("All columns must be from the same table.")
        self.statements.append((
            f"RETURNING {', '.join([x.column_name for x in columns])}",
            DeleteClause.RETURNING,
            None,
        ))
        # We are returning columns, so we cannot return a table at this point
        self._can_return_table = False
        return self

    def order_by(self, *columns: Column, ascending: bool = True) -> None: # type: ignore
        """You cannot use ORDER BY in a DELETE statement."""
        raise InvalidOperationError("You cannot use ORDER BY in a DELETE statement.")

    def limit(self, limit: int) -> None: # type: ignore
        """You cannot use LIMIT in a DELETE statement."""
        raise InvalidOperationError("You cannot use LIMIT in a DELETE statement.")

    def offset(self, offset: int) -> None: # type: ignore
        """You cannot use OFFSET in a DELETE statement."""
        raise InvalidOperationError("You cannot use OFFSET in a DELETE statement.")

    def group_by(self, *columns: Column) -> None: # type: ignore
        """You cannot use GROUP BY in a DELETE statement."""
        raise InvalidOperationError("You cannot use GROUP BY in a DELETE statement.")
