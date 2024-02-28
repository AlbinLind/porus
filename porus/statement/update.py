"""Update statement functionality for porus."""
from typing import TYPE_CHECKING

from porus.column import Column
from porus.statement.base_statement import BaseStatement
from porus.statement.clause_enums import UpdateClause
from porus.table import Table

if TYPE_CHECKING:
    from porus.column import SetStatement
    from porus.engine import Engine


class Update(BaseStatement):
    """Update statement, used to update rows in a table, just as you would with an SQL UPDATE
    statement.
    """
    def __init__(
        self,
        *,
        table_or_subquery: list["SetStatement"],
        engine: "Engine",
    ) -> None:
        """Create a new Update object."""
        super().__init__(table_or_subquery=[t.column for t in table_or_subquery], engine=engine)
        if isinstance(table_or_subquery, Table):
            raise ValueError(
                "You cannot update a table directly. You must use column(s). If you want to replace a row, use engine.replace()"
            )
        self.table_name = table_or_subquery[0].column.table_name
        self.set_statements = table_or_subquery
        self._update_columns()
        if not all(x.column.table_name == self.table_name for x in table_or_subquery):
            raise ValueError("All columns must be from the same table.")
        self._set_statement()

    def _update_columns(self) -> "Update":
        """Create the update clause."""
        self.statements.append((f"UPDATE {self.table_name}", UpdateClause.UPDATE, None))
        return self

    def _set_statement(self) -> "Update":
        """Create the set clause."""
        stm = f"SET {', '.join([x.statement for x in self.set_statements])}"
        values = []
        [values.extend(x.values) for x in self.set_statements]
        self.statements.append((stm, UpdateClause.SET, values))
        return self

    def _validate_query(self) -> None:
        """Make sure that the query are valid, and that there are no conflicting clauses."""
        clauses = [x[1] for x in self.statements]
        if UpdateClause.UPDATE not in clauses:
            raise ValueError("You must provide a table to update.")
        if UpdateClause.SET not in clauses:
            raise ValueError("You must provide a SET clause.")
        if (
            UpdateClause.LIMIT in clauses or UpdateClause.OFFSET in clauses
        ) and UpdateClause.RETURNING not in clauses:
            raise ValueError("You cannot use LIMIT or OFFSET without RETURNING.")
        if UpdateClause.OFFSET in clauses and UpdateClause.LIMIT not in clauses:
            self.limit(-1)

    def returning(self, *columns: Column) -> "Update":
        """Return the columns after the update. If no columns are provided, returns all columns."""
        if len(columns) == 0:
            self.statements.append(("RETURNING *", UpdateClause.RETURNING, None))
            return self
        if not all(isinstance(x, Column) for x in columns):
            raise ValueError("All elements in the returning list must be of type Column.")
        if not all(x.table_name == self.table_name for x in columns):
            raise ValueError("All columns must be from the same table.")
        self.statements.append((
            f"RETURNING {', '.join([c.column_name for c in columns])}",
            UpdateClause.RETURNING,
            None,
        ))
        return self
