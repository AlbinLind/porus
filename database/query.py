from enum import Enum
from database.column import Column, WhereStatement
from database.table import Table, TableMeta
from database.utilities import _convert_values


from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from database.engine import Engine


class QueryClause(Enum):
    SELECT = 0
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    ORDER_BY = 4
    LIMIT = 5
    OFFSET = 6


class Query:
    def __init__(self, select: Union[list["Column"], type["Table"]], engine: "Engine"):
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
        if not self.select:
            raise ValueError("You must provide at least one column to select.")
        if not isinstance(self.select, list):
            raise ValueError("Select must be a list of columns.")
        table_name = self.select[0].table_name
        if not all(x.table_name == table_name for x in self.select):
            raise ValueError("All columns must be from the same table.")
        self.statements.append((f"FROM {table_name}", QueryClause.FROM, None))
        self.statements.append(
            (
                f"SELECT {', '.join([x.get_query() for x in self.select])}",
                QueryClause.SELECT,
                None,
            )
        )
        return self

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
