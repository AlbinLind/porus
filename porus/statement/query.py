from porus.statement.clause_enums import QueryClause
from porus.statement.base_statement import BaseStatement
from porus.column import Column
from porus.table import Table, TableMeta


from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from porus.engine import Engine


class Query(BaseStatement):
    def __init__(
        self,
        *,
        table_or_subquery: Union[list["Column"], type["Table"]],
        engine: "Engine",
    ):
        super().__init__(table_or_subquery=table_or_subquery, engine=engine)
        if isinstance(table_or_subquery, TableMeta):
            self._select_table(str(table_or_subquery.table_name))
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
