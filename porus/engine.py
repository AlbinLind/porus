"""This module contains the Engine class, which is used to interact with the database."""
import sqlite3
from typing import Any, Union

from porus.column import Column, SetStatement
from porus.statement.delete import Delete
from porus.statement.query import Query
from porus.statement.update import Update
from porus.table import Table
from porus.utilities import _convert_values, _get_type


class Engine:
    """The Engine class is used to interact with the database. It is used to create tables, insert objects,
    query objects, and delete them in the database.
    """
    def __init__(self, path: str) -> None:
        """Create a new Engine object."""
        self.path = path
        self.conn = sqlite3.connect(self.path)

    def _check_if_table_exists(self, table: type["Table"]) -> bool:
        statement = "SELECT name FROM sqlite_master WHERE type='table' "
        f"AND name='{table.__name__.lower()}';"
        result = self.conn.execute(statement).fetchone()
        if result:
            return True
        return False

    def _create_table(self, table: type["Table"]) -> None:
        """Private function that creates the statement to create a table in the database."""
        statement = f"CREATE TABLE {table.table_name}"
        columns = []
        for field in table.model_fields:
            column_statement = f"{field} {_get_type(table.model_fields[field].annotation)}"
            if table.model_fields[field].json_schema_extra:  # noqa: SIM102
                if table.model_fields[field].json_schema_extra.get("primary_key"):  # type: ignore
                    column_statement += " PRIMARY KEY"
            columns.append(column_statement)
        statement += f"({', '.join(columns)})"
        self.conn.execute(statement)
        self.conn.commit()

    def push(self, table: type["Table"]):
            """Pushes a table to the database, and adds it if it does not already exists.

            Args:
                table (type["Table"]): The table to be pushed.
            """
            if not self._check_if_table_exists(table):
                self._create_table(table)

    def _convert_row_to_object(
        self, table: Union["Table", type["Table"]], row: tuple[Any]
    ) -> "Table":
        """Private method that converts a row to an object."""
        fields = table.model_fields
        if len(row) != len(fields):
            raise ValueError(
                f"Number of columns in the row ({len(row)}) does not match the number"
                f" of fields in the table ({len(fields)})."
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
            """Inserts a list of objects into the database.

            Args:
                objs (list["Table"]): The list of objects to be inserted.

            Returns:
                list[Any]: A list of inserted objects.
            """
            row_list = []
            for obj in objs:
                keys = []
                values = []
                for key, value in obj.model_dump().items():
                    if obj.model_fields[key].json_schema_extra:  # noqa: SIM102
                        if (
                            obj.model_fields[key].json_schema_extra.get("primary_key")  # type: ignore
                            and not value
                        ):
                            continue
                    keys.append(str(key))
                    values.append(value)

                statement = f"INSERT INTO {obj.table_name} ({', '.join(keys)}) VALUES "
                f"({', '.join(['?' for _ in range(len(values))])}) RETURNING *;"
                values = _convert_values(values)
                result = self.conn.execute(statement, values).fetchone()
                row_list.append(self._convert_row_to_object(obj, result))
            self.conn.commit()
            return row_list

    def query(self, *select: Union["Column", type["Table"]]) -> Query:
        """Executes a query on the database. Chain the query with methods such as where, limit,
        offset, sort_by, order_by, group_by.

        Example:
        >>> engine.query(User.c.name).where(User.c.id > 1).sort_by(User.c.followers).limit(10).all()

        Note that if you provide a Table and use the group_by method, you will not receive an
        object of that table, but a list of tuples, like you where using columns.

        Args:
            *select: Variable number of arguments representing the columns or tables to select from.

        Returns:
            A Query object representing the executed query.

        Raises:
            ValueError: If the select parameter is not a Column or a Table, or if any element in
            the select list is not of type Column.
        """
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
            """Update the records in the table with the specified set statements.
            This method is the same as the SQL UPDATE statement. If you want to replace
            objects, use engine.replace().

            Args:
                *update (SetStatement): The set statements to be applied to the records.

            Returns:
                Update: An Update object representing the update operation.

            Raises:
                ValueError: If any element in the update list is not of type SetStatement.
            """
            if not all(isinstance(x, SetStatement) for x in update):
                raise ValueError(
                    "All elements in the update list must be of type SetStatement, if you want to"
                    "replace objects, use engine.replace()."
                )
            return Update(table_or_subquery=list(update), engine=self)

    def delete(self, table: type["Table"]) -> "Delete":
            """Creates a Delete object for the specified table.

            Args:
                table (type["Table"]): The table to delete records from.

            Returns:
                Delete: The Delete object for further operations.
            """
            return Delete(table_or_subquery=table, engine=self)
