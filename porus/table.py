"""Contains the custom meta class for pydantic models and the table class."""
from typing import Any

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass  # noqa: PLC2701

from porus.column import GenericColumn


class TableMeta(ModelMetaclass):
    """Meta class for the Table class. This class is used so that we can get the "column"
    attribute from the table class, which is actually a pydantic model.
    """

    def __getattr__(self, name: str) -> Any:
        """Gets a column attribute."""
        if name in {"_", "c"}:
            return GenericColumn(self)
        return super().__getattr__(name)  # type: ignore


class Table(BaseModel, metaclass=TableMeta):
    """Base class for tables in porus ORM. This class is also a pydantic model,
    so it can be used in the same way as you would use it. However, it also contains some other
    notable functionality.

    To use it just create a class that inherits from this class, and then add the columns as class
    variables.

    >>> from porus import Table, ColumnField
    >>> class Article(Table):
    >>>    id: int = ColumnField(primary_key=True)
    >>>    title: str
    >>>    body: str
    >>>    author: str
    >>> article = Article(title="Test", body="Test", author="Test")
    >>> print(article.model_dump()) # will print the model as a dictionary.

    This is what makes porus, strong.
    """

    def __init_subclass__(cls) -> None:
        """Function that runs after a subclass is created. This function sets the table_name."""
        cls.table_name = cls.__name__.lower()  # type: ignore

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self.__class__.__name__.lower()
