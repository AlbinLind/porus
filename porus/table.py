from pydantic import BaseModel

from porus.column import GenericColumn
from pydantic._internal._model_construction import ModelMetaclass


class TableMeta(ModelMetaclass):
    def __getattr__(self, name):
        if name == "_" or name == "c":
            return GenericColumn(self)
        return super().__getattr__(name)  # type: ignore


class Table(BaseModel, metaclass=TableMeta):
    def __init_subclass__(cls, **kwargs):
        setattr(cls, "table_name", cls.__name__.lower())

    @property
    def table_name(cls) -> str:
        return cls.__class__.__name__.lower()