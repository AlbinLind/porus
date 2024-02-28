from enum import Enum

class DeleteClause(Enum):
    DELETE = 0
    WHERE = 1
    RETURNING = 2
    # These may not be available in all SQLite compiles
    # There seem to be no way to check if they are available
    # ORDER_BY = 3
    # LIMIT = 4
    # OFFSET = 5

class UpdateClause(Enum):
    UPDATE = 0
    SET = 1
    FROM = 2
    WHERE = 3
    RETURNING = 4
    ORDER_BY = 5
    LIMIT = 6
    OFFSET = 7

class QueryClause(Enum):
    SELECT = 0
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    ORDER_BY = 4
    LIMIT = 5
    OFFSET = 6