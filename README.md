# Database
I will use this project to learn how to build my own `ORM` (Object-Relational Mapping) for `SQLite`, or at least my take on it. The project will not use any other database than `SQLite`, simply because if I need capabilities beyond what `SQLite` can offer, I better go with a robust system, such as `SQLAlchemy`. At a later stage introducing `rust` as the backend could significantly increase the speed of the library (c.f. [this blog post](https://avi.im/blag/2021/fast-sqlite-inserts/)).

# Installation
Simply install by ...

# Simple Example
```python
from database import Table, Engine, ColumnField

class User(Table):
    id: int = ColumnField(primary_key = True)
    name: str
    some_number: int

engine = Engine(":memory:")

# We make sure that we have the User table in our database
engine.push(User)

# We make one user, you have all the powers of pydantic
# in this model.
user_one = User(name="foo", some_number=100)

# We insert our user model to the database, 
# it will automatically commit the change for you.
# We pass it as a list, and you can provide 
# multiple models at the same time
engine.insert([user_one])

# Create another model
engine.insert([User(name="bar", some_number=150)])

# Make a query where we count how many users
# have `some_number` above 100 and returns
# the first (and only in this case) row
results = engine.query(User.c.id.count()).where(User.c.some_number >= 100).first()

# We get a tuple with 1 entry (the COUNT(id))
assert results[0] == 2

# Makes a delete query where we delete
# all rows where `some_numer` is below 125
# and return the deleted rows
results = engine.delete(User).where(User.c.some_number < 125).returning().all()

# The returned object is based on the User model.
# It will always try to return a model instead of a tuple
assert results[0].name == "foo"
```

