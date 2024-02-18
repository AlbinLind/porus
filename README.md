# Database
I will use this project to learn how to build my own `ORM` (Object-Relational Mapping) for `SQLite`, or at least my take on it. The project will not use any other database than `SQLite`, simply because if I need capabilities beyond what `SQLite` can offer, I better go with a robust system, such as `SQLAlchemy`. At a later stage introducing `rust` as the backend could significantly increase the speed of the library (c.f. [this blog post](https://avi.im/blag/2021/fast-sqlite-inserts/)).

The project will also leverage some packages that are "essential" in today's (2024) Python ecosystem — `pydantic` modern Python's power horse.

# Development
Firstly I will create a [MVP](https://en.wikipedia.org/wiki/Minimum_viable_product) where I setup an abstract class from which I can create tables.
### MVP
- [ ] Build `TableBase`
- [ ] Create an engine that is associated with the table
- [ ] Be able to create a table with columns
- [ ] Function to add to the database
- [ ] Function to get `n` items from the database
