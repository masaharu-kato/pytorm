from sql.datatypes import Int, Text, DateTime
from sql.objects import Column
from sql.schema import Database, Table

db = sql.Database('DB')

class Category(Table):
    name = Column(Text)

class Group(Table):
    category = Column(Category)
    name     = Column(Text)

class Item(Table):
    group = Column(Group)
    name  = Column(Text)

class Shop(Table):
    name = Column(Text)
    url  = Column(Text)

class ItemPrice(Table):
    item  = Column(Item)
    shop  = Column(Shop)
    price = Column(Int)
    url   = Column(Text)

class User(Table):
    name = Column(Text)
    age  = Column(Int)

class Review(Table):
    item     = Column(Item)
    user     = Column(User)
    datetime = Column(DateTime)
    rate     = Column(Int)
    comment  = Column(Text)


# db = sq.Database('DB')

# db.prepare_table('kinds', [
#     sq.Column('id', 'INT', is_primary=True),
#     sq.Column('name', 'VARCHAR(32)'),
# ])

# db.prepare_table('kind_with_dates', [
#     sq.Column('id', 'INT', is_primary=True),
#     sq.Column('bdate', 'Date'),
#     sq.Column('edate', 'Date'),
# ])

# db.prepare_table('subkinds', [
#     sq.Column('id', 'INT', is_primary=True),
#     sq.Column('kind_id', 'INT', links=[db['kinds']['id'], db['kind_with_dates']['id']]),
#     sq.Column('name', 'varchar(64)'),
# ])

# db.prepare_table('items', [
#     sq.Column('id', 'INT', is_primary=True, auto_increment=True),
#     sq.Column('subkind_id', 'INT', links=[db['subkinds']['id']]),
#     sq.Column('name', 'varchar(128)'),
# ])

