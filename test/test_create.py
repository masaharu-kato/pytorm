import sql_query as sq
import datetime


class Category:
    name: str

class Group:
    category: Category
    name: str

class Item:
    group: Group
    name: str

class Shop:
    name: str
    url: str

class ItemPrice:
    item: Item
    shop: Shop
    price: int
    url: str

class User:
    name: str
    age: int

class Review:
    item: Item
    user: User
    datetime: datetime.datetime
    rate: int
    comment: str






def main():
    
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


if __name__ == "__main__":
    main()
