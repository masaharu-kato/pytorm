import sql_query as sq

db = sq.Database('DB')

db.prepare_table('kinds', [
    sq.Column('id', 'INT', is_primary=True),
    sq.Column('name', 'VARCHAR(32)'),
])

db.prepare_table('kind_with_dates', [
    sq.Column('id', 'INT', is_primary=True),
    sq.Column('bdate', 'Date'),
    sq.Column('edate', 'Date'),
])

db.prepare_table('subkinds', [
    sq.Column('id', 'INT', is_primary=True),
    sq.Column('kind_id', 'INT', links=[db['kinds']['id'], db['kind_with_dates']['id']]),
    sq.Column('name', 'varchar(64)'),
])

db.prepare_table('items', [
    sq.Column('id', 'INT', is_primary=True, auto_increment=True),
    sq.Column('subkind_id', 'INT', links=[db['subkinds']['id']]),
    sq.Column('name', 'varchar(128)'),
])

db.prepare_table('rel_item_pairs', [
    sq.Column('id', 'INT', is_primary=True, auto_increment=True),
    sq.Column('item1_id', 'INT', links=[db['items']['id']]),
    sq.Column('item2_id', 'INT', links=[db['items']['id']]),
    sq.Column('rate', 'REAL'),
])

db.finalize_tables()


def is_same(expr1, expr2) -> bool:
    return repr(expr1) == repr(expr2)


def test_db_schema():
    items = db['items']
    assert type(items) == sq.Table
    assert items.name == 'items'
    assert is_same(items, db['items'])
    assert is_same(items, db[items])
    assert is_same(items, db.table('items'))
    assert is_same(items, db.table(items))
    assert is_same(items.entity(), items)
    assert is_same(items.db, db)
    assert items.__sql__().query() == '`items`'
    
    items_id = items['id']
    assert type(items_id) == sq.Column
    assert is_same(items_id.name, 'id')
    assert is_same(items_id, items['id'])
    assert is_same(items_id, items[items_id])
    assert is_same(items_id, items.link_to('id'))
    assert is_same(items_id, items.column('id'))
    assert is_same(items_id, items.column(items_id))
    assert is_same(items_id, db['items']['id'])
    assert is_same(items_id, db['items'].link_to('id'))
    assert is_same(items_id, db.link_to('items')['id'])
    assert is_same(items_id, db.link_to('items').link_to('id'))
    assert is_same(items_id, db['items'] >> 'id')
    assert is_same(items_id, (db >> 'items')['id'])
    assert is_same(items_id, db >> 'items' >> 'id')
    assert is_same(items_id, db >> items >> 'id')
    assert is_same(items_id, db >> items >> items_id)
    assert is_same(items_id, 'id' << (items << db))
    assert is_same(items_id, 'items' << db >> 'id')
    assert is_same(items_id, items >> 'id')
    assert is_same(items_id, items >> items_id)
    assert is_same(items_id, 'id' << items)
    assert is_same(items_id, db[items][items_id])
    assert is_same(items_id, db[items]['id'])
    assert is_same(items_id, db['items'][items_id])
    assert is_same(items_id, db.table('items').column('id'))
    assert is_same(items_id, db.table('items')['id'])
    assert is_same(items_id, db['items'].column('id'))
    assert is_same(items_id.entity(), items_id)
    assert is_same(items_id.table, items)
    assert is_same(items_id.db(), db)
    assert is_same(items_id, items.key_column)
    assert items_id.reference_resolved() == True
    assert items_id.is_primary == True
    assert items_id.auto_increment == True
    assert items_id.default_expr == None
    assert items_id.is_unique == False
    assert items_id.nullable == False
    assert items_id.basetype == 'INT'
    assert items_id.__sql__().query() == '`items`.`id`'

    subkinds = db['subkinds']
    linked_subkinds = items['subkind_id']['subkinds']
    assert is_same(linked_subkinds.entity(), subkinds)
    assert is_same(linked_subkinds, items['subkind_id']['subkinds'])
    assert is_same(linked_subkinds, items['subkind_id'].link_to('subkinds'))
    assert is_same(linked_subkinds, items['subkind_id'] >> 'subkinds')
    assert is_same(linked_subkinds, items['subkind_id'][subkinds])
    assert is_same(linked_subkinds, items['subkind_id'].link_to(subkinds))
    assert is_same(linked_subkinds, items['subkind_id'] >> subkinds)
    assert is_same(linked_subkinds, (items >> 'subkind_id')['subkinds'])
    assert is_same(linked_subkinds, (items >> 'subkind_id').link_to('subkinds'))
    assert is_same(linked_subkinds, items >> 'subkind_id' >> 'subkinds')
    assert is_same(linked_subkinds, (items >> 'subkind_id')[subkinds])
    assert is_same(linked_subkinds, (items >> 'subkind_id').link_to(subkinds))
    assert is_same(linked_subkinds, items >> 'subkind_id' >> subkinds)
    assert is_same(linked_subkinds, items[subkinds])
    assert is_same(linked_subkinds, items.link_to(subkinds))
    assert is_same(linked_subkinds, items >> subkinds)
    assert is_same(linked_subkinds, subkinds << items['subkind_id'])
    assert is_same(linked_subkinds, subkinds << items.link_to('subkind_id'))
    assert is_same(linked_subkinds, subkinds << (items >> 'subkind_id'))
    assert is_same(linked_subkinds, subkinds << ('subkind_id' << items))
    assert is_same(linked_subkinds, subkinds << items)

