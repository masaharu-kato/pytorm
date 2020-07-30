import sql_query as sq
import string
import itertools


def create_database():
    
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

    return db


def main():

    db = create_database()

    # kinds_records = [(i, 'kind{}'.format(c)) for i, c in enumerate(string.ascii_uppercase[:8], 1)]
    # subkinds_records = [(i, (i - 1) // 3 + 1, 'subkind{}'.format(c),) for i, c in enumerate(string.ascii_uppercase[:24], 1)]
    # items_records = itertools.chain(
    #     [(i * 2    , 'apple{}' .format(i)) for i in range(12)],
    #     [(i * 2 + 1, 'orange{}'.format(i)) for i in range(12)],
    #     [(i * 3    , 'grape{}' .format(i)) for i in range( 8)],
    #     [(i * 4 + 3, 'lemon{}' .format(i)) for i in range( 6)],
    # )

    # db['kinds'].insert(('id', 'name'), kinds_records)
    # db['subkinds'].insert(('id', 'kind_id', 'name'), subkinds_records)
    # db['items'].insert(('subkind_id', 'name'), items_records)

    kinds, subkinds, items = db['kinds', 'subkinds', 'items']

    kinds.insert(('id', 'name'), [])
    subkinds.insert(('id', 'kind_id', 'name'), [])
    items.insert(('subkind_id', 'name'), [])
    
    kinds.select()
    subkinds.select()
    items.select()

    db['items']
    db['items']['subkind_id']
    db['items']['subkind_id']['subkinds']
    t = db['items']['subkind_id']['subkinds']['kind_id']
    t2 = t['kinds']
    t2['name']

    q1 = db.select([
        db['items']['id'],
        db['items']['subkind_id']['subkinds']['kind_id']['kinds']['name'],
        db['items']['subkind_id']['subkinds']['name'],
        db['items']['name']
    ])

    q2 = db.select([
        items['id'],
        items['subkind_id'][subkinds]['kind_id'][kinds]['name'],
        items['subkind_id'][subkinds]['name'],
        items['name']
    ])

    q3 = db.select([
        items['id'],
        items[subkinds][kinds]['name'],
        items[subkinds]['name'],
        items['name']
    ])

    q4 = db.select([items['id', subkinds[kinds['name'], 'name'], 'name']])
    # kinds[] -> str
    # subkinds[] -> (Column, str)
    # items[] = (str, ColumnsInTable, str)

    # q_ans = 'SELECT `items`.`id`, `kinds`.`name`, `subkinds`.`name`, `items`.`name` FROM `items` INNER JOIN `subkinds` ON `items`.`subkind_id` = `subkinds`.`id` INNER JOIN `kinds` ON `subkinds`.`kind_id` = `kinds`.`id`'

    kind_with_dates, rel_item_pairs = db['kind_with_dates', 'rel_item_pairs']

    q5a = db.select([
        items['id'],
        items[subkinds][kinds]['name'],
        *items[subkinds][kind_with_dates]['bdate', 'edate'],
        items[subkinds]['name'],
        items['name']
    ])

    q5b = db.select([
        items['id'],
        items >> subkinds >> kinds['name'],
        *(items >> subkinds >> kind_with_dates['bdate', 'edate']),
        items >> subkinds['name'],
        items['name']
    ])

    q5c = db.select([
        items['id'],
        kinds['name'] << subkinds << items,
        *((kind_with_dates << subkinds << items)['bdate', 'edate']),
        subkinds['name'] << items,
        items['name']
    ])

    try:
        _q6 = db.select([
            rel_item_pairs[items]['name'],
        ])
    except RuntimeError as err:
        print('Exception: ', err)

    q6a = db.select([
        rel_item_pairs['item1_id'][items].alias('items1')['name'],
        rel_item_pairs['item2_id'][items].alias('items2')['name'],
    ])

    q6b = db.select([
        rel_item_pairs['item1_id'][items].alias('items1')['name'],
        rel_item_pairs['item2_id'][items].alias('items2')['name'],
    ])

    q6_ans = db.select([
        (rel_item_pairs['item1_id'][items]@'items1')['name'],
        (rel_item_pairs['item2_id'][items]@'items2')['name'],
    ])

    q6_ans = db.select([
        (rel_item_pairs['item1_id'] >> items @ 'items1')['name'],
        (rel_item_pairs['item2_id'] >> items @ 'items2')['name'],
    ])

    
    # A, B, C, D = ['A', 'B', 'C', 'D']
    # q = db[A['id', B[C[D['name'], 'name'], 'name'], 'name']]
    #   = db[A['id', B[C[D.name, 'name'], 'name'], 'name']]
    #   = db[A['id', B[(C->D).name, C.name], 'name'], 'name']]
    #   = db[A['id', (B->C->D).name, (B->C).name, B.name], 'name']]
    #   = db[A.id, (A->B->C->D).name, (A->B->C).name, (A->B).name A.name]

    # q = db[A['id'], A[B][C][D]['name'], A[B][C]['name'], A[B]['name'], A['name']]


if __name__ == "__main__":
    main()

