import sql_query as sq
import string
import itertools


def main():
    db = sq.Database('hogehoge')

    db.prepare_table('kinds', [
        sq.ColArgs('id', sq.ColIntType, is_primary=True),
        sq.ColArgs('name', sq.ColType('varchar(32)', str)),
    ])

    db.prepare_table('subkinds', [
        sq.ColArgs('id', sq.ColIntType, is_primary=True),
        sq.ColArgs('kind_id', sq.ColIntType, link_col=db['kinds']['id']),
        sq.ColArgs('name', sq.ColType('varchar(64)', str)),
    ])

    db.prepare_table('items', [
        sq.ColArgs('id', sq.ColIntType, is_primary=True, auto_increment=True),
        sq.ColArgs('subkind_id', sq.ColIntType, link_col=db['subkinds']['id']),
        sq.ColArgs('name', sq.ColType('varchar(128)', str)),
    ])

    db.finalize_tables()

    kinds_records = [(i, 'kind{}'.format(c)) for i, c in enumerate(string.ascii_uppercase[:8], 1)]
    subkinds_records = [(i, (i - 1) // 3 + 1, 'subkind{}'.format(c),) for i, c in enumerate(string.ascii_uppercase[:24], 1)]
    items_records = itertools.chain(
        [(i * 2    , 'apple{}' .format(i)) for i in range(12)],
        [(i * 2 + 1, 'orange{}'.format(i)) for i in range(12)],
        [(i * 3    , 'grape{}' .format(i)) for i in range( 8)],
        [(i * 4 + 3, 'lemon{}' .format(i)) for i in range( 6)],
    )

    db['kinds'].insert(('id', 'name'), kinds_records)
    db['subkinds'].insert(('id', 'kind_id', 'name'), subkinds_records)
    db['items'].insert(('subkind_id', 'name'), items_records)

    db['kinds'].select()
    db['subkinds'].select()
    db['items'].select()
    db.select([db['items']['id'], db['kinds']['name'], db['subkinds']['name'], db['items']['name']])



if __name__ == "__main__":
    main()

