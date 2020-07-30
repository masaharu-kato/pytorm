import sql_query as sq

def main():
    
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


if __name__ == "__main__":
    main()
