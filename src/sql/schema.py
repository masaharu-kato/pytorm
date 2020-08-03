"""
    sql.schema - SQL schema abstract classes
"""
from typing import Any, final, Iterable, Iterator, Optional, Sequence, Tuple, Union
from abc import abstractmethod
from sql.expression import Expr, Query

class SchemaExpr(Expr):

    @abstractmethod
    def __repr__(self) -> str:
        """ Get the string representation for debug """
    
    # @abstractmethod
    # def __hash__(self) -> int: pass
    
    # @abstractmethod
    # def __eq__(self, expr) -> bool: pass

    # @abstractmethod
    # def __ne__(self, expr) -> bool: pass

    @abstractmethod
    def link_to(self, val):
        """ 
            Return a linked schema object (may be aliased) 
        """

    @final
    def __rshift__(self, val):
        """ 
            Syntax self >> val implementation
            Alias of link_to method
        """
        return self.link_to(val)

    @final
    def __lshift__(self, val):
        """ 
            Syntax self << val implementation
            Alias of link_to method
        """
        if not hasattr(val, 'link_to'):
            return NotImplemented
        return val.link_to(self)

    @final
    def __rrshift__(self, val):
        """ 
            Syntax val >> self implementation
            Alias of link_to method
        """
        if not hasattr(val, 'link_to'):
            return NotImplemented
        return val.link_to(self)

    @final
    def __rlshift__(self, val):
        """ 
            Syntax val << self implementation
            Alias of link_to method
        """
        return self.link_to(val)

    @final
    def __getitem__(self, val):
        """ 
            Syntax self[val] implementation
            Alias of link_to method
        """
        return self.link_to(val)

    @final
    def __getattr__(self, name:str):
        """ 
            Syntax self.name implementation
            Alias of link_to method
        """
        try:
            return self.link_to(name)
        except KeyError:
            raise AttributeError()


class SingleSchemaExpr(SchemaExpr):

    @abstractmethod
    def entity(self):
        """ Return the pure schema object (not referenced, linked, or aliased schema object) """

    @abstractmethod
    def column_connections(self) -> Iterator[Tuple['Column', 'Column']]:
        """ Return the column connections until this schema object """


class SchemaExprs(SchemaExpr):
    """ A List of schema expressions """

    def __init__(self, schemas:Iterable['SchemaExpr']) -> None:
        self.schemas = list(schemas)

    def link_to(self, val) -> 'SchemaExprs':
        return SchemaExprs(s.link_to(val) for s in self.schemas)

    def flatten_schemas(self) -> Iterator['SchemaExpr']:
        for schema in self.schemas:
            if isinstance(schema, SchemaExprs):
                yield from self.flatten_schemas()
            else:
                yield schema

    def __sql__(self) -> Query:
        return Query(self.schemas)

    def __repr__(self) -> str:
        return '(' + ', '.join(map(repr, self.schemas)) + ')'

    def __iter__(self) -> Iterator[SchemaExpr]:
        return iter(self.schemas)

    # def __hash__(self) -> int:
    #     return hash(repr(self))

    # def __eq__(self, other) -> bool:
    #     if not isinstance(other, SchemaExprs):
    #         raise NotImplementedError()
    #     return len(self.schemas) == len(other.schemas) \
    #  and all(self_s == other_s for self_s, other_s in zip(self.schemas, other.schemas))

    # def __ne__(self, other) -> bool:
    #     return not self.__eq__(other)


TableName = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)

class ColumnExpr(SingleSchemaExpr):
    """
        ColumnExpr Expression (Abstract class)
    """

    @abstractmethod
    def entity(self) -> 'Column': 
        """ Return the pure column object (not referenced, linked, or aliased column) """

    @final
    def link_to(self, val):
        """ 
            Return a linked-table or linked-column (may be aliased) 
            with self and given table name string or (linked-, aliased-)table or (linked-, aliased-)column
        """
        if isinstance(val, str):
            return self.link_to(self.entity().db().table(val))

        if isinstance(val, Table):
            return LinkedTable(val, self)

        if isinstance(val, Column):
            return self.link_to(val.table).column(val)

        if isinstance(val, LinkedTable):
            return self.link_to(val.linking_column).link_to(val.Table)

        if isinstance(val, ColumnInLinkedTable):
            return self.link_to(val.linked_table).column(val.column)

        if isinstance(val, AliasedTable):
            return AliasedTable(self.link_to(val.table), val.alias_name)

        if isinstance(val, ColumnInAliasedTable):
            return self.link_to(val.aliased_table).column(val.column)

        if isinstance(val, Iterable):
            return SchemaExprs(map(self.link_to, val))

        raise TypeError('Unexcepted type `{}`.'.format(type(val)))


class TableExpr(SingleSchemaExpr):
    """
        Table Expression (Abstract class)
    """

    @abstractmethod
    def entity(self) -> 'Table':
        """ Return the pure table object (not referenced, linked, or aliased table) """

    @abstractmethod
    def _make_column(self, column_entity:'Column'):
        """ Return a column object which belongs to self table """

    def column(self, column_or_name:Union[ColumnName, ColumnExpr]):
        if isinstance(column_or_name, ColumnExpr):
            column = column_or_name.entity()
            if not self.entity().column_exists(column):
                raise RuntimeError('Column {} does not exist on {}.'.format(repr(column), repr(self)))
        else:
            column = self.entity().column_by_name(column_or_name)
        
        return self._make_column(column)


    @final
    def link_to(self, val):
        """ 
            Return a linked-table or linked-column (may be aliased) 
            with self and given column name string or (linked-, aliased-)table or (linked-, aliased-)column
        """

        if isinstance(val, str): # ColumnName
            return self.column(val)

        if isinstance(val, Table):
            return LinkedTable(val, self.entity().connection_to(val))

        if isinstance(val, Column):
            if self.entity().column_exists(val):
                return self.column(val)
            return self.link_to(val.table).column(val)

        if isinstance(val, LinkedTable):
            return self.link_to(val.linking_column).link_to(val.table)

        if isinstance(val, ColumnInLinkedTable):
            return self.link_to(val.linked_table).column(val.column)

        if isinstance(val, AliasedTable):
            return AliasedTable(self.link_to(val.table), val.alias_name)

        if isinstance(val, ColumnInAliasedTable):
            return self.link_to(val.aliased_table).column(val.column)

        if isinstance(val, Iterable):
            return SchemaExprs(map(self.link_to, val))

        raise TypeError('Unexcepted type `{}`.'.format(type(val)))


    @abstractmethod
    def alias(self, alias_name:str) -> 'AliasedTable':
        """ Return a aliased-table object of self """


# Reference of Table
class TableRef(TableExpr):
    """ A (forward) reference to table object using a table name """

    def __init__(self, db:'Database', name:TableName) -> None:
        self.db = db
        self.name = name
        if self.db.table_exists(name):
            self._entity = self.db.table(name)

    def resolve_reference(self) -> None:
        if not self.reference_resolved():
            if not self.db.table_exists(self.name):
                raise RuntimeError('Failed to resolve reference: Table `{}` was not found in {}'
                    .format(self.name, repr(self.db)))
            self._entity = self.db.table(self.name)

    def reference_resolved(self) -> bool:
        return hasattr(self, '_entity')

    def entity(self) -> 'Table':
        if not self.reference_resolved():
            self.resolve_reference()
        return self._entity

    def _make_column(self, column_entity:'Column'):
        if not self.reference_resolved():
            self.resolve_reference()
        return self.entity()._make_column(column_entity)

    def alias(self, aliased_name:str) -> 'AliasedTable':
        return AliasedTable(self.entity(), aliased_name)

    def column_by_name(self, column_name:ColumnName):
        if not self.reference_resolved():
            return ColumnRef(self, column_name)
        return self.entity().column_by_name(column_name)

    def __repr__(self) -> str:
        return repr(self.db) + '.ref:`' + self.name + '`'

    def __sql__(self):
        return self.entity().__sql__()

    def column_connections(self):
        return self.entity().column_connections()




# Reference of Column
class ColumnRef(ColumnExpr):
    """ A (forward) reference to column object using a column name """

    def __init__(self, table:TableRef, name:ColumnName) -> None:
        self.table = table
        self.name = name
        if self.table._entity:
            self._entity = self.table.entity().column(name)

    def resolve_reference(self):
        if not self.reference_resolved():
            self.table.resolve_reference()
            self._entity = self.table.entity().column(self.name)

    def reference_resolved(self) -> bool:
        return hasattr(self, '_entity')

    def entity(self) -> 'Column':
        if not self.reference_resolved():
            self.resolve_reference()
        return self._entity

    def __repr__(self) -> str:
        return repr(self.table) + '.`' + self.name + '`'

    def __sql__(self):
        return self.entity().__sql__()

    def column_connections(self):
        return self.entity().column_connections()




class Column(ColumnExpr):
    """ A Column object in the table """
    
    def __init__(self, 
        name: Union[str, ColumnName], # Column name
        basetype: str, # SQL Type
        *,
        nullable: bool = False, # nullable or not
        default: Optional[Expr] = None, # default value
        is_unique: bool = False, # unique key or not
        is_primary: bool = False, # primary key or not
        auto_increment: Optional[bool] = None, # auto increment or not
        links: Sequence[Union['Column', ColumnRef]] = [], # linked columns
    ) -> None:
        self.name = ColumnName(name)
        self.basetype = basetype
        # self.pytype = sql_keywords.types[basetype]
        self.nullable = nullable
        self.default_expr = default
        self.is_unique = is_unique
        self.is_primary = is_primary
        self.auto_increment = auto_increment
        self.links = list(links)
        self.table:'Table'
        self.column_links_table:Dict[TableName, Column]
        self._reference_resolved = False

        # self.column_links_table:Dict[str, Union['Column', ColumnRef]] = {} # key: table name
        # for link_column in links:
        #     if link_column.table in self.column_links_table:
        #         raise RuntimeError('Cannot link to multiple columns in the same table.')
        #     self.column_links_table[link_column.table.name] = link_column


    def set_table(self, table:'Table') -> 'Column':
        """ set the table object """
        self.table = table
        return self

    def resolve_reference(self) -> None:
        """ resolve references and generate link information """
        self.column_links_table = {}
        for column_or_ref in self.links:
            if isinstance(column_or_ref, ColumnRef):
                column = column_or_ref.resolve_reference()
            else:
                column = column_or_ref
            if column.table.name in self.column_links_table:
                raise RuntimeError('Multiple link columns to the same table exist.')
            self.column_links_table[column.table.name] = column
        self._reference_resolved = True

    def reference_resolved(self) -> bool:
        """ Return if the references are resolved """
        return self._reference_resolved


    def db(self) -> 'Database':
        """ Get the database object """
        return self.table.db

    def entity(self) -> 'Column':
        """ Return the entity object (this object) """
        return self

    def __sql__(self) -> Query:
        """ Generate the sql query representation """
        return Query(self.table, '.' + Query.as_obj(self.name))

    def __repr__(self) -> str:
        """ Get the string representation for debug """
        return repr(self.table) + '.' + self.name

    def column_connections(self) -> Iterator[Tuple['Column', 'Column']]:
        """ Get column connections (yield nothing) """
        while False:
            yield None

    def has_connection_to(self, table_or_name:Union[TableName, TableExpr]) -> bool:
        """ Return if this column has connection to the given table """
        if not self.reference_resolved:
            raise RuntimeError('References are not resolved.')
        table = self.db().table(table_or_name)
        return table.name in self.column_links_table

    def connection_to(self, table_or_name:Union[TableName, TableExpr]):
        """ Get the connection object to the given table """
        table = self.db().table(table_or_name)
        if not self.has_connection_to(table):
            raise RuntimeError('Link from {} to {} is not found.'.format(repr(self), repr(table)))
        return self.column_links_table[table.name]

    def creation_sql(self) -> Query:
        """ Get the sql query to create this column """
        q = Query(self.name, self.basetype)
        if self.nullable: q += 'NOT NULL'
        if self.default_expr: q += Query('DEFAULT', self.default_expr)
        if self.is_unique: q += 'UNIQUE KEY'
        if self.is_primary: q += 'PRIMARY KEY'
        if self.auto_increment: q += 'AUTO_INCREMENT'
        return q
        

# class ColumnsInTable(Expr):
    
#     def __init__(self, table:Union['Table', 'LinkedTable'], column_names:Iterable[str]):
#         self.table = table
#         self.columns = list(map(self.table.column, column_names))

#     def __iter__(self):
#         return iter(self.columns)

#     def column_connections(self) -> Iterator[Tuple[Column, Column]]:
#         return self.table.column_connections()

#     def __sql__(self) -> Query:
#         return Query(self.columns)


class Table(TableExpr):

    def __init__(self,
        db:'Database',
        name:TableName,
        columns:Iterable[Column],
        **options
    ) -> None:
        self.db = db
        self.name = name
        self.columns = list(column.set_table(self) for column in columns)
        self.column_dict = {column.name: column for column in self.columns}
        # self.column_links_table = [c for c in self.columns if c.links]
        # self.linked_tables = set(column.link.table for column in self.column_links_table)
        # self.linkpaths:Optional[Dict[TableName, List[Table]]] = None
        # self.linked_fk_cols:List[Column] = set()
        # self.linked_tables = set()
        self.created_on_db = None
        self._reference_resolved = False

        self.db.append_table(self)

        key_columns = [c for c in self.columns if c.is_primary]
        if len(key_columns) >= 2:
            raise RuntimeError('Multiple primary key is not allowed.')
        if not len(key_columns):
            raise RuntimeError('Primary key not found.')
        self.key_column = key_columns[0]

        # for fkey_col in self.fk_cols:
        #     fkey_col.link_col.table.linked_fk_cols.add(fkey_col)
        #     fkey_col.link_col.table.linked_tables.add(fkey_col.table)

    def resolve_references(self) -> None:
        """ Resolve references of the columns in this table """

        for column in self.columns:
            column.resolve_reference()

        self.link_columns_to_table:Dict[TableName, List[Column]] = {}
        for column_from in self.columns:
            for table_name in column_from.column_links_table:
                if table_name not in self.link_columns_to_table:
                    self.link_columns_to_table[table_name] = []
                self.link_columns_to_table[table_name].append(column_from)

        self._reference_resolved = True

    def reference_resolved(self) -> bool:
        """ Returns if references are resolved or not """
        return self._reference_resolved


    ### ---- abstract method implementations ---- ###

    def __sql__(self) -> Query:
        """ SQL-query convertion """
        return Query.as_obj(self.name)

    def __repr__(self) -> str:
        """ string representation for debug """
        return self.db.name + '{' + self.name + '}'

    def entity(self) -> 'Table':
        """ get the unreferenced original table object """
        return self

    def _make_column(self, column_entity:Column) -> Column:
        """ Return the column object belongs to this table """
        return column_entity

    def alias(self, aliased_name:str) -> 'AliasedTable':
        """ Get the aliased table (a table object with alias name) """
        return AliasedTable(self, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        """ Return column-connections (No connections) """
        while False:
            yield None

    
    ### ---- Database actions for table itself ---- ###

    def exists_in_db(self) -> bool:
        """ Check the existense on the database """
        return len(self.db.exec(Query(
            'SHOW TABLES LIKE', Query(self.name, as_obj=True)
        )).fetch_all()) > 0

    def creation_sql(self) -> Query:
        """ Get the sql query to create table """
        return Query(
            'CREATE TABLE ',
            Query(self.name, as_obj=True),
            '(', [c.creation_sql() for c in self.columns], ')'
        )

    def create(self) -> ExecutionQuery:
        """ Create table on the database """
        return ExecutionQuery(self.creation_sql())

    def create_if_not_exists(self) -> Optional[SQLExecResult]:
        if self.exists_in_db():
            return None
        return self.create()

    def truncate(self) -> ExecutionQuery:
        """ SQL Truncate table """
        return ExecutionQuery(Query('TRUNCATE TABLE', self))

    def drop(self) -> ExecutionQuery:
        """ SQL Drop table """
        return ExecutionQuery(Query('DROP TABLE', self))

        
    ### ---- Database actions for table records ---- ###

    def prepare_select(self,
        columns: Optional[Sequence[Union[ColumnName, Expr]]] = None,
        *args,
        **kwargs
    ) -> 'Select':
        if columns is None:
            return Select(self.db, self.columns, *args, **kwargs)
        return Select(
            self.db, 
            [(self.to_self_column(column) if isinstance(column, str) else column)
                for column in columns],
            *args,
            **kwargs
        )
    
    def select(self,
        columns: Optional[Sequence[Union[ColumnName, Expr]]] = None,
        *args,
        **kwargs
    ) -> ExecutionQuery:
        """ SQL SELECT query """
        return self.prepare_select(columns, *args, **kwargs).execution_query()

    def insert(self,
        columns_or_names: Sequence[Union[ColumnName, Column]],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> ExecutionQuery:
        """ SQL INSERT query """
        return ExecutionQuery(
            Query(
                'INSERT INTO', self, '(', [Query.as_obj(c.name) for c in map(self.to_self_column, columns_or_names)], ')',
                'VALUES', '(', [Query('%s') for _ in range(len(columns_or_names))], ')'
            ),
            ((self.to_query_exec_val(val) for val in vals) for vals in vals_itr),
        )
        
    def update(self,
        _raw_column_exprs: Union[Dict[ColumnName, ExprLike], Iterable[Tuple[Union[ColumnName, Column], ExprLike]]],
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> ExecutionQuery:
        """ SQL UPDATE query """
        if isinstance(_raw_column_exprs, dict):
            raw_column_exprs = _raw_column_exprs.items()
        else:
            raw_column_exprs = _raw_column_exprs
        return ExecutionQuery(Query(
            'UPDATE', self,
            'SET', [
                Query(self.to_self_column(column_or_name), '=', to_expr(expr))
                for column_or_name, expr in raw_column_exprs
            ],
            Query('WHERE', to_expr(where)) if where else None,
            Query('LIMIT', count) if count else None,
        ))

    def delete(self,
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> ExecutionQuery:
        """ SQL DELETE query """
        return ExecutionQuery(Query(
            'DELETE FROM', self,
            Query('WHERE', to_expr(where)) if where else None,
            Query('LIMIT', count) if count else None,
        ))

    def select_key_with_insertion(self,
        columns : List[Column],
        records_itr: Iterable[Iterable[ExprLike]],
    ) -> Dict[tuple, int]:
        """ SQL selections and insertion query """

        if self.key_column is None: raise RuntimeError('No primary key found in this table.')

        records = list(records_itr)

        # Get existing records
        vals_to_key = {vals:key for key, *vals in self.select(
            [self.key_column, *columns],
            where=(columns in to_expr(records)))}
        
        # Insert new records and get their keys
        new_records = filter(lambda rec: rec not in vals_to_key, records_itr)
        self.insert(columns, new_records)

        return {
            **vals_to_key,
            **{vals:key for key, *vals in self.select(
                [self.key_column, *columns],
                where=(columns in to_expr(new_records))
            )}
        }


    ## ---- column utility methods ---- ##

    def to_self_column(self, column_or_name:Union[ColumnName, Column]) -> Column:
        """ Get the column object which belongs to this table """

        if isinstance(column_or_name, str):
            column = self.column(column_or_name)
        else:
            column = column_or_name
        
        if not is_same(column.table, self):
            raise RuntimeError('Column(s) in the other table(s) are specified.')
        
        return column

    def column_by_name(self, name:ColumnName) -> Column:
        """ get column by name in this table """
        if not name in self.column_dict:
            raise RuntimeError('Column not found in this table.')
        return self.column_dict[name]

    def column_exists(self, col_or_name:Union[str, ColumnExpr]) -> bool:
        """ check if the column exists in this table """
        if isinstance(col_or_name, ColumnExpr):
            return is_same(col_or_name.entity().table, self)
        return col_or_name in self.column_dict

        
    ## ---- connection utility methods ---- ##

    def has_connection_to(self, dest_table:'Table') -> bool:
        """ Check if this table has just one connection to the given table """
        if not self.reference_resolved():
            raise RuntimeError('References are not resolved in {}.'.format(repr(self)))
        return (
            not dest_table.name in self.link_columns_to_table
            or len(self.link_columns_to_table[dest_table.name]) != 1
        )

    def connection_to(self, dest_table:'Table') -> Column:
        """ Get the connecting column to the given table """
        if not self.has_connection_to(dest_table):
            raise RuntimeError('There are no connection or multiple connection from {} to {}'.format(repr(self), repr(dest_table)))
        return self.link_columns_to_table[dest_table.name][0]

    ## ---- database utility class --- ##

    @staticmethod
    def to_query_exec_val(v:Any) -> QueryExecValTypes:
        if v is None:
            return v
        if isinstance(v, (bool, int, float, str)):
            return v
        if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
            return v.isoformat()

        return str(v)


class ColumnInLinkedTable(ColumnExpr):
    """ A specific column in the linked-table """

    def __init__(self, linked_table:'LinkedTable', column:Column) -> None:
        if not isinstance(linked_table, LinkedTable) or not isinstance(column, Column):
            raise TypeError()
        self.linked_table = linked_table
        self.column = column

    def db(self) -> 'Database':
        """ Return the database object """
        return self.column.db()

    def entity(self) -> Column:
        return self.column.entity()

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        return self.linked_table.column_connections()

    def connection_to(self, table_or_name:Union[TableName, 'Table']) -> Column:
        return self.column.connection_to(table_or_name)

    def __repr__(self) -> str:
        return repr(self.column) + '<-' + repr(self.linked_table.linking_column)

    def __sql__(self) -> Query:
        return self.column.__sql__()


class LinkedTable(TableExpr):
    """ A linked-table object """

    def __init__(self, table:Table, linking_column:Union[Column, ColumnInLinkedTable]) -> None:

        # Check types
        if not isinstance(table, Table):
            raise TypeError('Unexcepted table type `{}`.'.format(type(table)))
        if not isinstance(linking_column, (Column, ColumnInLinkedTable)):
            raise TypeError('Unexcepted column type `{}`.'.format(type(linking_column)))

        self.table = table
        self.linking_column = linking_column
        
        # Check given connection
        if not self.linking_column.entity().has_connection_to(table):
            raise RuntimeError('There is no connection form {} to {}.'.format(repr(self.linking_column), repr(self.table)))

    def entity(self) -> Table:
        return self.table.entity()

    def _make_column(self, column_entity:Column) -> ColumnInLinkedTable:
        return ColumnInLinkedTable(self, column_entity)

    def alias(self, aliased_name:str) -> 'AliasedTable':
        return AliasedTable(self, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        yield from self.linking_column.column_connections()
        column = self.linking_column.entity()
        yield (column, column.column_links_table[self.table.name])

    def __repr__(self) -> str:
        return (
            self.entity().db.name
            + '{' + repr(self.table) + '<-' + repr(self.linking_column) + '}'
        )

    def __sql__(self) -> Query:
        return self.table.__sql__()


class ColumnInAliasedTable(ColumnExpr):
    """ A specific column in the aliased-table """
    
    def __init__(self, aliased_table:'AliasedTable', column:Column) -> None:

        # Check type
        if not isinstance(aliased_table, AliasedTable):
            raise TypeError('Unexcepted table type `{}`.'.format(type(aliased_table)))

        self.aliased_table = aliased_table
        self.column = column

    def entity(self) -> Column:
        return self.column.entity()

    def link_to(self, val):
        return self.column.link_to(val)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        return self.aliased_table.column_connections()

    def __sql__(self) -> Query:
        """ SQL-query convertion """
        return Query(self.aliased_table, '.' + Query.as_obj(self.column.entity().name))

    def __repr__(self) -> str:
        """ Get the string representation for debug """
        return repr(self.aliased_table) + '.' + self.column.entity().name


class AliasedTable(TableExpr):
    """ Table with alias name """
    
    def __init__(self, table:Union[Table, LinkedTable], alias_name:str) -> None:

        # Check type
        if not isinstance(table, Table) and not isinstance(table, LinkedTable):
            raise TypeError('Unexcepted table type `{}`.'.format(type(table)))

        self.table = table
        self.alias_name = alias_name

    def entity(self) -> Table:
        """ Get the original (not alised, not linked) table object """
        return self.table.entity()

    def _make_column(self, column_entity:Column) -> ColumnInAliasedTable:
        """ Get the aliased-column object (the specified column in this aliased-table) """
        return ColumnInAliasedTable(self, column_entity)

    def alias(self, aliased_name:str) -> 'AliasedTable':
        """ Get an aliased-table with alternative name """ 
        return AliasedTable(self.table, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        """ Get column connections  """
        if isinstance(self.table, LinkedTable): 
            yield from self.table.linking_column.column_connections()
            column = self.table.linking_column.entity()
            yield (
                column,
                self.column(
                    column.column_links_table[self.table.entity().name]
                ).entity()
            )
            return
        return self.table.column_connections()

    def __sql__(self) -> Query:
        """ SQL-query convertion """
        return Query.as_obj(self.alias_name)

    def __full_sql__(self) -> Query:
        """ SQL-query of original table with alias """
        return Query(self.table, 'AS', Query.as_obj(self.alias_name))

    def __repr__(self) -> str:
        """ Get the string representation for debug """
        return self.entity().db.name + '{' + repr(self.table) + '@' + self.alias_name + '}'



class Database(SchemaExpr):
    """ Database schema object """

    def __init__(self, name:str) -> None:
        self.name = name
        self.tables:List[Table] = []
        self.table_dict :Dict[str, Table] = {}
        self.column_dict:Dict[str, Column] = {}
        # self.tables_priority:Dict[TableName, int] = {}

        self.reference_resolved = False


    ## ---- override methods ---- ##

    def __sql__(self) -> Query:
        return Query(self.name, as_obj=True)

    def __repr__(self) -> str:
        return self.name


    ## ---- table getting methods ---- ##

    def table(self, name:Union[TableName, TableExpr]) -> Table:
        """ Get table object by table name """
        if isinstance(name, TableExpr):
            table = name.entity()
            if not is_same(table.db, self):
                raise KeyError('Table {} not found.'.format(repr(table)))
            return table

        if not name in self.table_dict:
            raise KeyError('Table `{}` not found.'.format(name))

        return self.table_dict[name]

    def table_exists(self, name:Union[TableName, TableExpr]) -> bool:
        """ Check if a table with the specified name exists """
        if isinstance(name, TableExpr):
            return is_same(name.entity().db, self)
        return name in self.table_dict

    def table_ref(self, name:TableName) -> TableRef:
        """ Get the temporary reference of table """
        return TableRef(self, name)

    def table_or_ref(self, name:Union[TableName, TableExpr]) -> Union[Table, TableRef]:
        """ Get table object or its reference by table name """
        if isinstance(name, TableExpr):
            table = name.entity()
            if is_same(table.db, self):
                return table
            return self.table_ref(table.name)
        
        if name in self.table_dict:
            return self.table_dict[name]
        return self.table_ref(name)

    def link_to(self, val):
        if isinstance(val, str) or isinstance(val, TableExpr):
            return self.table(val)

        if isinstance(val, Iterable):
            return SchemaExprs(map(self.link_to, val))

        raise TypeError('Unexcepted type `{}`.'.format(type(val)))
        

    ## ---- table creation methods ---- ##
    
    def table_class(self, table_class:Type) -> Type:
        """ Table class decorator """
        props = get_type_hints(table_class)
        print(props)
        return table_class

    def prepare_table(self,
        name:Union[str, TableName],
        columns:Iterable[Column],
        **options
    ) -> Table:
        """ Prepare table for this database with table schema """
        return Table(self, TableName(name), columns, **options)

    # Called by Table object
    def append_table(self, table:Table) -> None:
        self.tables.append(table)
        self.table_dict[table.name] = table
        for column in table.columns:
            self.column_dict[table.name + '.' + column.name] = column


    ### ---- table resolution methods ---- ####

    def resolve_references(self) -> None:
        """ Resolve references in tables """
        for table in self.tables:
            table.resolve_references()

        self.reference_resolved = True
        

    def finalize_tables(self) -> None:
        self.resolve_references()
        # self.refresh_tables_priority()
        # for table in self.sort_tables_by_priority(self.tables):
        #     table.create_if_not_exists()
        

    ### ---- Database methods ---- ####

    def prepare_select(self, *args, **kwargs) -> 'Select':
        return Select(self, *args, **kwargs)

    def select(self, *args, **kwargs) -> SQLExecResult:
        return self.prepare_select(*args, **kwargs).exec()

    def insert(self,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> Tuple[str, Iterable[Iterable[QueryExecValTypes]]]:
        """ Insert records across tables """
        # TODO: Implementation
        pass
