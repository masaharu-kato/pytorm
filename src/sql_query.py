from typing import Any, Callable, cast, Dict, final, get_type_hints, Iterable, Iterator, List, NewType, NoReturn, Optional, overload, Sequence, Tuple, Type, TypeVar, Union
from abc import ABCMeta, abstractmethod
import sql_keywords
import datetime
import mysql.connector # type:ignore


class Expr(metaclass=ABCMeta):
    """ SQL expression base type """
    def __add__(self, expr): return OpExpr('+', self, expr)
    def __sub__(self, expr): return OpExpr('-', self, expr)
    def __mul__(self, expr): return OpExpr('*', self, expr)
    def __mod__(self, expr): return OpExpr('%', self, expr)
    def __and__(self, expr): return OpExpr('&', self, expr)
    def __or__ (self, expr): return OpExpr('|', self, expr)
    def __radd__(self, expr): return OpExpr('+', expr, self)
    def __rsub__(self, expr): return OpExpr('-', expr, self)
    def __rmul__(self, expr): return OpExpr('*', expr, self)
    def __rmod__(self, expr): return OpExpr('%', expr, self)
    def __rand__(self, expr): return OpExpr('&', expr, self)
    def __ror__ (self, expr): return OpExpr('|', expr, self)

    def __lt__(self, expr): return OpExpr('<' , self, expr)
    def __le__(self, expr): return OpExpr('<=', self, expr)
    def __eq__(self, expr): return OpExpr('=' , self, expr)
    def __ne__(self, expr): return OpExpr('!=', self, expr)
    def __gt__(self, expr): return OpExpr('>' , self, expr)
    def __ge__(self, expr): return OpExpr('>=', self, expr)

    def __contains__(self, expr):
        """ 'A in B' expression """
        return OpExpr('IN', expr, self)

    @abstractmethod
    def __sql__(self) -> 'SQLQuery': pass

    def __full_sql__(self) -> 'SQLQuery':
        return self.__sql__() # default implementation

    def alias(self, alias_name:str):
        return AliasedExpr(self, alias_name)

    @final
    def __matmul__(self, alias_name:str):
        if not isinstance(alias_name, str):
            return NotImplemented
        return self.alias(alias_name)

    @abstractmethod
    def __repr__(self) -> str: pass

    def __bool__(self) -> bool:
        raise NotImplementedError()

    def __int__(self) -> int:
        raise NotImplementedError()

    def __float__(self) -> float:
        raise NotImplementedError()

    def __str__(self) -> str:
        raise NotImplementedError()

    def __hash__(self) -> int:
        raise NotImplementedError()


def is_same(expr1:Expr, expr2:Expr) -> bool:
    return repr(expr1) == repr(expr2)
    
    
def to_expr(v:Any) -> Expr:
    if isinstance(v, Expr):
        return v
    if not isinstance(v, str) and isinstance(v, Iterable):
        return Values([to_expr(_v) for _v in v])
    return Value(v)


RawValTypes = Union[bool, int, float, str, datetime.datetime, datetime.date, datetime.time]
QueryExecValTypes = Optional[Union[bool, int, float, str]]
ExprLike = Union[Expr, RawValTypes]
Exprs = Union[Expr, Sequence[Expr]]
QueryTypes = Optional[Union['SQLQuery', Expr, RawValTypes, Iterable[Union['SQLQuery', Expr, RawValTypes]]]]


class AliasedExpr(Expr):
    """ SQL Expression with alias name """

    def __init__(self, expr:Expr, alias_name:str) -> None:
        self.expr = expr
        self.alias_name = alias_name

    def __sql__(self) -> 'SQLQuery':
        return q_obj(self.alias_name)

    def __full_sql__(self) -> 'SQLQuery':
        return SQLQuery(self.expr.__sql__(), 'AS', q_obj(self.alias_name))

    def __repr__(self) -> str:
        return '{' + repr(self.expr) + '@' + self.alias_name + '}'



class SQLQuery(Expr):
    """ SQL Query text object """

    def __init__(self, *exprs:QueryTypes, **options) -> None:
        self.q = ''
        for expr in exprs:
            self._add_text(self._to_str(expr, **options))


    def _add_text(self, text:Optional[str]) -> None:
        if text:
            # print('self.q({}) <- text({})'.format(self.q, text))
            # print(self.q[-1] if self.q else None, text[0])
            if self.q and self.q[-1] not in {'.', '('} and text[0] not in {'.', '(', ')'}:
                self.q += ' '
            self.q += text

    def __add__(self, expr:QueryTypes) -> 'SQLQuery':
        return SQLQuery(self.q, self._to_str(expr))

    def __radd__(self, expr:QueryTypes) -> 'SQLQuery':
        return SQLQuery(self._to_str(expr), self.q)

    def __iadd__(self, expr:QueryTypes) -> 'SQLQuery':
        self._add_text(self._to_str(expr))
        return self

    def __sql__(self) -> 'SQLQuery':
        return self

    def __repr__(self) -> str:
        return 'SQL(' + self.q + ')'
            
    def query(self) -> str:
        """ Get Query string """
        return self.q

    @classmethod
    def _to_str(cls,
        obj:QueryTypes,
        *,
        as_obj:bool=False,
        quoted:bool=False,
        use_full:bool=False,
    ) -> str:

        if obj is None:
            return ''

        if isinstance(obj, int):
            if as_obj or quoted:
                raise RuntimeError('Cannot specify `as_obj` or `quoted` for integer value.')
            return str(obj)

        if isinstance(obj, str):
            raw = obj
        else:
            if isinstance(obj, Iterable):
                return ', '.join(map(cls._to_str, obj))

            if not isinstance(obj, Expr):
                raise RuntimeError('Cannot convert value to SQL format.')

            if use_full:
                raw = obj.__full_sql__().query()
            else:
                raw = obj.__sql__().query()
                
        if as_obj:
            if quoted:
                raise RuntimeError('Cannot specify both `as_obj` and `quoted`.')
            return '`' + raw.replace('`', '``') + '`'

        if quoted:
            return '"' + raw.replace('\\', '\\\\').replace('"', '\\"')

        return raw


def q_obj(*obj:QueryTypes) -> SQLQuery:
    return SQLQuery(*obj, as_obj=True)


class Value(Expr):
    """ SQL value expression (int, float, string, datetime, ...) """

    def __init__(self, v:RawValTypes) -> None:
        self.v = v

    def __sql__(self) -> SQLQuery:
        if isinstance(self.v, int) or isinstance(self.v, float):
            return SQLQuery(str(self.v))

        if isinstance(self.v, datetime.datetime) or isinstance(self.v, datetime.date) or isinstance(self.v, datetime.time):
            return SQLQuery(self.v.isoformat(), quoted=True)

        return SQLQuery(self.v.replace('"', '\\"'), quoted=True)

    def __repr__(self) -> str:
        return 'Val(' + repr(self.v) + ')'


class Values(Expr):
    """ SQL multiple values expression """

    def __init__(self, values:Iterable[Expr]):
        self.values = [to_expr(v) for v in values]

    def __iter__(self):
        return self.values

    def __sql__(self) -> SQLQuery:
        return SQLQuery('(', self.values, ')')

    def __repr__(self) -> str:
        return 'Vals(' + ', '.join(map(repr, self.values)) + ')'


class OpExpr(Expr):
    """ Operator expression """

    def __init__(self, op:str, larg, rarg) -> None:
        self.op = op
        self.larg = to_expr(larg)
        self.rarg = to_expr(rarg)

    def __sql__(self) -> SQLQuery:
        if self.op not in sql_keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return SQLQuery('(', self.larg, self.op, self.rarg, ')')

    def __repr__(self) -> str:
        return 'Op:' + repr(self.larg) + ' ' + self.op + ' ' + repr(self.rarg) + ')'


# class MultipleOperation(Expr):

#     def __init__(self, op:str, exprs):
#         self.op = op
#         self.exprs = to_expr(exprs)

#     def __sql__(self) -> SQLQuery:
#         if self.op not in sql_keywords.operators:
#             raise RuntimeError('Unknown operator `{}`'.format(self.op))
#         return '({})'.format(' {} '.format(self.op).join(self.exprs)) 



class FuncExpr(Expr):
    """ Function expression """
    
    def __init__(self, name:str, *args) -> None:
        self.name = name
        self.args = [to_expr(arg) for arg in args]

    def __sql__(self) -> SQLQuery:
        if self.name not in sql_keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return SQLQuery(self.name, '(', self.args, ')')

    def __repr__(self) -> str:
        return 'Func:' + self.name + '(' + ', '.join(map(repr, self.args)) + ')'


class SQLExecResult:
    """ SQL Execution Result Object """

    def fetch(self) -> tuple:
        return tuple()

    def fetch_all(self) -> List[tuple]:
        return []

    def __iter__(self):
        while False:
            yield None


class SQLExecutor:
    """ SQL Executor Object (Database Cursor) """

    def __init__(self, *args, **options) -> None:
        # TODO: Implementation
        self.conn = None

    def connect(self, *args, **kwargs):
        self.close_connection()
        self.conn = mysql.connector.connect(*args, **kwargs)

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()


    def exec(self, _q:SQLQuery, values:Optional[Iterable[QueryExecValTypes]] = None) -> SQLExecResult:
        q = to_sql_query(_q)
        print('Exec SQL:', q, 'Values:', list(values) if values else None)
        
        return SQLExecResult()

    def exec_many(self, _q:SQLQuery, vals_itr:Iterable[Iterable[QueryExecValTypes]]) -> SQLExecResult:
        q = to_sql_query(_q)
        print('Execmany SQL:', q, 'Values:', [list(vals) for vals in vals_itr])
        return SQLExecResult()


def to_sql_query(q:SQLQuery) -> str:
    return q.query()



# class ColType:

#     def __init__(self, basetype:str, *, nullable:bool=True, default:Optional[Expr]=None):
#         self.basetype = basetype
#         self.nullable = nullable
#         self.default = default


#     def pytype(self) -> Type:
#         pybasetype = sql_keywords.types[self.basetype]
#         if self.nullable:
#             return cast(Type, Optional[pybasetype])
#         return pybasetype


#     def __sql__(self) -> str:
#         return self.basetype


#     def creation_sql(self) -> str:
#         return self.basetype + (' NOT NULL' if not self.nullable else '')



# ColIntType = ColType('INT', nullable=False)

        
TableName = NewType('TableName', str)
ColumnName = NewType('ColumnName', str)


class SchemaExpr(Expr):

    @abstractmethod
    def __repr__(self) -> str: pass
    
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
        pass

    @final
    def __rshift__(self, val):
        """ 
            Operator self >> val implementation
            Alias of link_to method
        """
        return self.link_to(val)

    @final
    def __lshift__(self, val):
        """ 
            Operator self << val implementation
            Alias of link_to method
        """
        if not hasattr(val, 'link_to'):
            return NotImplemented
        return val.link_to(self)

    @final
    def __rrshift__(self, val):
        """ 
            Operator val >> self implementation
            Alias of link_to method
        """
        if not hasattr(val, 'link_to'):
            return NotImplemented
        return val.link_to(self)

    @final
    def __rlshift__(self, val):
        """ 
            Operator val << self implementation
            Alias of link_to method
        """
        return self.link_to(val)

    @final
    def __getitem__(self, val):
        """ 
            Operator self[val] implementation
            Alias of link_to method
        """
        return self.link_to(val)


class SingleSchemaExpr(SchemaExpr):

    @abstractmethod
    def entity(self):
        """ Return the pure schema object (not referenced, linked, or aliased schema object) """
        pass

    @abstractmethod
    def column_connections(self) -> Iterator[Tuple['Column', 'Column']]: pass


class ColumnExpr(SingleSchemaExpr):
    """
        ColumnExpr Expression (Abstract class)
    """

    @abstractmethod
    def entity(self) -> 'Column': 
        """ Return the pure column object (not referenced, linked, or aliased column) """
        pass

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

        if isinstance(val, LinkedColumn):
            return self.link_to(val.linked_table).column(val.column)

        if isinstance(val, AliasedTable):
            return AliasedTable(self.link_to(val.table), val.alias_name)

        if isinstance(val, AliasedColumn):
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
        pass

    @abstractmethod
    def _make_column(self, column_entity:'Column'):
        """ Return a column object which belongs to self table """
        pass

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
            return LinkedTable(val, self.entity().column_links_to_table(val))

        if isinstance(val, Column):
            if self.entity().column_exists(val):
                return self.column(val)
            return self.link_to(val.table).column(val)

        if isinstance(val, LinkedTable):
            return self.link_to(val.linking_column).link_to(val.table)

        if isinstance(val, LinkedColumn):
            return self.link_to(val.linked_table).column(val.column)

        if isinstance(val, AliasedTable):
            return AliasedTable(self.link_to(val.table), val.alias_name)

        if isinstance(val, AliasedColumn):
            return self.link_to(val.aliased_table).column(val.column)

        if isinstance(val, Iterable):
            return SchemaExprs(map(self.link_to, val))

        raise TypeError('Unexcepted type `{}`.'.format(type(val)))


    @abstractmethod
    def alias(self, alias_name:str) -> 'AliasedTable':
        """ Return a aliased-table object of self """
        pass


class SchemaExprs(SchemaExpr):

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

    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.schemas)

    def __repr__(self) -> str:
        return '(' + ', '.join(map(repr, self.schemas)) + ')'

    def __iter__(self) -> Iterator[SchemaExpr]:
        return iter(self.schemas)

    # def __hash__(self) -> int:
    #     return hash(repr(self))

    # def __eq__(self, other) -> bool:
    #     if not isinstance(other, SchemaExprs):
    #         raise NotImplementedError()
    #     return len(self.schemas) == len(other.schemas) and all(self_s == other_s for self_s, other_s in zip(self.schemas, other.schemas))

    # def __ne__(self, other) -> bool:
    #     return not self.__eq__(other)



# Reference of Table
class TableRef(TableExpr):

    def __init__(self, db:'Database', name:TableName) -> None:
        self.db = db
        self.name = name
        if self.db.table_exists(name):
            self._entity = self.db.table(name)

    def resolve_reference(self) -> None:
        if not self.reference_resolved():
            if not self.db.table_exists(self.name):
                raise RuntimeError('Failed to resolve reference: Table `{}` was not found in {}'.format(self.name, repr(self.db)))
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
        self.name = cast(ColumnName, name)
        self.basetype = basetype
        # self.pytype = sql_keywords.types[basetype]
        self.nullable = nullable
        self.default_expr = default
        self.is_unique = is_unique
        self.is_primary = is_primary
        self.auto_increment = auto_increment
        self.links = list(links)

        self._reference_resolved = False

        # self.link_table_columns:Dict[str, Union['Column', ColumnRef]] = {} # key: table name
        # for link_column in links:
        #     if link_column.table in self.link_table_columns:
        #         raise RuntimeError('Cannot link to multiple columns in the same table.')
        #     self.link_table_columns[link_column.table.name] = link_column


    def set_table(self, table:'Table') -> 'Column':
        self.table = table
        return self


    def resolve_reference(self) -> None:
        self.link_table_columns:Dict[TableName, Column] = {}
        for column_or_ref in self.links:
            column = column_or_ref.resolve_reference() if isinstance(column_or_ref, ColumnRef) else column_or_ref
            if column.table.name in self.link_table_columns:
                raise RuntimeError('Multiple link columns to the same table exist.')
            self.link_table_columns[column.table.name] = column
        # print(repr(self), self.links, self.link_table_columns)
        self._reference_resolved = True

    def reference_resolved(self) -> bool:
        return self._reference_resolved


    def db(self) -> 'Database':
        return self.table.db

    # def base_table(self) -> 'Table':
    #     """ get the base table (from the linked table) """
    #     return self.table.base_table()

    def entity(self) -> 'Column':
        return self

    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.table, '.' + q_obj(self.name))

    def __repr__(self) -> str:
        return repr(self.table) + '.' + self.name

    def has_connection_to(self, table_or_name:Union[TableName, TableExpr]) -> bool:
        if not self.reference_resolved:
            raise RuntimeError('References are not resolved.')
        table = self.db().table(table_or_name)
        return table.name in self.link_table_columns


    def column_connections(self) -> Iterator[Tuple['Column', 'Column']]:
        return iter([])

    def link_dest_column_of_table(self, table_or_name:Union[TableName, TableExpr]):
        if not self.reference_resolved:
            raise RuntimeError('References are not resolved.')
        table = self.db().table(table_or_name)
        if not table.name in self.link_table_columns:
            raise RuntimeError('Link from {} to {} is not found.'.format(repr(self), repr(table)))
        
        return self.link_table_columns[table.name]


    def creation_sql(self) -> SQLQuery:
        q = SQLQuery(self.name, self.basetype)
        if self.nullable: q += 'NOT NULL'
        if self.default_expr: q += SQLQuery('DEFAULT', self.default_expr)
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

#     def __sql__(self) -> SQLQuery:
#         return SQLQuery(self.columns)




class Table(TableExpr):

    def __init__(self, db:'Database', name:TableName, columns:Iterable[Column], **options) -> None:
        self.db = db
        self.name = name
        self.columns = list(column.set_table(self) for column in columns)
        self.column_dict = {column.name: column for column in self.columns}
        # self.link_table_columns = [c for c in self.columns if c.links]
        # self.linked_tables = set(column.link.table for column in self.link_table_columns)
        # self.linkpaths:Optional[Dict[TableName, List[Table]]] = None
        # self.linked_fk_cols:List[Column] = set()
        # self.linked_tables = set()
        self.created_on_db = None

        self.reference_resolved = False

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
        for column in self.columns:
            column.resolve_reference()

        self.link_columns_to_table:Dict[TableName, List[Column]] = {}
        for column_from in self.columns:
            for table_name in column_from.link_table_columns:
                if table_name not in self.link_columns_to_table:
                    self.link_columns_to_table[table_name] = []
                self.link_columns_to_table[table_name].append(column_from)

        self.reference_resolved = True


    ### ---- abstract method implementations ---- ###

    def __sql__(self) -> SQLQuery:
        """ SQL-query convertion """
        return q_obj(self.name)

    def __repr__(self) -> str:
        """ string representation for debug """
        return self.db.name + '{' + self.name + '}'

    def entity(self) -> 'Table':
        """ get the unreferenced original table object """
        return self

    def _make_column(self, column_entity:Column) -> Column:
        return column_entity

    def alias(self, aliased_name:str) -> 'AliasedTable':
        return AliasedTable(self, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        """ Return column-connections (No connections) """
        while False:
            yield None


    # def expr_str_to_column(self, col:Union[ColumnName, Expr]) -> Expr:
    #     return self.column(col) if isinstance(col, str) else col

    
    ### ---- Database actions for table itself ---- ###

    def exists_in_db(self) -> bool:
        """ Check the existense on the database """
        return len(self.db.exec(SQLQuery('SHOW TABLES LIKE', SQLQuery(self.name, as_obj=True))).fetch_all()) > 0

    def creation_sql(self) -> SQLQuery:
        """ Get the sql query to create table """
        return SQLQuery('CREATE TABLE ', SQLQuery(self.name, as_obj=True), '(', [c.creation_sql() for c in self.columns], ')')

    def create(self) -> SQLExecResult:
        """ Create table on the database """
        return self.db.exec(self.creation_sql())

    def create_if_not_exists(self) -> Optional[SQLExecResult]:
        if self.exists_in_db(): return None
        return self.create()

    def truncate(self) -> SQLExecResult:
        """ SQL Truncate table """
        return self.db.exec(SQLQuery('TRUNCATE TABLE', self))

    def drop(self) -> SQLExecResult:
        """ SQL Drop table """
        return self.db.exec(SQLQuery('DROP TABLE' + self))

        
    ### ---- Database actions for table records ---- ###

    def prepare_select(self, columns: Optional[Sequence[Union[ColumnName, Expr]]] = None, *args, **kwargs) -> 'Select':
        if columns is None:
            return Select(self.db, self.columns, *args, **kwargs)
        return Select(self.db, [(self.to_self_column(column) if isinstance(column, str) else column) for column in columns], *args, **kwargs)
    
    def select(self, columns: Optional[Sequence[Union[ColumnName, Expr]]] = None, *args, **kwargs) -> SQLExecResult:
        """ SQL SELECT query """
        return self.prepare_select(columns, *args, **kwargs).exec()


    def insert(self,
        columns_or_names: Sequence[Union[ColumnName, Column]],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> SQLExecResult:
        """ SQL INSERT query """
        return self.db.exec_many(
            SQLQuery(
                'INSERT INTO', self, '(', [q_obj(c.name) for c in map(self.to_self_column, columns_or_names)], ')',
                'VALUES', '(', [SQLQuery('%s') for _ in range(len(columns_or_names))], ')'
            ),
            ((self.to_query_exec_val(val) for val in vals) for vals in vals_itr),
        )

        
    def update(self,
        _raw_column_exprs: Union[Dict[ColumnName, ExprLike], Iterable[Tuple[Union[ColumnName, Column], ExprLike]]],
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        """ SQL UPDATE query """
        raw_column_exprs = _raw_column_exprs.items() if isinstance(_raw_column_exprs, dict) else _raw_column_exprs
        column_exprs = ((self.to_self_column(column_or_name), to_expr(expr)) for column_or_name, expr in raw_column_exprs)

        return self.db.exec(SQLQuery(
            'UPDATE', self,
            'SET', [SQLQuery(column, '=', expr) for column, expr in column_exprs],
            SQLQuery('WHERE', to_expr(where)) if where else None,
            SQLQuery('LIMIT', count) if count else None,
        ))


    def delete(self,
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        """ SQL DELETE query """

        return self.db.exec(SQLQuery(
            'DELETE FROM', self,
            SQLQuery('WHERE', to_expr(where)) if where else None,
            SQLQuery('LIMIT', count) if count else None,
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

        return {**vals_to_key, **{vals:key for key, *vals in self.select([self.key_column, *columns], where=(columns in to_expr(new_records)))}}


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

    def has_connection_to(self, table:'Table') -> bool:
        """ Check if this table has just one connection to the given table """
        if not self.reference_resolved:
            raise RuntimeError('References are not resolved in {}.'.format(repr(self)))
        return not table in self.link_columns_to_table or len(self.link_columns_to_table[table.name]) != 1

    def column_links_to_table(self, table:'Table') -> Column:
        
        if not self.reference_resolved:
            raise RuntimeError('References are not resolved in {}.'.format(repr(self)))

        # base_table = table.base_table()
        base_table = table
        
        if not base_table.name in self.link_columns_to_table or not len(self.link_columns_to_table[base_table.name]):
            raise RuntimeError('There are no columns from {} to {}'.format(repr(self), repr(base_table)))

        columns = self.link_columns_to_table[base_table.name]
        if len(columns) > 1:
            raise RuntimeError('There are multiple columns ({}) to {}.'.format(', '.join(map(repr, columns)), repr(base_table)))

        return columns[0]

    ## ---- database utility class --- ##

    @staticmethod
    def to_query_exec_val(v:Any) -> QueryExecValTypes:
        if v is None:
            return v
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return v
        if isinstance(v, str):
            return v
        if isinstance(v, datetime.date):
            return v.isoformat()
        if isinstance(v, datetime.datetime):
            return v.isoformat()
        if isinstance(v, datetime.time):
            return v.isoformat()

        return str(v)



class LinkedColumn(ColumnExpr):
    def __init__(self, linked_table:'LinkedTable', column:Column) -> None:
        
        # Check types
        if not isinstance(linked_table, LinkedTable) or not isinstance(column, Column):
            raise TypeError()

        self.linked_table = linked_table
        self.column = column


    def db(self) -> 'Database':
        return self.column.db()

    def entity(self) -> Column:
        return self.column.entity()

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        return self.linked_table.column_connections()

    def link_dest_column_of_table(self, table_or_name:Union[TableName, 'Table']) -> Column:
        return self.column.link_dest_column_of_table(table_or_name)

    def __repr__(self) -> str:
        return repr(self.column) + '<-' + repr(self.linked_table.linking_column)

    def __sql__(self) -> SQLQuery:
        return self.column.__sql__()




class LinkedTable(TableExpr):
    def __init__(self, table:Table, linking_column:Union[Column, LinkedColumn]) -> None:

        # Check types
        if not isinstance(table, Table):
            raise TypeError('Unexcepted table type `{}`.'.format(type(table)))
        if not isinstance(linking_column, Column) and not isinstance(linking_column, LinkedColumn):
            raise TypeError('Unexcepted column type `{}`.'.format(type(linking_column)))


        self.table = table
        self.linking_column = linking_column
        
        # Check given connection
        if not self.linking_column.entity().has_connection_to(table):
            raise RuntimeError('There is no connection form {} to {}.'.format(repr(self.linking_column), repr(self.table)))

    def entity(self) -> Table:
        return self.table.entity()

    def _make_column(self, column_entity:Column) -> LinkedColumn:
        return LinkedColumn(self, column_entity)

    def alias(self, aliased_name:str) -> 'AliasedTable':
        return AliasedTable(self, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        yield from self.linking_column.column_connections()
        column = self.linking_column.entity()
        yield (column, column.link_table_columns[self.table.name])

    def __repr__(self) -> str:
        return self.entity().db.name + '{' + repr(self.table) + '<-' + repr(self.linking_column) + '}'

    def __sql__(self) -> SQLQuery:
        return self.table.__sql__()



class AliasedColumn(ColumnExpr):
    """ Column of table alias name """
    
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

    def __rshift__(self, val):
        """ Operator >> implementation """
        return self.link_to(val)

    def __lshift__(self, val):
        """ Operator << implementation """
        if not hasattr(val, 'link_to'):
            raise NotImplementedError()
        return val.link_to(self)

    def __getitem__(self, val):
        return self.column[val]

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        return self.aliased_table.column_connections()

    def __sql__(self) -> SQLQuery:
        """ SQL-query convertion """
        return SQLQuery(self.aliased_table, '.' + q_obj(self.column.entity().name))

    def __repr__(self) -> str:
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
        return self.table.entity()

    def _make_column(self, column_entity:Column) -> AliasedColumn:
        return AliasedColumn(self, column_entity)

    def alias(self, aliased_name:str) -> 'AliasedTable':
        return AliasedTable(self.table, aliased_name)

    def column_connections(self) -> Iterator[Tuple[Column, Column]]:
        if isinstance(self.table, LinkedTable): 
            yield from self.table.linking_column.column_connections()
            column = self.table.linking_column.entity()
            yield (column, self.column(column.link_table_columns[self.table.table.name]).entity())
            return
        return self.table.column_connections()

    def __sql__(self) -> SQLQuery:
        """ SQL-query convertion """
        return q_obj(self.alias_name)

    def __full_sql__(self) -> SQLQuery:
        """ SQL-query of original table with alias """
        return SQLQuery(self.table, 'AS', q_obj(self.alias_name))

    def __repr__(self) -> str:
        return self.entity().db.name + '{' + repr(self.table) + '@' + self.alias_name + '}'


class Database(SchemaExpr, SQLExecutor):
    def __init__(self, name:str) -> None:
        self.name = name
        self.tables:List[Table] = []
        self.table_dict :Dict[str, Table] = {}
        self.column_dict:Dict[str, Column] = {}
        # self.tables_priority:Dict[TableName, int] = {}

        self.reference_resolved = False


    ## ---- override methods ---- ##

    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.name, as_obj=True)

    def __repr__(self) -> str:
        return self.name


    ## ---- table getting methods ---- ##

    def table(self, name:Union[TableName, TableExpr]) -> Table:
        """ Get table object by table name """
        if isinstance(name, TableExpr):
            table = name.entity()
            if not is_same(table.db, self):
                raise RuntimeError('Table {} not found.'.format(repr(table)))
            return table

        if not name in self.table_dict:
            raise RuntimeError('Table `{}` not found.'.format(name))

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
            else:
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

    def prepare_table(self, name:Union[str, TableName], columns:Iterable[Column], **options) -> Table:
        """ Prepare table for this database with table schema """
        return Table(self, cast(TableName, name), columns, **options)

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



    # def refresh_tables_priority(self) -> None:
    #     tables_graph = {table: table.link_tables for table in self.tables}
    #     print(tables_graph)
    #     tres = toposort.toposort(tables_graph)
    #     for i, tables in enumerate(tres, 1):
    #         for table in tables:
    #             self.tables_priority[table] = i


    # def sort_tables_by_priority(self, tables:List[Table]) -> List[Table]:
    #     return sorted(tables, key=lambda table: self.tables_priority[table])


    # def tables_fk_columns(self, tables:List[Table]) -> Iterable[Tuple[Table, Iterable[Column]]]:
    #     sorted_tables = self.sort_tables_by_priority(tables)
    #     print('sorted_tables:', list(t.name for t in sorted_tables))
    #     for i, table in enumerate(sorted_tables):
    #         yield (table, (fk_col for fk_col in table.fk_cols if fk_col.link_col.table in sorted_tables[:i]))


    # def tables_inner_join(self, tables:List[Table]) -> SQLQuery:
    #     q = SQLQuery()
    #     f_table_only = False
    #     for table, fk_cols_itr in self.tables_fk_columns(tables):
    #         fk_cols = list(fk_cols_itr)
    #         if fk_cols:
    #             q += SQLQuery('INNER JOIN', table, 'ON', ' & '.join('{} = {}'.format(sql(fk_col), sql(fk_col.link_col)) for fk_col in fk_cols))
    #             f_table_only = False
    #         else:   
    #             if f_table_only: q += ','
    #             q += table
    #             f_table_only = True
    #     return q


    # @classmethod
    # def find_columns_in_expr(cls, expr:Expr) -> Iterator[Column]:
    #     if isinstance(expr, Column):
    #         yield expr
    #     elif isinstance(expr, OpExpr):
    #         yield from cls.find_columns_in_expr(expr.larg)
    #         yield from cls.find_columns_in_expr(expr.rarg)
    #     elif isinstance(expr, FuncExpr):
    #         for carg in expr.args:
    #             yield from cls.find_columns_in_expr(carg)
    #     elif isinstance(expr, ExprIn):
    #         yield from cls.find_columns_in_expr(expr.target) 
    #     return


    # @staticmethod
    # def tables_of_columns(columns:Iterable[Column]) -> Iterator[Table]:
    #     return iter(set(col.table for col in columns))
        

    # def tables_inner_join_by_exprs(self, exprs:Iterable[Expr]) -> SQLQuery:
    #     tables = list(self.tables_of_columns(itertools.chain.from_iterable(self.find_columns_in_expr(expr) for expr in exprs)))
    #     return self.tables_inner_join(tables)



class Select:

    def __init__(self,
        db     : Database,
        columns: Sequence[Expr],
        *,
        where  : Optional[ExprLike] = None,
        group  : Optional[Sequence[Expr]] = None,
        having : Optional[ExprLike] = None,
        order  : Optional[Iterable[Tuple[Expr, str]]] = None,
        # tables : Optional[Sequence[ExprLike]] = None,
        count  : Optional[int] = None,
        offset : Optional[int] = None,
    ) -> None:
        self.db: Database = db
        self.column_exprs: List[Expr] = list(columns)
        self.where_expr  : Optional[Expr] = to_expr(where) if where is not None else None
        self.group_exprs : Optional[Sequence[Expr]] = list(group) if group is not None else None
        self.having_expr : Optional[Expr] = to_expr(having) if having is not None else None
        self.order_exprs : Optional[List[Tuple[Expr, bool]]] = [(expr, self._is_asc_or_desc(ad_str)) for expr, ad_str in order] if order is not None else None
        # self.tables_expr = tables or self.db.tables_of_columns(self.column_exprs) 
        self.count : Optional[int] = count
        self.offset: Optional[int] = offset


    def sql_query(self) -> SQLQuery:
        q = SQLQuery('SELECT', self.column_exprs, 'FROM', self.tables_query())
        if self.where_expr  is not None: q += SQLQuery('WHERE', self.where_expr)
        if self.group_exprs is not None: q += SQLQuery('GROUP BY', self.group_exprs)
        if self.having_expr is not None: q += SQLQuery('HAVING', self.having_expr)
        if self.order_exprs is not None: q += SQLQuery('ORDER BY', [SQLQuery(column, ('ASC' if dstr else 'DESC')) for column, dstr in self.order_exprs])
        if self.count  is not None: q += SQLQuery('LIMIT', self.count)
        if self.offset is not None: q += SQLQuery('OFFSET', self.offset)
        return q

    def exec(self):
        self.result = self.db.exec(self.sql_query())

    def __iter__(self):
        return self.result

    def next_block(self):
        self.offset += self.count

    def prev_block(self):
        self.offset -= self.count


    @staticmethod
    def _is_asc_or_desc(s:str) -> bool:
        if s == '+' or s.upper() == 'A' or s.upper() == 'ASC':
            return True
        if s == '-' or s.upper() == 'D' or s.upper() == 'DESC':
            return False

        raise RuntimeError('Invalid string of asc or desc')


    @classmethod
    def extract_column_exprs(cls, expr:Union[Exprs, Iterable[Exprs]]) -> Iterator[ColumnExpr]:
        
        if not isinstance(expr, str) and isinstance(expr, Iterable):
            for _expr in expr:
                yield from cls.extract_column_exprs(_expr)
            return
        
        if isinstance(expr, ColumnExpr):
            yield expr
            return

        if isinstance(expr, OpExpr):
            yield from cls.extract_column_exprs(expr.larg)
            yield from cls.extract_column_exprs(expr.rarg)
            return

        if isinstance(expr, FuncExpr):
            for _expr in expr.args:
                yield from cls.extract_column_exprs(_expr)
            return

        raise TypeError('Unexcepted type `{}`.'.format(type(expr)))


    def all_exprs(self) -> Iterator[Exprs]:
        yield from self.column_exprs
        if self.where_expr is not None:
            yield self.where_expr 
        if self.group_exprs is not None:
            yield from self.group_exprs
        if self.having_expr is not None:
            yield self.having_expr
        if self.order_exprs is not None:
            yield from (order_expr[0] for order_expr in self.order_exprs)


    def tables_query(self) -> SQLQuery:

        all_exprs = list(self.all_exprs())
        all_column_exprs = list(self.extract_column_exprs(all_exprs))

        tables_column_connections:Dict[TableName, List[Tuple[Column, Column]]] = {}
        
        for column_expr in all_column_exprs:
            # print(repr(column_expr))

            column_cons = list(column_expr.column_connections())
            # print('column_cons:', column_cons)

            if not column_cons:
                continue
            
            c_table = column_cons[0][0].table 
            
            if c_table.name not in tables_column_connections:
                tables_column_connections[c_table.name] = []
            
            c_cons = tables_column_connections[c_table.name]
            for new_con in column_cons:
                if all((is_same(new_con[0], _con[0]) and is_same(new_con[1], _con[1])) for _con in c_cons):
                    # print('add', column_con)
                    c_cons.append(new_con)


        tables_joins:List[SQLQuery] = []

        for table_name, column_connections in tables_column_connections.items():

            c_query = SQLQuery(self.db.table(table_name))

            for column_from, column_to in column_connections:

                if isinstance(column_to, AliasedColumn):
                    c_table_query = SQLQuery(column_to.aliased_table, use_full=True)
                else:
                    c_table_query = SQLQuery(column_to.table)

                c_query += SQLQuery('INNER JOIN', c_table_query, 'ON', column_from, '=', column_to)
                
            tables_joins.append(c_query)

        return SQLQuery(tables_joins)
