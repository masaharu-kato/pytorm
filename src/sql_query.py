from typing import Any, Dict, Iterable, Iterator, List, Sequence, Optional, Tuple, Union, Type, cast
import sql_keywords
from dataclasses import dataclass
import datetime
import mysql.connector as mydb # type:ignore
import itertools
import collections
import toposort # type:ignore



QueryValTypes = [bool, int, float, str]
RawValTypes = Union[bool, int, float, str, datetime.datetime, datetime.date, datetime.time]
QueryVal = Union[bool, int, float, str]


# def is_iterable(target) -> bool:
#     return isinstance(target, Iterable) and not isinstance(target, str)

# # def _kw_with_dots(kw:str) -> str:
# #     return '.'.join(SQL.kw(w) for w in kw.split('.'))


class SQLQuery(str):
    """ SQL Query text object """

    def __init__(self, *exprs:ExprsStr, as_obj:bool=False, quoted:bool=False):
        self.q = ' '.join(sql_or_str(expr, as_obj=as_obj, quoted=quoted) for expr in exprs)

    def __add__(self, expr:ExprsStr) -> 'SQLQuery':
        return SQLQuery(self.q + ' ' + sql_or_str(expr))

    def __radd__(self, expr:ExprsStr) -> 'SQLQuery':
        return SQLQuery(sql_or_str(expr) + ' ' + self.q)

    def __iadd__(self, expr:ExprsStr):
        self.q += sql_or_str(expr)

    def __sql__(self) -> 'SQLQuery':
        return self
            
    def query(self) -> str:
        """ Get Query string """
        return self.q

# SQLQueryLike = Union[SQLQuery, Iterable[ExprsStr]]



# 
class Expr:
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
        return ExprIn(expr, self)

    def __sql__(self) -> SQLQuery:
        """ Convert to string for SQL """
        pass
    

ExprLike = Union[Expr, RawValTypes]
Exprs = Union[Expr, Sequence[Expr]]
ExprsStr = Optional[Union[Exprs, str]]



def sql(obj:Exprs) -> str:

    if isinstance(obj, Iterable):
        return ', '.join(map(sql, obj))
        
    if hasattr(obj, '__sql__'):
        return obj.__sql__().query()
        
    raise RuntimeError('Cannot convert value to SQL format.')


def sql_or_str(obj:ExprsStr, *, as_obj:bool=False, quoted:bool=False) -> str:
    if obj is None:
        return ''

    if isinstance(obj, int):
        if as_obj or quoted:  raise RuntimeError('Cannot specify `as_obj` or `quoted` for integer value.')
        return obj

    if isinstance(obj, str):
        raw = obj
    else:
        raw = sql(obj)

    if as_obj:
        if quoted: raise RuntimeError('Cannot specify both `as_obj` and `quoted`.')
        return '`' + raw.replace('`', '``') + '`'

    if quoted:
        return '"' + raw.translate(str.maketrans({'"': '\\"', '\\': '\\\\'})) + '"'

    return raw


# def sql_join(vals:Iterable[ExprsStr], delimiter:str = ', ') -> str:
#     return delimiter.join(map(sql, vals))





class Value(Expr):
    """ SQL value expression (int, float, string, datetime, ...) """

    def __init__(self, v:RawValTypes):
        self.v = v

    def __sql__(self) -> SQLQuery:
        if isinstance(self.v, int) or isinstance(self.v, float):
            return SQLQuery(str(self.v))

        if isinstance(self.v, datetime.datetime) or isinstance(self.v, datetime.date) or isinstance(self.v, datetime.time):
            return SQLQuery(self.v.isoformat(), quoted=True)

        return SQLQuery(self.v.replace('"', '\\"'), quoted=True)


class Values(Expr):
    """ SQL multiple values expression """

    def __init__(self, values:Iterable[Expr]):
        self.values = [to_expr(v) for v in values]

    def __iter__(self):
        return self.values

    def __sql__(self) -> SQLQuery:
        return SQLQuery('(', self.values, ')')


class OpExpr(Expr):
    """ Operator expression """

    def __init__(self, op:str, larg, rarg):
        self.op = op
        self.larg = to_expr(larg)
        self.rarg = to_expr(rarg)

    def __sql__(self) -> SQLQuery:
        if self.op not in sql_keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return SQLQuery('(', self.larg, self.op, self.rarg, ')')


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
    
    def __init__(self, name:str, *args):
        self.name = name
        self.args = [to_expr(arg) for arg in args]

    def __sql__(self) -> SQLQuery:
        if self.name not in sql_keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return SQLQuery(self.name, '(', self.args, ')')


class ExprIn(Expr):
    """ 'A IN B' expression """

    def __init__(self, target:Expr, values:Union[Values, Iterable[Expr]]):
        self.target = to_expr(target)
        self.values = values if isinstance(values, Values) else Values([to_expr(v) for v in values])

    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.target, 'IN', self.values)



def to_expr(v:Any) -> Expr:
    if isinstance(v, Expr):
        return v
    if isinstance(v, Iterable):
        return Values([to_expr(_v) for _v in v])
    return Value(v)


def _to_query_val(v:Any) -> QueryVal:
    if v is None or any(isinstance(v, t) for t in QueryValTypes):
        return v
    return str(v)



class SQLExecResult:
    """ SQL Execution Result Object """

    def fetch(self) -> tuple:
        return tuple()

    def fetch_all(self) -> List[tuple]:
        return []

    def __iter__(self):
        return iter([])


class SQLExecutor:
    """ SQL Executor Object (Database Cursor) """

    def __init__(self, *args, **options):
        # TODO: Implementation
        pass


    def exec(self, _q:SQLQuery, values:Optional[Iterable[QueryVal]] = None) -> SQLExecResult:
        q = to_sql_query(_q)
        print('Exec SQL:', q, list(values) if values else None)
        return SQLExecResult()

    def exec_many(self, _q:SQLQuery, vals_itr:Iterable[Iterable[QueryVal]]) -> SQLExecResult:
        q = to_sql_query(_q)
        print('Execmany SQL:', q, [list(vals) for vals in vals_itr])
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



# Reference of Table
@dataclass
class TableRef:
    db: 'Database'
    name: str

    def resolve(self) -> 'Table':
        try:
            return self.db.table(self.name)
        except KeyError:
            raise RuntimeError('Table reference resolution failed: `{}`'.format(self.name))

    def column_ref(self, name:str) -> 'ColumnRef':
        return ColumnRef(self, name)

    def __getitem__(self, name:Union[str, Iterable[str]]) -> Union['ColumnRef', Iterator['ColumnRef']]:
        if isinstance(name, Iterable) return (self[_name] for _name in name)
        return self.column_ref(name)
        

# Reference of Column
@dataclass
class ColumnRef:
    table: Union['Table', TableRef]
    name: str

    def resolve(self) -> 'Column':
        # Resolve table reference (if it is reference)
        if isinstance(self.table, TableRef):
            self.table = self.table.resolve()

        # Resolve column reference on table
        if self.table.column_exists(self.name):
            raise RuntimeError('Column reference resolution failed: `{}`'.format(self.name))

        return self.table.column(self.name)


class Column(Expr):
    
    def __init__(self, 
        name: str, # Column name
        basetype: str, # SQL Type
        *,
        nullable: bool = True, # nullable or not
        default: Optional[Expr] = None, # default value
        unique: bool = False, # unique key or not
        primary: bool = False, # primary key or not
        auto_increment: Optional[bool] = None, # auto increment or not
        links: Sequence[Union[Column, ColumnRef]] = [], # linked columns
    ):
        self.name = name
        self.basetype = basetype
        self.pytype = sql_keywords.types[basetype]
        self.nullable = nullable
        self.default_expr = default
        self.is_unique = unique
        self.is_primary = primary
        self.auto_increment = auto_increment

        self.link_columns:Dict[str, Union[Column, ColumnRef]] = {} # Table name of link column -> Link column
        for link_column in links:
            if link_column.table.name in self.link_columns:
                raise RuntimeError('Cannot link to multiple columns in the same table.')
            self.link_columns[link_column.table.name] = link_column


    def set_table(self, table:'Table') -> 'Column':
        self.table = table
        return self


    def __sql__(self) -> SQLQuery:
        return SQLQuery(sql(self.table) + '.' + SQLQuery(self.name, as_obj=True))

    def __repr__(self) -> str:
        return repr(self.table) + '.' + self.name

    def __hash__(self) -> int:
        return hash(repr(self))

    def link_tables(self) -> Iterator[Table]:
        return (column.table for column in self.link_columns.values())

    def link_table_names(self) -> Iterator[str]:
        return self.link_columns.keys()

    def link_to(self, table_name:str) -> 'LinkedTable':
        if not table_name in self.link_table_names():
            raise RuntimeError('Link to the specified table is not found.')

        return LinkedTable(self.link_columns[table_name].table, self.table)

    def __getitem__(self, table_name:Union[str, Iterable[str]]) -> Union['LinkedTable', Iterator['LinkedTable']]:
        if isinstance(table_name, Iterable):
            return (self[_table_name] for _table_name in table_name)
        return self.link_to(table_name)

    def resolve_reference(self) -> None:
        for i, link_column in enumerate(self.link_columns):
            if isinstance(link_column, ColumnRef):
                self.link_columns[i] = link_column.resolve()

    def creation_sql(self) -> SQLQuery:
        q = SQLQuery(self.name, self.basetype)
        if self.nullable: q += 'NOT NULL'
        if self.default_expr: q += SQLQuery('DEFAULT', self.default_expr)
        if self.is_unique: q += 'UNIQUE KEY'
        if self.is_primary: q += 'PRIMARY KEY'
        if self.auto_increment: q += 'AUTO_INCREMENT'
        return q
        




class Table(Expr):

    def __init__(self, db:'Database', name:str, columns:Iterable[Column], **options):
        self.db = db
        self.name = name
        self.columns = list(column.set_table(self) for column in columns)
        self.column_dict = {column.name: column for column in self.columns}
        # self.link_columns = [c for c in self.columns if c.links]
        # self.linked_tables = set(column.link.table for column in self.link_columns)
        # self.linkpaths:Optional[Dict[Table, List[Table]]] = None
        # self.linked_fk_cols:List[Column] = set()
        # self.linked_tables = set()
        self.created_on_db = None

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

    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.name, as_obj=True)

    def __repr__(self) -> str:
        return 'Table({})'.format(self.name)

    def __hash__(self) -> int:
        return hash(self.db.name + '.' + self.name)
        
    def __eq__(self, other) -> bool:
        return self.db == other.db and self.name == other.name


    def column(self, col_name:str) -> Column:
        return self.column_dict[col_name]

    def column_exists(self, col_name:str) -> bool:
        return col_name in self.column_dict

    # def column_ref(self, col_name:str) -> ColumnRef:
    #     return ColumnRef(self, col_name)

    # def __getitem__(self, col_name:str) -> Union[Column, ColumnRef]:
    #     if col_name in self.column_dict:
    #         return self.column_dict[col_name]
    #     return self.column_ref(col_name)

    def __getitem__(self, name_or_names:Union[str, Iterable[str]]) -> Union[Column, Iterator[Column]]:
        """ Get the column(s) by column name(s) """

        # Multiple specifications
        if isinstance(name_or_names, Iterable):
            names = name_or_names
            return (self[name] for name in names)

        # Single specifications
        name = name_or_names
        return self.column(name)


    # def __getattribute__(self, col_name:str) -> Column:
    #     return self.__getitem__(col_name)


    def resolve_references(self) -> None:
        for column in self.columns:
            column.resolve_reference()
        # self.link_tables = set(fcol.link_col.table for fcol in self.fk_cols)
        # print(self.link_tables)


    # def refresh_linkpaths(self) -> Dict[Table, dict]:

    #     if self.linkpaths is None:
    #         self.linkpaths = collections.defaultdict(lambda: [])

    #     for link_table in self.link_tables:
    #         if link_table.linkpaths is None: link_table.refresh_linkpaths()
    #         for dest_table, paths in link_table.linkpaths.items():
    #             self.linkpaths[dest_table].extend([[link_table, *path_tables] for path_tables in paths])



    def expr_str_to_column(self, col:Union[str, Expr]) -> Expr:
        return self.column(col) if isinstance(col, str) else col

    def to_column(self, col:Union[str, Column]) -> Column:
        return self.column(col) if isinstance(col, str) else col

    def creation_sql(self) -> SQLQuery:
        return SQLQuery('CREATE TABLE ', SQLQuery(self.name, as_obj=True), '(', [c.creation_sql() for c in self.columns], ')')

    def exists_in_db(self) -> bool:
        return len(self.db.exec(['SHOW TABLES LIKE', SQLQuery(self.name, as_obj=True)]).fetch_all()) > 0

    def create(self) -> SQLExecResult:
        """ Create table on the database """
        return self.db.exec(self.creation_sql())

    def create_if_not_exists(self) -> Optional[SQLExecResult]:
        if self.exists_in_db(): return None
        return self.create()

    def truncate(self) -> SQLExecResult:
        """ SQL Truncate table """
        return self.db.exec(['TRUNCATE TABLE', self])

    def drop(self) -> SQLExecResult:
        """ SQL Drop table """
        return self.db.exec(['DROP TABLE' + self])

    def prepare_select(self, columns: Optional[Sequence[Union[str, Expr]]] = None, *args, **kwargs) -> 'Select':
        if columns is None:
            return Select(self.db, self.columns, *args, **kwargs)
        return Select(self.db, [self.expr_str_to_column(column) for column in columns], *args, **kwargs)


    
    def select(self, columns: Optional[Sequence[Union[str, Expr]]] = None, *args, **kwargs) -> SQLExecResult:
        return self.prepare_select(columns, *args, **kwargs).exec()


    def insert(self,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> SQLExecResult:
        """ SQL INSERT query """

        return self.exec_many(SQLQuery('INSERT INTO', self, '(', columns, ')',
            'VALUES', '(', ['%s' for _ in range(len(columns))], ')'),
            ((_to_query_val(val) for val in vals) for vals in vals_itr),
        )

        
    def update(self,
        column_exprs: List[Tuple[Column, ExprLike]],
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        """ SQL UPDATE query """

        q = SQLQuery('UPDATE', self, 'SET', [SQLQuery(col, '=',  expr) for col, expr in column_exprs])
        if where: q += SQLQuery('WHERE', to_expr(where))
        if count: q += SQLQuery('LIMIT', count)
        
        return self.exec(q)


    def delete(self,
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        """ SQL DELETE query """

        q = SQLQuery('DELETE FROM', self)
        if where: q += SQLQuery('WHERE', to_expr(where))
        if count: q += SQLQuery('LIMIT', count)

        return self.exec(q)



    def select_key_with_insertion(self,
        columns : List[Column],
        records_itr: Iterable[Iterable[ExprLike]],
    ) -> Dict[tuple, int]:
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



class LinkedColumn(Expr):
    def __init__(self, linked_table:'LinkedTable', column:Column):
        self.linked_table = linked_table
        self.column = column


    def __getitem__(self, name:Union[str, Iterable[str]]) -> Union['LinkedTable', Iterator['LinkedTable']]:
        return self.column[name]


class LinkedTable(Expr):
    def __init__(self, table:Table, origin_table:Optional['LinkedTable']=None):
        self.table = table
        self.origin_table = origin_table


    def __getitem__(self, name:Union[str, Iterable[str]]) -> Union[LinkedColumn, Iterator[LinkedColumn]]:
        if isinstance(name, Iterable):
            return (self[_name] for _name in name)
        return LinkedColumn(self, self.table[name])




class Database(SQLExecutor):
    def __init__(self, name:str):
        self.name = name
        self.tables:List[Table] = []
        self.table_dict :Dict[str, Table] = {}
        self.column_dict:Dict[str, Column] = {}
        self.tables_priority:Dict[Table, int] = {}


    def __sql__(self) -> SQLQuery:
        return SQLQuery(self.name, as_obj=True)


    def __repr__(self) -> str:
        return 'DB({})'.format(self.name)

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other) -> bool:
        return self.name == other.name


    def table(self, name:str) -> Table:
        """ Get table object by table name """
        return self.table_dict[name]

    def table_exists(self, name:str) -> bool:
        """ Check if a table with the specified name exists """
        return name in self.table_dict

    def table_ref(self, name:str) -> TableRef:
        """ Get the temporary reference of table """
        return TableRef(self, name)

    def __getitem__(self, name_or_names:Union[str, Iterable[str]]) -> Union[Table, TableRef, Sequence[Table, TableRef]]:
        """ Get the table(s) by table name(s) """

        # Multiple specifications
        if isinstance(name_or_names, Iterable):
            names = name_or_names
            return tuple(self[name] for name in names)

        # Single specifications
        # Returns the table object if exists, else returns its reference
        name = name_or_names
        if name in self.table_dict:
            return self.table_dict[name]
        return self.table_ref(name)

    # def __getattribute__(self, name:str) -> Union[Table, TableRef]:
    #     return self.__getitem__(name)


    # Called by Table object
    def append_table(self, table:Table) -> None:
        self.tables.append(table)
        self.table_dict[table.name] = table
        for column in table.columns:
            self.column_dict[table.name + '.' + column.name] = column


    def prepare_table(self, name:str, columns:Iterable[Column], **options) -> Table:
        """ Prepare table for this database with table schema """
        return Table(self, name, columns, **options)


    # def create_table(self, name:str, columns_args:Iterable[ColArgs], **options)) -> Table:
    #     table = self.prepare_table(name, columns_args, **options)
    #     table.create()
    #     return table


    def resolve_references(self) -> None:
        """ Resolve references in tables """
        for table in self.tables:
            table.resolve_references()


    # def refresh_tables_priority(self) -> None:
    #     tables_graph = {table: table.link_tables for table in self.tables}
    #     print(tables_graph)
    #     tres = toposort.toposort(tables_graph)
    #     for i, tables in enumerate(tres, 1):
    #         for table in tables:
    #             self.tables_priority[table] = i


    # def sort_tables_by_priority(self, tables:List[Table]) -> List[Table]:
    #     return sorted(tables, key=lambda table: self.tables_priority[table])


    def tables_fk_columns(self, tables:List[Table]) -> Iterable[Tuple[Table, Iterable[Column]]]:
        sorted_tables = self.sort_tables_by_priority(tables)
        print('sorted_tables:', list(t.name for t in sorted_tables))
        for i, table in enumerate(sorted_tables):
            yield (table, (fk_col for fk_col in table.fk_cols if fk_col.link_col.table in sorted_tables[:i]))


    def tables_inner_join(self, tables:List[Table]) -> SQLQuery:
        q = SQLQuery()
        f_table_only = False
        for table, fk_cols_itr in self.tables_fk_columns(tables):
            fk_cols = list(fk_cols_itr)
            if fk_cols:
                q += SQLQuery('INNER JOIN', table, 'ON', ' & '.join('{} = {}'.format(sql(fk_col), sql(fk_col.link_col)) for fk_col in fk_cols))
                f_table_only = False
            else:   
                if f_table_only: q += ','
                q += table
                f_table_only = True
        return q


    @classmethod
    def find_columns_in_expr(cls, expr:Expr) -> Iterator[Column]:
        if isinstance(expr, Column):
            yield expr
        elif isinstance(expr, OpExpr):
            yield from cls.find_columns_in_expr(expr.larg)
            yield from cls.find_columns_in_expr(expr.rarg)
        elif isinstance(expr, FuncExpr):
            for carg in expr.args:
                yield from cls.find_columns_in_expr(carg)
        elif isinstance(expr, ExprIn):
            yield from cls.find_columns_in_expr(expr.target) 
        return


    @staticmethod
    def tables_of_columns(columns:Iterable[Column]) -> Iterator[Table]:
        return iter(set(col.table for col in columns))
        

    def tables_inner_join_by_exprs(self, exprs:Iterable[Expr]) -> SQLQuery:
        tables = list(self.tables_of_columns(itertools.chain.from_iterable(self.find_columns_in_expr(expr) for expr in exprs)))
        return self.tables_inner_join(tables)


    def finalize_tables(self) -> None:
        self.resolve_references()
        # self.refresh_tables_priority()
        # for table in self.sort_tables_by_priority(self.tables):
        #     table.create_if_not_exists()


    # def drop_table(self, name:str) -> None:
    #     table.drop()
    #     self.tables.remove(table)


    # def create_tables(self, table_args:Dict[str, Iterable[ColArgs]]) -> List[Table]:
    #     tables = [Table(self, name, cargs) for name, cargs in table_args.items()]
    #     self.tables.extend(tables)
    #     return tables



    def to_column(self, col:Union[str, Column]) -> Column:
        return self.column_dict[col] if isinstance(col, str) else col

    def to_table(self, table:Union[str, Table]) -> Table:
        return self.table(table) if isinstance(table, str) else table

    # def to_columns(self, columns:Sequence[Union[str, Column]]) -> List[Column]:
    #     return list(map(self.to_column, columns))

    # def to_tables(self, tables:Sequence[Union[str, Table]]) -> List[Table]:
    #     return list(map(self.to_table, tables))


    # def select(self,
    #     columns : Sequence[Union[Expr, str]],
    #     where   : Optional[ExprLike] = None,
    #     *,
    #     tables  : Optional[Sequence[Union[Expr, str]]] = None,
    #     group   : Optional[Sequence[Union[Expr, str]]] = None,
    #     having  : Optional[ExprLike] = None,
    #     order   : Optional[Sequence[Tuple[Union[Expr, str]], str]] = None,
    #     count   : Optional[int] = None,
    #     offset  : Optional[int] = None,
    # ) -> SQLExecResult:



    def insert(self,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> Tuple[str, Iterable[Iterable[QueryVal]]]:
        """ Insert records across tables """
        # TODO: Implementation
        pass




class Select:

    def __init__(self,
        db     : Database,
        columns: Sequence[ExprLike],
        *,
        where  : Optional[ExprLike] = None,
        group  : Optional[Iterable[Column]] = None,
        having : Optional[ExprLike] = None,
        order  : Optional[Iterable[Tuple[Column, str]]] = None,
        tables : Optional[Sequence[ExprLike]] = None,
        count  : Optional[int] = None,
        offset : Optional[int] = None,
    ):
        self.db = db
        self.column_exprs = columns
        self.where_expr = to_expr(where) if where else None
        self.group_exprs = group
        self.having_expr = to_expr(having) if having else None
        self.order_exprs = [(column, self._is_asc_or_desc(ad_str)) for column, ad_str in order] if order else None
        self.tables_expr = tables or self.db.tables_of_columns(self.column_exprs) 
        self.count = count
        self.offset = offset


    @staticmethod
    def _is_asc_or_desc(s:str) -> bool:
        if s == '+' or s.upper() == 'A' or s.upper() == 'ASC':
            return True
        if s == '-' or s.upper() == 'D' or s.upper() == 'DESC':
            return False

        raise RuntimeError('Invalid string of asc or desc')


    def sql_query(self) -> SQLQuery:
        q = SQLQuery('SELECT', self.column_exprs, 'FROM', self.tables_expr)
        if self.where_expr : q += SQLQuery('WHERE', self.where_expr)
        if self.group_exprs: q += SQLQuery('GROUP BY', self.group_exprs)
        if self.having_expr: q += SQLQuery('HAVING', self.having_expr)
        if self.order_exprs: q += SQLQuery('ORDER BY', [SQLQuery(column, ('ASC' if dstr else 'DESC')) for column, dstr in self.order_exprs])
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

