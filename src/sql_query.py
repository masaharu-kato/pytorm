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


def is_iterable(target) -> bool:
    return isinstance(target, Iterable) and not isinstance(target, str)

# def _kw_with_dots(kw:str) -> str:
#     return '.'.join(SQL.kw(w) for w in kw.split('.'))


# class SQL(str):

#     def __init__(self, val:Any, *, raw:bool=False):
#         if raw:
#             init_val = val
#         else:
#             if not hasattr(val, '__sql__'):
#                 raise RuntimeError('Cannot convert value to SQL.')
#             init_val = val.__sql__()

#         super().__init__(init_val)
        
#     def __sql__(self) -> 'SQL':
#         return self



def sql(val:Any) -> str:
    if not hasattr(val, '__sql__'):
        raise RuntimeError('Cannot convert value to SQL.')
    return val.__sql__()


def to_objname(name) -> str:
    return '`{}`'.format(name.replace('`', '``'))



# SQL expression base type
class Expr:
    def __add__(self, expr): return Operation('+', self, expr)
    def __sub__(self, expr): return Operation('-', self, expr)
    def __mul__(self, expr): return Operation('*', self, expr)
    def __mod__(self, expr): return Operation('%', self, expr)
    def __and__(self, expr): return Operation('&', self, expr)
    def __or__ (self, expr): return Operation('|', self, expr)
    def __radd__(self, expr): return Operation('+', expr, self)
    def __rsub__(self, expr): return Operation('-', expr, self)
    def __rmul__(self, expr): return Operation('*', expr, self)
    def __rmod__(self, expr): return Operation('%', expr, self)
    def __rand__(self, expr): return Operation('&', expr, self)
    def __ror__ (self, expr): return Operation('|', expr, self)

    def __lt__(self, expr): return Operation('<' , self, expr)
    def __le__(self, expr): return Operation('<=', self, expr)
    def __eq__(self, expr): return Operation('=' , self, expr)
    def __ne__(self, expr): return Operation('!=', self, expr)
    def __gt__(self, expr): return Operation('>' , self, expr)
    def __ge__(self, expr): return Operation('>=', self, expr)

    # 'A in B' expression
    def __contains__(self, expr):
        return ExprIn(expr, self)

    # Convert to string for SQL
    def __sql__(self) -> str: pass
    

ExprLike = Union[Expr, RawValTypes]

Exprs = Sequence[Union[Expr, Sequence[Expr]]]


# @dataclass
# class RawExpr(Expr):
#     text: str

#     def __sql__(self) -> str:
#         return self.text

#     def join(self, exprs:Iterable[Expr]) -> Iterator[Expr]:
#         is_first = True
#         for expr in exprs:
#             if not is_first:
#                 yield self
#             else:
#                 is_first = True
#             yield expr


# CommaExpr = RawExpr(',')

# def cmjoin(exprs:Iterable[Expr]) -> Iterator[Expr]:
#     return CommaExpr.join(exprs)


# SQL value expression (int, float, string, datetime, ...)
class Value(Expr):
    def __init__(self, v:RawValTypes):
        self.v = v

    def __sql__(self) -> str:
        if isinstance(self.v, int) or isinstance(self.v, float):
            return str(self.v)

        if isinstance(self.v, datetime.datetime) or isinstance(self.v, datetime.date) or isinstance(self.v, datetime.time):
            return '"{}"'.format(self.v.isoformat())

        return '"{}"'.format(self.v.replace('"', '\\"'))


class Values(Expr):
    def __init__(self, values:Iterable[Expr]):
        self.values = [to_expr(v) for v in values]

    def __iter__(self):
        return self.values

    def __sql__(self) -> str:
        return '({})'.format(', '.join(map(sql, self.values)))


class Operation(Expr):

    def __init__(self, op:str, larg, rarg):
        self.op = op
        self.larg = to_expr(larg)
        self.rarg = to_expr(rarg)

    def __sql__(self) -> str:
        if self.op not in sql_keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return '({} {} {})'.format(sql(self.larg), self.op, sql(self.rarg))


# class MultipleOperation(Expr):

#     def __init__(self, op:str, exprs):
#         self.op = op
#         self.exprs = to_expr(exprs)

#     def __sql__(self) -> SQL:
#         if self.op not in sql_keywords.operators:
#             raise RuntimeError('Unknown operator `{}`'.format(self.op))
#         return '({})'.format(' {} '.format(self.op).join(self.exprs)) 



class Function(Expr):
    
    def __init__(self, name:str, *args):
        self.name = name
        self.args = [to_expr(arg) for arg in args]

    def __sql__(self) -> str:
        if self.name not in sql_keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return '{}({})'.format(self.name, ', '.join(map(sql, self.args)))


class ExprIn(Expr):

    def __init__(self, target:Expr, values:Union[Values, Iterable[Expr]]):
        self.target = to_expr(target)
        self.values = values if isinstance(values, Values) else Values([to_expr(v) for v in values])

    def __sql__(self) -> str:
        return '{} IN {}'.format(self.target, self.values)



def to_expr(v:Any) -> Expr:
    if isinstance(v, Expr):
        return v
    if is_iterable(v):
        return Values([to_expr(_v) for _v in v])
    return Value(v)


def _to_query_val(v:Any) -> QueryVal:
    if v is None or any(isinstance(v, t) for t in QueryValTypes):
        return v
    return str(v)


class ColType:

    def __init__(self, basetype:str, *, nullable:bool=True, default:Optional[Expr]=None):
        self.basetype = basetype
        self.nullable = nullable
        self.default = default


    def pytype(self) -> Type:
        pybasetype = sql_keywords.types[self.basetype]
        if self.nullable:
            return cast(Type, Optional[pybasetype])
        return pybasetype


    def __sql__(self) -> str:
        return self.basetype


    def creation_sql(self) -> str:
        return self.basetype + (' NOT NULL' if not self.nullable else '')



ColIntType = ColType('INT', nullable=False)



@dataclass
class Column(Expr):
    table: 'Table'
    name: str
    _type: ColType
    # comment: Optional[str] = None

    def __sql__(self) -> str:
        return sql(self.table) + '.' + to_objname(self.name)

    def __repr__(self) -> str:
        return repr(self.table) + '.' + self.name

    def __hash__(self) -> int:
        return hash(repr(self))

    def resolve_reference(self) -> None:
        pass

    def creation_sql(self) -> str:
        return to_objname(self.name) + ' ' + self._type.creation_sql()



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


    def column_ref(self, col_name:str) -> 'ColumnRef':
        return ColumnRef(self, col_name)

    def __getitem__(self, col_name:str) -> 'ColumnRef':
        return self.column_ref(col_name)
        

# Reference of Column
@dataclass
class ColumnRef:
    table: Union['Table', TableRef]
    name: str

    def resolve(self) -> Column:
        # Resolve table reference (if it is reference)
        if isinstance(self.table, TableRef):
            self.table = self.table.resolve()

        # Resolve column reference on table
        if self.table.column_exists(self.name):
            raise RuntimeError('Column reference resolution failed: `{}`'.format(self.name))

        return self.table.column(self.name)


@dataclass
class ForeignKeyColumn(Column):
    link_col: Union[Column, ColumnRef]

    def resolve_reference(self) -> None:
        if isinstance(self.link_col, ColumnRef):
            self.link_col = self.link_col.resolve()


@dataclass
class PrimaryKeyColumn(Column):
    auto_increment: bool = False
    
    def creation_sql(self) -> str:
        return super().creation_sql() + ' PRIMARY KEY' + (' AUTO_INCREMENT' if self.auto_increment else '')


@dataclass
class ColArgs:
    name: str
    _type: ColType
    is_primary: bool = False
    auto_increment: bool = False
    link_col: Optional[Union[Column, ColumnRef]] = None

    def make_column(self, table:'Table') -> Column:
        if self.is_primary:
            if self.link_col: raise RuntimeError('Column link is not allowed on the primary key.')
            return PrimaryKeyColumn(table, self.name, self._type, self.auto_increment)
        
        if self.auto_increment:
            raise RuntimeError('Auto increment is not allowed on non-primary keys.')
        
        if self.link_col:
            return ForeignKeyColumn(table, self.name, self._type, self.link_col)

        return Column(table, self.name, self._type)



class SQLExecResult:

    def fetch(self) -> tuple:
        return tuple()

    def fetch_all(self) -> List[tuple]:
        return []

    def __iter__(self):
        return iter([])


class SQLExecutor:
    def __init__(self, *args):
        # TODO: Implementation
        pass

    def exec(self, sql:str, values:Optional[Iterable[QueryVal]] = None) -> SQLExecResult:
        print('Exec SQL:', sql, list(values) if values else None)
        return SQLExecResult()

    def exec_many(self, sql:str, vals_itr:Iterable[Iterable[QueryVal]]) -> SQLExecResult:
        print('Execmany SQL:', sql, [list(vals) for vals in vals_itr])
        return SQLExecResult()



class Table(Expr):

    def __init__(self, db:'Database', name:str, columns_args:Iterable[ColArgs], **options):
        self.db = db
        self.name = name
        self.columns = list(col_args.make_column(self) for col_args in columns_args)
        self.column_dict = {col.name: col for col in self.columns}
        self.fk_cols:List[ForeignKeyColumn] = [c for c in self.columns if isinstance(c, ForeignKeyColumn)]
        self.link_tables = set(fcol.link_col.table for fcol in self.fk_cols)
        self.linkpaths:Optional[Dict[Table, List[Table]]] = None
        # self.linked_fk_cols:List[Column] = set()
        # self.linked_tables = set()
        self.created_on_db = None

        self.db.append_table(self)



        pkey_cols = list(filter(lambda c:isinstance(c, PrimaryKeyColumn), self.columns))
        if len(pkey_cols) >= 2:
            raise RuntimeError('Multiple primary key is not allowed.')
        self.pkey_col = pkey_cols[0] if pkey_cols else None

        # for fkey_col in self.fk_cols:
        #     fkey_col.link_col.table.linked_fk_cols.add(fkey_col)
        #     fkey_col.link_col.table.linked_tables.add(fkey_col.table)

    def __sql__(self) -> str:
        return to_objname(self.name)

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

    def __getitem__(self, col_name:str) -> Column:
        return self.column(col_name)

    # def __getattribute__(self, col_name:str) -> Column:
    #     return self.__getitem__(col_name)


    def resolve_references(self) -> None:
        for column in self.columns:
            column.resolve_reference()
        self.link_tables = set(fcol.link_col.table for fcol in self.fk_cols)
        print(self.link_tables)


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

    def creation_sql(self) -> str:
        return 'CREATE TABLE ' + to_objname(self.name) \
            + ' (' + ', '.join(column.creation_sql() for column in self.columns) + ')'

    def exists_in_db(self) -> bool:
        return len(self.db.exec('SHOW TABLES LIKE ' + to_objname(self.name)).fetch_all()) > 0

    def create(self) -> SQLExecResult:
        return self.db.exec(self.creation_sql())

    def create_if_not_exists(self) -> Optional[SQLExecResult]:
        if self.exists_in_db(): return None
        return self.create()

    def truncate(self) -> SQLExecResult:
        return self.db.exec('TRUNCATE TABLE ' + sql(self))

    def drop(self) -> SQLExecResult:
        return self.db.exec('DROP TABLE ' + sql(self))

    def select(self, _columns: Optional[Sequence[Union[str, Expr]]] = None, *args, **kwargs) -> 'Select':
        if _columns is None:
            return Select(self.db, self.columns, *args, **kwargs)
        return Select(self.db, [self.expr_str_to_column(_c) for _c in _columns], *args, **kwargs)

    def insert(self, columns:Sequence[Union[str, Column]], vals_itr:Iterable[Iterable[ExprLike]]) -> SQLExecResult:
        return self.db.insert(self, [self.to_column(c) for c in columns], vals_itr)

    def update(self, _column_exprs: List[Tuple[Union[str, Column], ExprLike]], where: Optional[ExprLike]) -> SQLExecResult:
        column_exprs = [(self.to_column(col), val) for col, val in _column_exprs]
        return self.db.update(self, column_exprs, where)
        
    def delete(self, where: Optional[ExprLike], count: Optional[int] = None) -> SQLExecResult:
        return self.db.delete(self, where, count)

    def select_key_with_insertion(self,
        columns : List[Column],
        records_itr: Iterable[Iterable[ExprLike]],
    ) -> Dict[tuple, int]:
        if self.pkey_col is None: raise RuntimeError('No primary key found in this table.')

        records = list(records_itr)

        # Get existing records
        vals_to_key = {vals:key for key, *vals in self.select(
            [self.pkey_col, *columns],
            where=(columns in to_expr(records)))}
        
        # Insert new records and get their keys
        new_records = filter(lambda rec: rec not in vals_to_key, records_itr)
        self.insert(columns, new_records)

        return {**vals_to_key, **{vals:key for key, *vals in self.select([self.pkey_col, *columns], where=(columns in to_expr(new_records)))}}



class Database(SQLExecutor):
    def __init__(self, name:str):
        self.name = name
        self.tables:List[Table] = []
        self.table_dict :Dict[str, Table] = {}
        self.column_dict:Dict[str, Column] = {}
        self.tables_priority:Dict[Table, int] = {}


    def __sql__(self) -> str:
        return to_objname(self.name)


    def __repr__(self) -> str:
        return 'DB({})'.format(self.name)

    def __hash__(self) -> int:
        return hash(repr(self))

    def __eq__(self, other) -> bool:
        return self.name == other.name


    def table(self, name:str) -> Table:
        return self.table_dict[name]

    def table_exists(self, name:str) -> bool:
        return name in self.table_dict

    def table_ref(self, name:str) -> TableRef:
        return TableRef(self, name)

    def __getitem__(self, name:str) -> Union[Table, TableRef]:
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


    def prepare_table(self, name:str, columns_args:Iterable[ColArgs], **options) -> Table:
        return Table(self, name, columns_args, **options)


    # def create_table(self, name:str, columns_args:Iterable[ColArgs], **options)) -> Table:
    #     table = self.prepare_table(name, columns_args, **options)
    #     table.create()
    #     return table


    def resolve_references(self) -> None:
        for table in self.tables:
            table.resolve_references()


    def refresh_tables_priority(self) -> None:
        tables_graph = {table: table.link_tables for table in self.tables}
        print(tables_graph)
        tres = toposort.toposort(tables_graph)
        for i, tables in enumerate(tres, 1):
            for table in tables:
                self.tables_priority[table] = i


    def sort_tables_by_priority(self, tables:List[Table]) -> List[Table]:
        return sorted(tables, key=lambda table: self.tables_priority[table])


    def tables_fk_columns(self, tables:List[Table]) -> Iterable[Tuple[Table, Iterable[ForeignKeyColumn]]]:
        sorted_tables = self.sort_tables_by_priority(tables)
        print('sorted_tables:', list(t.name for t in sorted_tables))
        for i, table in enumerate(sorted_tables):
            yield (table, (fk_col for fk_col in table.fk_cols if fk_col.link_col.table in sorted_tables[:i]))


    def tables_inner_join(self, tables:List[Table]) -> str:
        res = ''
        f_table_only = False
        for table, fk_cols_itr in self.tables_fk_columns(tables):
            fk_cols = list(fk_cols_itr)
            if fk_cols:
                res += 'INNER JOIN ' + sql(table) + ' ON ' + ' & '.join('{} = {}'.format(sql(fk_col), sql(fk_col.link_col)) for fk_col in fk_cols)
                f_table_only = False
            else:   
                if f_table_only: res += ','
                res += ' ' + sql(table)
                f_table_only = True
        return res


    @classmethod
    def find_columns_in_expr(cls, expr:Expr) -> Iterator[Column]:
        if isinstance(expr, Column):
            yield expr
        elif isinstance(expr, Operation):
            yield from cls.find_columns_in_expr(expr.larg)
            yield from cls.find_columns_in_expr(expr.rarg)
        elif isinstance(expr, Function):
            for carg in expr.args:
                yield from cls.find_columns_in_expr(carg)
        elif isinstance(expr, ExprIn):
            yield from cls.find_columns_in_expr(expr.target) 
        return


    @staticmethod
    def tables_of_columns(columns:Iterable[Column]) -> Iterator[Table]:
        return iter(set(col.table for col in columns))
        

    def tables_inner_join_by_exprs(self, exprs:Iterable[Expr]) -> str:
        tables = list(self.tables_of_columns(itertools.chain.from_iterable(self.find_columns_in_expr(expr) for expr in exprs)))
        return self.tables_inner_join(tables)


    def finalize_tables(self) -> None:
        self.resolve_references()
        self.refresh_tables_priority()
        for table in self.sort_tables_by_priority(self.tables):
            table.create_if_not_exists()


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
        table: Table,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> SQLExecResult:
        return self.exec_many(
            'INSERT INTO ' + sql(table) + '(' + ', '.join(map(sql, columns)) + ')' \
                + 'VALUES' + '(' + ', '.join('%s' for _ in range(len(columns))) + ')',
            ((_to_query_val(val) for val in vals) for vals in vals_itr),
        )

        


    def insert_on_tables(self,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> Tuple[str, Iterable[Iterable[QueryVal]]]:
        # TODO:
        pass



    def update(self,
        table: Table,
        column_exprs: List[Tuple[Column, ExprLike]],
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        q = 'UPDATE ' + sql(table) + ' SET ' + ', '.join('{} = {}'.format(sql(col), sql(expr)) for col, expr in column_exprs)
        if where: q += ' WHERE ' + to_expr(where)
        if count: q += ' LIMIT ' + sql(count)
        return self.exec(q)


    def delete(self,
        table: Table,
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        q = 'DELETE FROM ' + sql(table)
        if where: q += ' WHERE ' + to_expr(where)
        if count: q += ' LIMIT ' + sql(count)
        return self.exec(q)




@dataclass
class Select:
    db         : Database
    tables     : Sequence[ExprLike]
    columns    : Sequence[ExprLike]
    where      : Optional[ExprLike] = None
    group      : Optional[Iterable[Column]] = None
    having     : Optional[ExprLike] = None
    order      : Optional[Iterable[Union[Column, Tuple[Column, str]]]] = None
    count      : Optional[int] = None
    offset     : Optional[int] = None

    unit_count : Optional[int] = None
    result : Optional[Iterable] = None


    def sql_query(self) -> str:
        q = 'SELECT ' + ', '.join(map(sql, map(self.db.to_column, self.columns))) + ' FROM ' + ', '.join(map(sql, map(self.db.to_table, self.tables)))
        if self.where : q += ' WHERE ' + to_expr(self.where)
        if self.group : q += ' GROUP BY ' + ', '.join(map(sql, map(self.db.to_column, self.group)))
        if self.having: q += ' HAVING ' + to_expr(self.having)
        if self.order : q += ' ORDER BY ' + ', '.join((sql(self.db.to_column(column)) + ' ' + dstr) for column, dstr in self.order)
        if self.count : q += ' LIMIT ' + sql(self.count)
        if self.offset: q += ' OFFSET ' + sql(self.offset)
        return q


    def refresh(self):
        self.result = self.db.select(self.columns, self.where, self.group, self.having, self.order, self.count, self.offset)
            
    def __iter__(self):
        return self.result


    def next_block(self):
        self.offset += self.count

    def prev_block(self):
        self.offset -= self.count

