from typing import Any, Dict, Iterable, Iterator, List, Sequence, Optional, Tuple, Union, Type
import sql_keywords
from dataclasses import dataclass
import datetime
import mysql.connector as mydb # type:ignore
import itertools 
import toposort # type:ignore



QueryValTypes = [bool, int, float, str]
RawValTypes = Union[bool, int, float, str, datetime.datetime, datetime.date, datetime.time]
QueryVal = Union[bool, int, float, str]


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




def is_iterable(target) -> bool:
    return isinstance(target, Iterable) and not isinstance(target, str)


def _rw(w:str) -> str:
    return str(w).strip('`').replace('`', '') # ignore '`' chars in word

def _kw(kw:str) -> str:
    return '`{}`'.format(_rw(kw))

def _kw_with_dots(kw:str) -> str:
    return '.'.join(_kw(w) for w in kw.split('.'))


class Expression:
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


    def __str__(self): pass
    def __contains__(self, _): pass


ExprLike = Union[Expression, RawValTypes]


class Value(Expression):
    def __init__(self, v:RawValTypes):
        self.v = v

    def __str__(self) -> str:
        if isinstance(self.v, int):
            return str(self.v)

        return '"{}"'.format(str(self.v).replace('"', '\\"'))


class Values(Expression):
    def __init__(self, values:Iterable[Expression]):
        self.values = [_to_expr(v) for v in values]

    def __iter__(self):
        return self.values

    def __contains__(self, expr:Expression):
        return ExprIn(expr, self)

    def __str__(self) -> str:
        return '({})'.format(', '.join(map(str, self.values)))


class Operation(Expression):

    def __init__(self, op:str, larg, rarg):
        self.op = op
        self.larg = _to_expr(larg)
        self.rarg = _to_expr(rarg)

    def __str__(self) -> str:
        if self.op not in sql_keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return '({} {} {})'.format(self.larg, self.op, self.rarg) 


# class MultipleOperation(Expression):

#     def __init__(self, op:str, exprs):
#         self.op = op
#         self.exprs = _to_expr(exprs)

#     def __str__(self) -> str:
#         if self.op not in sql_keywords.operators:
#             raise RuntimeError('Unknown operator `{}`'.format(self.op))
#         return '({})'.format(' {} '.format(self.op).join(self.exprs)) 



class Function(Expression):
    
    def __init__(self, name:str, *args):
        self.name = name
        self.args = [_to_expr(arg) for arg in args]

    def __str__(self) -> str:
        if self.name not in sql_keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return '{}({})'.format(self.name, ', '.join(map(str, self.args))) 


class ExprIn(Expression):

    def __init__(self, target:Expression, values:Union[Values, Iterable[Expression]]):
        self.target = _to_expr(target)
        self.values = values if isinstance(values, Values) else Values([_to_expr(v) for v in values])

    def __str__(self) -> str:
        return '{} IN {}'.format(self.target, self.values)



def _to_expr(v:Any) -> Expression:
    if isinstance(v, Expression):
        return v
    if is_iterable(v):
        return Values([_to_expr(_v) for _v in v])
    return Value(v)


def _to_query_val(v:Any) -> QueryVal:
    if v is None or any(isinstance(v, t) for t in QueryValTypes):
        return v
    return str(v)


@dataclass
class ColType:
    dbtype: str
    pytype: Type

    def __str__(self) -> str:
        return self.dbtype


ColIntType = ColType('INT NOT NULL', int)


@dataclass
class Column(Expression):
    table: 'Table'
    name: str
    _type: ColType

    def __str__(self) -> str:
        return str(self.table) + '.' + _kw(self.name)

    def __repr__(self) -> str:
        return 'Column({})'.format(self.name)

    def resolve_reference(self) -> None:
        pass

        

# Reference of Table
class TableRef:
    def __init__(self, db:'Database', name:str):
        self.db = db
        self.name = name

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
class ColumnRef:

    def __init__(self, table:Union['Table', TableRef], name:str):
        self.table = table
        self.name = name

    def resolve(self) -> Column:
        if isinstance(self.table, TableRef):
            self.table = self.table.resolve()
        try:
            return self.table.column(self.name)
        except KeyError:
            raise RuntimeError('Column reference resolution failed: `{}`'.format(self.name))



@dataclass
class ForeignKeyColumn(Column):
    link_col: Union[Column, ColumnRef]

    def resolve_reference(self) -> None:
        if isinstance(self.link_col, ColumnRef):
            self.link_col = self.link_col.resolve()


@dataclass
class PrimaryKeyColumn(Column):
    auto_increment: bool = False


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



class Table(Expression):

    def __init__(self, db:'Database', name:str, columns_args:Iterable[ColArgs], **options):
        self.db = db
        self.name = name
        self.columns = list(col_args.make_column(self) for col_args in columns_args)
        self.column_dict = {col.name: col for col in self.columns}
        self.fk_cols:List[ForeignKeyColumn] = [c for c in self.columns if isinstance(c, ForeignKeyColumn)]
        self.link_tables = set(fcol.link_col.table for fcol in self.fk_cols)
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


    def __str__(self) -> str:
        return _kw(self.name)


    def __repr__(self) -> str:
        return 'Table({})'.format(self.name)


    def __hash__(self) -> int:
        return hash(str(self))


    def __eq__(self, other) -> bool:
        return self.db == other.db and self.name == other.name



    def column(self, col_name:str) -> Column:
        return self.column_dict[col_name]

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

    def expr_str_to_column(self, col:Union[str, Expression]) -> Expression:
        return self.column(col) if isinstance(col, str) else col

    def to_column(self, col:Union[str, Column]) -> Column:
        return self.column(col) if isinstance(col, str) else col

    def create(self) -> SQLExecResult:
        return self.db.exec('CREATE TABLE {}({});'.format(
            str(self),
            ', '.join(
                '{} {}{}{}'.format(
                    _kw(col.name),
                    col._type,
                    ' PRIMARY KEY' if isinstance(col, PrimaryKeyColumn) else '',
                    ' AUTO_INCREMENT' if isinstance(col, PrimaryKeyColumn) and col.auto_increment else '',
                )
                for col in self.columns
            ),
        ))

    def exists_in_db(self) -> bool:
        return len(self.db.exec('SHOW TABLES LIKE {};'.format(Value(self.name))).fetch_all()) > 0

    def create_if_not_exists(self) -> Optional[SQLExecResult]:
        if self.exists_in_db(): return None
        return self.create()

    def truncate(self) -> SQLExecResult:
        return self.db.exec('TRUNCATE TABLE {};'.format(self))

    def drop(self) -> SQLExecResult:
        return self.db.exec('DROP TABLE {};'.format(self))

    def select(self, _columns: Optional[Sequence[Union[str, Expression]]] = None, *args, **kwargs) -> 'Select':
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
            where=(columns in _to_expr(records)))}
        
        # Insert new records and get their keys
        new_records = filter(lambda rec: rec not in vals_to_key, records_itr)
        self.insert(columns, new_records)

        return {**vals_to_key, **{vals:key for key, *vals in self.select([self.pkey_col, *columns], where=(columns in _to_expr(new_records)))}}


class Database(SQLExecutor):
    def __init__(self, name:str, tables:List[Table] = []):
        self.name = name
        self.tables = tables
        self.table_dict = {table.name: table for table in self.tables}
        self.tables_priority:Dict[str, int] = {}


    def __str__(self) -> str:
        return _kw(self.name)


    def __repr__(self) -> str:
        return 'Database({})'.format(self.name)


    def __hash__(self) -> int:
        return hash(str(self))


    def __eq__(self, other) -> bool:
        return self.name == other.name


    def table(self, name:str) -> Table:
        return self.table_dict[name]

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
        tables_graph = {table.name: {t.name for t in table.link_tables} for table in self.tables}
        print(tables_graph)
        tres = toposort.toposort(tables_graph)
        for i, table_names in enumerate(tres, 1):
            for table_name in table_names:
                self.tables_priority[table_name] = i


    def sort_tables_by_priority(self, tables:List[Table]) -> List[Table]:
        return sorted(tables, key=lambda table: self.tables_priority[table.name])


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
                res += ' INNER JOIN {} ON {}'.format(table, ' & '.join('{} = {}'.format(fk_col, fk_col.link_col) for fk_col in fk_cols))
                f_table_only = False
            else:   
                if f_table_only: res += ', '
                res += str(table)
                f_table_only = True
        return res


    @classmethod
    def find_columns_in_expr(cls, expr:Expression) -> Iterator[Column]:
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
        

    def tables_inner_join_by_exprs(self, exprs:Iterable[Expression]) -> str:
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


    def select(self,
        # table_expr : ExprLike,
        columns: Sequence[Union[Expression, str]],
        where  : Optional[ExprLike] = None,
        group  : Optional[Iterable[Column]] = None,
        having : Optional[ExprLike] = None,
        order  : Optional[Iterable[Union[Column, Tuple[Column, str]]]] = None,
        count  : Optional[int] = None,
        offset : Optional[int] = None,
    ) -> SQLExecResult:

        # TODO: Convert string `tablename.columnname` to Column object
        colexprs = list(map(_to_expr, columns))
        res = 'SELECT {} FROM {}'.format(
            ', '.join(map(str, colexprs)),
            self.tables_inner_join_by_exprs(colexprs)
        )
        if where : res += ' WHERE {}'.format(_to_expr(where))
        if group : res += ' GROUP BY {}'.format(', '.join(map(str, columns)))
        if having: res += ' HAVING {}'.format(_to_expr(having))
        if order: 
            res += ' ORDER BY {}'.format(', '.join(
                '{} {}'.format(col_da[0], _rw(col_da[1])) if isinstance(col_da, tuple) else str(col_da) for col_da in order
            ))
        if count : res += ' LIMIT {}'.format(int(count))
        if offset: res += ' OFFSET {}'.format(int(offset))
        res += ';'
        return self.exec(res)


    def insert(self,
        table: Table,
        columns: List[Column],
        vals_itr:Iterable[Iterable[ExprLike]],
    ) -> SQLExecResult:
        return self.exec_many(
            'INSERT INTO {}({}) VALUES({});'.format(
                table,
                ', '.join(map(str, columns)),
                ', '.join('%s' for _ in range(len(columns)))
            ),
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
        res = 'UPDATE {} SET {}'.format(
            table,
            ', '.join('{} = {}'.format(col, _to_expr(expr)) for col, expr in column_exprs)
        )
        if where: res += ' WHERE {}'.format(_to_expr(where))
        if count: res += ' LIMIT {}'.format(int(count))
        res += ';'
        return self.exec(res)


    def delete(self,
        table: Table,
        where: Optional[ExprLike],
        count: Optional[int] = None,
    ) -> SQLExecResult:
        res = 'DELETE FROM {};'.format(table)
        if where: res += ' WHERE {}'.format(_to_expr(where))
        if count: res += ' LIMIT {}'.format(int(count))
        return self.exec(res)

        




@dataclass
class Select:
    db         : Database
    # table_expr : ExprLike
    columns    : Sequence[ExprLike]
    where      : Optional[ExprLike] = None
    group      : Optional[Iterable[Column]] = None
    having     : Optional[ExprLike] = None
    order      : Optional[Iterable[Union[Column, Tuple[Column, str]]]] = None
    count      : Optional[int] = None
    offset     : Optional[int] = None

    unit_count : Optional[int] = None
    result : Optional[Iterable] = None


    def refresh(self):
        self.result = self.db.select(self.columns, self.where, self.group, self.having, self.order, self.count, self.offset)
            
    def __iter__(self):
        return self.result


    def next_block(self):
        self.offset += self.count

    def prev_block(self):
        self.offset -= self.count

