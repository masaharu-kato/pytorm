from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union, Type
import sql_keywords
from dataclasses import dataclass
import mysql.connector as mydb
import datetime


ExecResult = Iterable

class SQLExecutor:
    def __init__(self, *args):
        # TODO: Implementation
        pass

    def _exec(self, sql:str, values:Optional[Iterable[QueryValTypes]] = None) -> ExecResult:
        print(sql, list(values))
        return iter([])

    def _exec_many(self, sql:str, vals_itr:Iterable[Iterable[QueryValTypes]]) -> ExecResult:
        print(sql, [list(vals) for vals in vals_itr])
        return iter([])




def is_iterable(target) -> bool:
    return isinstance(target, Iterable) and not isinstance(target, str)


def _rw(w:str) -> str:
    return str(w).strip('`').replace('`', '') # ignore '`' chars in word

def _kw(kw:str) -> str:
    return '.'.join('`{}`'.format(_rw(kw)))

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



RawValTypes = Union[None, bool, int, float, str, datetime.datetime, datetime.date, datetime.time]
ExprLike = Union[Expression, RawValTypes]
QueryValTypes = Union[None, bool, int, float, str]


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

    def __contains__(self, expr:Expression):
        return ExprIn(expr, self)

    def __str__(self) -> str:
        return '({})'.format(', '.join(self.values))


class Operation(Expression):

    def __init__(self, op:str, larg, rarg):
        self.op = op
        self.larg = _to_expr(larg)
        self.rarg = _to_expr(rarg)

    def __str__(self) -> str:
        if self.op not in sql_keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return '({} {} {})'.format(self.larg, self.op, self.rarg) 


class Function(Expression):
    
    def __init__(self, name:str, *args):
        self.name = name
        self.args = [_to_expr(arg) for arg in args]

    def __str__(self) -> str:
        if self.name not in sql_keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return '{}({})'.format(self.name, ', '.join(self.args)) 


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


def _to_query_val(v:Any) -> QueryValTypes:
    if isinstance(v, QueryValTypes):
        return v
    return str(v)


@dataclass
class ColumnType:
    dbtype: str
    pytype: Type

    def __str__(self) -> str:
        return self.dbtype


@dataclass
class Column(Expression):
    table: 'Table'
    name: str
    _type: ColumnType

    def __str__(self) -> str:
        return str(self.table) + '.' + _kw(self.name)

    def q_with_type(self) -> str:
        return '{} {}'.format(self, self._type)


@dataclass
class ForeignKeyColumn(Column):
    link_col: Column


@dataclass
class PrimaryKeyColumn(Column):
    auto_increment: bool = False


@dataclass
class ColArgs:
    name: str
    _type: Optional[ColumnType] = None
    is_primary: bool = False
    auto_increment: bool = False
    link_col: Optional[Column] = None

    def make_column(self, table:'Table') -> Column:
        if self.is_primary:
            if self.link_col: raise RuntimeError('Column link is not allowed on the primary key.')
            return PrimaryKeyColumn(table, self.name, self._type, self.auto_increment)
        
        if self.auto_increment:
            raise RuntimeError('Auto increment is not allowed on non-primary keys.')
        
        if self.link_col:
            return ForeignKeyColumn(table, self.name, self._type, self.link_col)

        return Column(table, self.name, self._type)



class Table(Expression, SQLExecutor):

    def __init__(self, name:str, columns_args:Iterable[ColArgs]):
        self.name = name
        self.columns = list(col_args.make_column(self) for col_args in columns_args)
        self.column_dict = {col.name: col for col in self.columns}
        self.fkey_cols:List[ForeignKeyColumn] = list(filter(lambda c:isinstance(c, ForeignKeyColumn), self.columns))
        self.link_tables = set(fcol.table for fcol in self.fkey_cols)
        self.linked_fkey_cols:List[Column] = set()
        self.linked_tables = set()

        pkey_cols = list(filter(lambda c:isinstance(c, PrimaryKeyColumn), self.columns))
        if len(pkey_cols) >= 2:
            raise RuntimeError('Multiple primary key is not allowed.')
        self.pkey_col = pkey_cols[0] if pkey_cols else None

        for fkey_col in self.fkey_cols:
            fkey_col.link_col.table.linked_fkey_cols.add(fkey_col)
            fkey_col.link_col.table.linked_tables.add(fkey_col.table)




    def __str__(self) -> str:
        return _kw(self.name)

    def column(self, colname:str) -> Column:
        return self.column_dict[colname]

    def _to_column(self, col:Union[str, Column]) -> Column:
        return self.column(col) if isinstance(col, str) else col

    def create(self) -> ExecResult:
        return self._exec('CREATE TABLE {} ({});'.format(
            str(self),
            ', '.join(col.q_with_type() for col in self.columns),
        ))

    def drop(self) -> ExecResult:
        return self._exec('DROP TABLE {};'.format(str(self)))

    def select(self, _columns: Optional[List[Union[str, ExprLike]]] = None, *args, **kwargs) -> 'Select':
        columns = map(self._to_column, _columns) if _columns else self.columns
        return Select(columns, *args, **kwargs)

    def insert(self, columns:List[Union[str, Column]], vals_itr:Iterable[Iterable[ExprLike]]) -> ExecResult:
        return self._exec(*q_insert(self, map(self._to_column, columns), vals_itr))

    def update(self, _column_exprs: List[Tuple[Union[str, Column], ExprLike]], where: Optional[ExprLike]) -> ExecResult:
        column_exprs = ((self._to_column(col), val) for col, val in _column_exprs)
        return self._exec(q_update(self, column_exprs, where))
        
    def delete(self, where: Optional[ExprLike], count: Optional[int] = None) -> ExecResult:
        return self._exec(q_delete(self, where, count))

    def last_insert_id(self) -> int:
        # TODO:
        pass

    def select_key_with_insertion(self,
        in_columns : List[Column],
        records_itr: Iterable[Iterable[ExprLike]],
    ) -> Dict[tuple, int]:
        records = list(records_itr)

        # Get existing records
        values_to_key = {}
        for key, *values in self.select([self.pkey_col, *in_columns], where=(in_columns in _to_expr(records))):
            values_to_key[values] = key

        # Insert new records and get their keys
        new_records = filter(lambda rec: rec not in values_to_key, records_itr)
        self.insert(in_columns, new_records)
        for key, *values in self.select([self.pkey_col, *in_columns], where=(in_columns in _to_expr(new_records))):
            values_to_key[values] = key
        
        return values_to_key


class Database(SQLExecutor):
    def __init__(self, tables:List[Table]):
        self.tables = tables

    




@dataclass
class Select(SQLExecutor):
    # table_expr : ExprLike
    columns    : List[ExprLike]
    where      : Optional[ExprLike] = None
    group      : Optional[Iterable[Column]] = None
    having     : Optional[ExprLike] = None
    order      : Optional[Iterable[Union[Column, Tuple[Column, str]]]] = None
    count      : Optional[int] = None
    offset     : Optional[int] = None

    unit_count : Optional[int] = None
    result : Optional[Iterable] = None


    def _q_sql(self) -> str:
        return q_select(self.columns, self.where, self.group, self.having, self.order, self.count, self.offset)

    def refresh(self):
        self.result = self._exec(self._q_sql())
            
    def __iter__(self):
        return self.result


    def next_block(self):
        self.offset += self.count

    def prev_block(self):
        self.offset -= self.count



def q_select(
    # table_expr : ExprLike,
    columns: List[ExprLike],
    where  : Optional[ExprLike] = None,
    group  : Optional[Iterable[Column]] = None,
    having : Optional[ExprLike] = None,
    order  : Optional[Iterable[Union[Column, Tuple[Column, str]]]] = None,
    count  : Optional[int] = None,
    offset : Optional[int] = None,
) -> str:
    res = 'SELECT {} FROM {}'.format(
        ', '.join(columns),
        _table_expr_by_columns(columns)
    )
    if where : res += ' WHERE {}'.format(_to_expr(where))
    if group : res += ' GROUP BY {}'.format(', '.join(columns))
    if having: res += ' HAVING {}'.format(_to_expr(having))
    if order: 
        res += ' ORDER BY {}'.format(', '.join(
            '{} {}'.format(col_da[0], _rw(col_da[1])) if isinstance(col_da, tuple) else col_da for col_da in order
        ))
    if count : res += ' LIMIT {}'.format(int(count))
    if offset: res += ' OFFSET {}'.format(int(offset))
    return res


def q_insert(
    table: Table,
    columns: List[Column],
    vals_itr:Iterable[Iterable[ExprLike]],
) -> Tuple[str, Iterable[Iterable[QueryValTypes]]]:
    return (
        'INSERT INTO {}({}) VALUES({})'.format(
            table,
            ', '.join(columns),
            ', '.join('%s' for _ in range(len(columns)))
        ),
        ((_to_query_val(val) for val in vals) for vals in vals_itr),
    )

    


def q_insert_on_tables(
    columns: List[Column],
    vals_itr:Iterable[Iterable[ExprLike]],
) -> Tuple[str, Iterable[Iterable[QueryValTypes]]]:
    # TODO:
    pass



def q_update(
    table: Table,
    column_exprs: List[Tuple[Column, ExprLike]],
    where: Optional[ExprLike],
    count: Optional[int] = None,
) -> str:
    res = 'UPDATE {} SET {}'.format(
        table,
        ', '.join('{} = {}'.format(col, _to_expr(expr)) for col, expr in column_exprs)
    )
    if where : res += ' WHERE {}'.format(_to_expr(where))
    if count : res += ' LIMIT {}'.format(int(count))
    return res


def q_delete(
    table: Table,
    where: Optional[ExprLike],
    count: Optional[int] = None,
) -> str:
    res = 'DELETE FROM {}'.format(table)
    if where : res += ' WHERE {}'.format(_to_expr(where))
    if count : res += ' LIMIT {}'.format(int(count))
    return res


def columns_in_expr(expr:Expression) -> Iterator[Column]:
    if isinstance(expr, Column):
        yield expr
    elif isinstance(expr, Operation):
        yield from columns_in_expr(expr.larg)
        yield from columns_in_expr(expr.rarg)
    elif isinstance(expr, Function):
        for carg in expr.args:
            yield from columns_in_expr(carg)
    elif isinstance(expr, ExprIn):
        yield from columns_in_expr(expr.target) 
    return


def tables_of_columns(columns:Iterable[Column]) -> Iterator[Table]:
    return (col.table for col in set(list(columns)))


def tables_dependency(tables:Iterator[Table]) -> Iterator[Tuple[Table, dict]]:
    # TODO: Implementation
    pass


def _table_expr_by_columns(columns:Iterable[Column]) -> Expression:
    # TODO: Implementation
    pass
