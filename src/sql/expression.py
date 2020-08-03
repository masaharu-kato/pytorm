"""
    sql.query - SQL query and schema objects (base classes)
"""

from typing import Any, final, Iterable, Iterator, List, Optional, Sequence, Tuple, Union
from abc import ABCMeta, abstractmethod
import datetime
from sql import keywords


class Expr(metaclass=ABCMeta):
    """ SQL expression base type """
    def __add__(self, expr):
        return OpExpr('+', self, expr)

    def __sub__(self, expr):
        return OpExpr('-', self, expr)

    def __mul__(self, expr):
        return OpExpr('*', self, expr)

    def __truediv__(self, expr):
        return OpExpr('/', self, expr)

    def __mod__(self, expr):
        return OpExpr('%', self, expr)

    def __and__(self, expr):
        return OpExpr('&', self, expr)

    def __or__ (self, expr):
        return OpExpr('|', self, expr)

    def __radd__(self, expr):
        return OpExpr('+', expr, self)

    def __rsub__(self, expr):
        return OpExpr('-', expr, self)

    def __rmul__(self, expr):
        return OpExpr('*', expr, self)

    def __rmod__(self, expr):
        return OpExpr('%', expr, self)

    def __rand__(self, expr):
        return OpExpr('&', expr, self)

    def __ror__ (self, expr):
        return OpExpr('|', expr, self)

    def __lt__(self, expr):
        return OpExpr('<' , self, expr)

    def __le__(self, expr):
        return OpExpr('<=', self, expr)

    def __eq__(self, expr):
        return OpExpr('=' , self, expr)

    def __ne__(self, expr):
        return OpExpr('!=', self, expr)

    def __gt__(self, expr):
        return OpExpr('>' , self, expr)

    def __ge__(self, expr):
        return OpExpr('>=', self, expr)

    def __contains__(self, expr):
        """ 'A in B' expression """
        return OpExpr('IN', expr, self)

    @abstractmethod
    def __sql__(self) -> 'Query':
        """ Get the sql query expression """

    def __full_sql__(self) -> 'Query':
        """ Get the full sql query expression """
        return self.__sql__() # default implementation

    def alias(self, alias_name:str):
        return AliasedExpr(self, alias_name)

    @final
    def __matmul__(self, alias_name:str):
        if not isinstance(alias_name, str):
            return NotImplemented
        return self.alias(alias_name)

    @abstractmethod
    def __repr__(self) -> str:
        """ Get the string representation for debug """

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


RawValType = Union[
    None, bool, int, float, str,
    datetime.datetime, datetime.date, datetime.time
]
ExprLike = Union[Expr, RawValType]
Exprs = Union[Expr, Sequence[Expr]]
QueryType = Union[
    'Query', Expr, RawValType,
    Iterable[Union['Query', Expr, RawValType]]
]

class AliasedExpr(Expr):
    """ SQL Expression with alias name """

    def __init__(self, expr:Expr, alias_name:str) -> None:
        self.expr = expr
        self.alias_name = alias_name

    def __sql__(self) -> 'Query':
        return Query.as_obj(self.alias_name)

    def __full_sql__(self) -> 'Query':
        return Query(self.expr.__sql__(), 'AS', Query.as_obj(self.alias_name))

    def __repr__(self) -> str:
        return '{' + repr(self.expr) + '@' + self.alias_name + '}'



class Query(Expr):
    """ SQL Query text object """

    def __init__(self, *exprs:Optional[QueryType], **options) -> None:
        self.exprs:List[QueryType] = [expr for expr in exprs if expr is not None]
        self.options = options

    def __sql__(self) -> 'Query':
        return self

    def __repr__(self) -> str:
        return 'Query(' + ' '.join(map(repr, self.exprs)) + ', ' + self.options + ')'
            
    def query(self) -> str:
        """ Get Query string """
        q = ''
        for expr in self.exprs:
            text = self._to_str(expr, **self.options)
            if text:
                # print('self.q({}) <- text({})'.format(self.q, text))
                # print(self.q[-1] if self.q else None, text[0])
                if q and q[-1] not in {'.', '('} and text[0] not in {'.', '(', ')'}:
                    q += ' '
                q += text
        return q

    @classmethod
    def _to_str(cls,
        obj:QueryType,
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

    @staticmethod
    def as_obj(*obj:Optional[QueryType]) -> 'Query':
        """ Create the query object with `as_obj` option """
        return Query(*obj, as_obj=True)

    @staticmethod
    def quoted(*obj:Optional[QueryType]) -> 'Query':
        """ Create the query object with `quoted` option """
        return Query(*obj, quoted=True)


class Value(Expr):
    """ SQL value expression (int, float, string, datetime, ...) """

    def __init__(self, v:RawValType) -> None:
        self.v = v

    def __sql__(self) -> Query:
        if self.v is None:
            return Query('NULL')

        if isinstance(self.v, (int, float)):
            return Query(str(self.v))

        if isinstance(self.v, (datetime.datetime, datetime.date, datetime.time)):
            return Query.quoted(self.v.isoformat())

        return Query.quoted(self.v)

    def __repr__(self) -> str:
        return 'Val(' + repr(self.v) + ')'


class Values(Expr):
    """ SQL multiple values expression """

    def __init__(self, values:Iterable[Expr]):
        self.values = [to_expr(v) for v in values]

    def __iter__(self):
        return self.values

    def __sql__(self) -> Query:
        return Query('(', self.values, ')')

    def __repr__(self) -> str:
        return 'Vals(' + ', '.join(map(repr, self.values)) + ')'


class OpExpr(Expr):
    """ Operator expression """

    def __init__(self, op:str, larg, rarg) -> None:
        self.op = op
        self.larg = to_expr(larg)
        self.rarg = to_expr(rarg)

    def __sql__(self) -> Query:
        if self.op not in keywords.operators:
            raise RuntimeError('Unknown operator `{}`'.format(self.op))
        return Query('(', self.larg, self.op, self.rarg, ')')

    def __repr__(self) -> str:
        return 'Op:' + repr(self.larg) + ' ' + self.op + ' ' + repr(self.rarg) + ')'


# class MultipleOperation(Expr):

#     def __init__(self, op:str, exprs):
#         self.op = op
#         self.exprs = to_expr(exprs)

#     def __sql__(self) -> Query:
#         if self.op not in sql_keywords.operators:
#             raise RuntimeError('Unknown operator `{}`'.format(self.op))
#         return '({})'.format(' {} '.format(self.op).join(self.exprs)) 



class FuncExpr(Expr):
    """ Function expression """
    
    def __init__(self, name:str, *args) -> None:
        self.name = name
        self.args = [to_expr(arg) for arg in args]

    def __sql__(self) -> Query:
        if self.name not in keywords.functions:
            raise RuntimeError('Unknown function `{}`'.format(self.name))
        return Query(self.name, '(', self.args, ')')

    def __repr__(self) -> str:
        return 'Func:' + self.name + '(' + ', '.join(map(repr, self.args)) + ')'

def to_sql_query(q:Query) -> str:
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
