"""
    sql.query - SQL query and schema objects (base classes)
"""

from typing import Any, final, Iterable, Iterator, List, Optional, Sequence, Tuple, Union
from abc import ABCMeta, abstractmethod
import datetime
from sql import keywords


class Expr(metaclass=ABCMeta):
    """ SQL expression base type """

    ## ---- basic methods ---- ##

    @abstractmethod
    def __sql__(self) -> 'Query':
        """ Get the sql query expression """

    def __full_sql__(self) -> 'Query':
        """ Get the full sql query expression
            In default, get the same expression as `self.__sql__()`

            For example, in the aliased-table object, 
                In `self.__sql__()`, returns the aliase name only
                In `self.__full_sql__()`, returns the `expression AS alias-name` 
                (See the implementation in AliasedTable class)
        """
        return self.__sql__() # default implementation

    def alias(self, alias_name:str):
        """ Return the aliased expression of this object
        """
        return AliasedExpr(self, alias_name)

    @final
    def __matmul__(self, alias_name:str):
        """ Return the aliased expression of this object
            The alias of `self.alias(alias_name)`
        """
        if not isinstance(alias_name, str):
            return NotImplemented
        return self.alias(alias_name)

    @abstractmethod
    def __repr__(self) -> str:
        """ Get the string representation for debug

            The identity of `Expr` class objects as the database objects
            are determined by this representation.
        """

    def extract_exprs(self) -> Iterator[Expr]:
        """ Extract expression(s) in this expression
            In default, return this expression object
        """
        yield self

    @final
    def is_same(self, expr) -> bool:
        return is_same(self, expr)


    ## ---- sql query operator expression ---- ##

    def __add__(self, expr):
        """ The sql query expression `self + expr` """
        return OpExpr('+', self, expr)

    def __sub__(self, expr):
        """ The sql query expression `self - expr` """
        return OpExpr('-', self, expr)

    def __mul__(self, expr):
        """ The sql query expression `self * expr` """
        return OpExpr('*', self, expr)

    def __truediv__(self, expr):
        """ The sql query expression `self / expr` """
        return OpExpr('/', self, expr)

    def __mod__(self, expr):
        """ The sql query expression `self % expr` """
        return OpExpr('%', self, expr)

    def __and__(self, expr):
        """ The sql query expression `self & expr` """
        return OpExpr('&', self, expr)

    def __or__ (self, expr):
        """ The sql query expression `self | expr` """
        return OpExpr('|', self, expr)

    def __radd__(self, expr):
        """ The sql query expression `expr + self` """
        return OpExpr('+', expr, self)

    def __rsub__(self, expr):
        """ The sql query expression `expr - self` """
        return OpExpr('-', expr, self)

    def __rmul__(self, expr):
        """ The sql query expression `expr * self` """
        return OpExpr('*', expr, self)

    def __rtruediv__(self, expr):
        """ The sql query expression `self / expr` """
        return OpExpr('/', expr, self)

    def __rmod__(self, expr):
        """ The sql query expression `expr % self` """
        return OpExpr('%', expr, self)

    def __rand__(self, expr):
        """ The sql query expression `expr & self` """
        return OpExpr('&', expr, self)

    def __ror__ (self, expr):
        """ The sql query expression `expr | self` """
        return OpExpr('|', expr, self)

    def __lt__(self, expr):
        """ The sql query expression `expr < self` """
        return OpExpr('<' , self, expr)

    def __le__(self, expr):
        """ The sql query expression `expr <= self` """
        return OpExpr('<=', self, expr)

    def __eq__(self, expr):
        """ The sql query expression `expr = self` """
        return OpExpr('=' , self, expr)

    def __ne__(self, expr):
        """ The sql query expression `expr != self` """
        return OpExpr('!=', self, expr)

    def __gt__(self, expr):
        """ The sql query expression `expr > self` """
        return OpExpr('>' , self, expr)

    def __ge__(self, expr):
        """ The sql query expression `expr >= self` """
        return OpExpr('>=', self, expr)

    def __contains__(self, expr):
        """ The sql query expression `expr IN self` """
        return OpExpr('IN', expr, self)


    ## ---- other not-implemented methods ---- ##

    def __bool__(self) -> bool:
        raise RuntimeError('Cannot convert expression object to boolean.')

    def __int__(self) -> int:
        raise RuntimeError('Cannot convert expression object to integer.')

    def __float__(self) -> float:
        raise RuntimeError('Cannot convert expression object to float.')

    def __str__(self) -> str:
        raise RuntimeError('Cannot convert expression object to string.')

    def __hash__(self) -> int:
        raise RuntimeError('Cannot calculate a hash value of expression object.')


def is_same(expr1:Expr, expr2:Expr) -> bool:
    if not isinstance(expr1, Expr) or not isinstance(expr2, Expr):
        raise NotImplementedError()
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



ExprsArg = Union[
    'Query', Expr, RawValType,
    Iterable[Union['Query', Expr, RawValType]]
]

class Query(Expr):
    """ SQL Query text object """

    def __init__(self, *exprs:Optional[ExprsArg], **options) -> None:
        self.exprs:List[ExprsArg] = [expr for expr in exprs if expr is not None]
        self.options = options

    def __sql__(self) -> 'Query':
        return self

    def __repr__(self) -> str:
        return 'Query(' + ' '.join(map(repr, self.exprs)) + ', ' + repr(self.options) + ')'
            
    def query_text(self) -> str:
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
        obj:ExprsArg,
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
                raw = obj.__full_sql__().query_text()
            else:
                raw = obj.__sql__().query_text()
                
        if as_obj:
            if quoted:
                raise RuntimeError('Cannot specify both `as_obj` and `quoted`.')
            return '`' + raw.replace('`', '``') + '`'

        if quoted:
            return '"' + raw.replace('\\', '\\\\').replace('"', '\\"')

        return raw

    @staticmethod
    def as_obj(*obj:Optional[ExprsArg]) -> 'Query':
        """ Create the query object with `as_obj` option """
        return Query(*obj, as_obj=True)

    @staticmethod
    def quoted(*obj:Optional[ExprsArg]) -> 'Query':
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

    def extract_exprs(self) -> Iterator[Expr]:
        yield from super().extract_exprs()
        yield from self.larg.extract_exprs()
        yield from self.rarg.extract_exprs()


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

    def extract_exprs(self) -> Iterator[Expr]:
        yield from super().extract_exprs()
        for arg in self.args:
            yield from arg.extract_exprs()

