import typing
import datetime


class TinyInt(int):
    pass

class SmallInt(int):
    pass

class MediumInt(int):
    pass

class Int(int):
    pass

class BigInt(int):
    pass

class Float(float):
    pass

class Double(float):
    pass

class Real(float):
    pass

class Decimal(float):
    pass


T = typing.TypeVar('T')
L = typing.TypeVar('L')

class Unsigned(typing.Generic[T]):
    pass

class WithLength(typing.Generic[T, L]):
    pass


class Char(str):
    @classmethod
    def __class_getitem__(cls, l:int):
        return '{}[{}]'.format(cls.__name__, l)

class VarChar(str):
    @classmethod
    def __class_getitem__(cls, l:int):
        return '{}[{}]'.format(cls.__name__, l)

class TinyBlob(bytes):
    pass

class Blob(bytes):
    @classmethod
    def __class_getitem__(cls, l:int):
        return '{}[{}]'.format(cls.__class__.__name__, l)

class MediumBlob(bytes):
    pass

class LongBlob(bytes):
    pass

class TinyText(str):
    pass

class Text(str):
    def __init__(self, l:int):
        self.length = l

class MediumText(str):
    pass

class LongText(str):
    pass

# class Enum:
#     pass

# class Set:
#     pass


class Date(datetime.date):
    pass

class Time(datetime.time):
    pass

class DateTime(datetime.datetime):
    pass

# class Timestamp(datetime.datetime):
#     pass

# class Year(int):
#     pass
