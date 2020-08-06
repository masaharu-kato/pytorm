"""
    sql.datatypes - The definitions of data types in the database system
"""
from typing import Collection
import datetime
from common.extype import ExType, RangedType, LenLimitedType

class DataType:
    """ The type of data in the database system """
    def __init__(self, dbtype:str, pytype:ExType):
        self.dbtype = dbtype
        self.pytype = pytype


def _singed_range(bits:int) -> Collection[int]:
    return range(-(2 ** (bits - 1)), 2 ** (bits - 1))

def _unsigned_range(bits:int) -> Collection[int]:
    return range(0, (2 ** bits))



TinyInt   = DataType('TINYINT'  , RangedType(int, _singed_range( 8)))
SmallInt  = DataType('SMALLINT' , RangedType(int, _singed_range(16)))
MediumInt = DataType('MEDIUMINT', RangedType(int, _singed_range(24)))
Int       = DataType('INT'      , RangedType(int, _singed_range(32)))
BigInt    = DataType('BIGINT'   , RangedType(int, _singed_range(64)))

UnsignedTinyInt   = DataType('UNSIGNED TINYINT'  , RangedType(int, _unsigned_range( 8)))
UnsignedSmallInt  = DataType('UNSIGNED SMALLINT' , RangedType(int, _unsigned_range(16)))
UnsignedMediumInt = DataType('UNSIGNED MEDIUMINT', RangedType(int, _unsigned_range(24)))
UnsignedInt       = DataType('UNSIGNED INT'      , RangedType(int, _unsigned_range(32)))
UnsignedBigInt    = DataType('UNSIGNED BIGINT'   , RangedType(int, _unsigned_range(64)))

Float   = DataType('FLOAT'  , ExType(float))
Double  = DataType('DOUBLE' , ExType(float))
Real    = DataType('REAL'   , ExType(float))
Decimal = DataType('DECIMAL', ExType(float))

def Char(l:int):
    return DataType('CHAR', LenLimitedType(str, l))

def VarChar(l:int):
    return DataType('VARCHAR', LenLimitedType(str, l))

def Binary(l:int):
    return DataType('BINARY', LenLimitedType(bytes, l))

def VarBinary(l:int):
    return DataType('VARBINARY', LenLimitedType(bytes, l))

TinyBlob   = DataType('TINYBLOB'  , LenLimitedType(bytes, 2 **  8 - 1))
Blob       = DataType('BLOB'      , LenLimitedType(bytes, 2 ** 16 - 1))
MediumBlob = DataType('MEDIUMBLOB', LenLimitedType(bytes, 2 ** 24 - 1))
LongBlob   = DataType('LONGBLOB'  , LenLimitedType(bytes, 2 ** 32 - 1))

TinyText   = DataType('TINYTEXT'  , LenLimitedType(str, 2 **  8 - 1))
Text       = DataType('TEXT'      , LenLimitedType(str, 2 ** 16 - 1))
MediumText = DataType('MEDIUMTEXT', LenLimitedType(str, 2 ** 24 - 1))
LongText   = DataType('LONGTEXT'  , LenLimitedType(str, 2 ** 32 - 1))

# class Enum:
#     pass

# class Set:
#     pass


Date = DataType('DATE', ExType(datetime.date))
Time = DataType('TIME', ExType(datetime.time))
DateTime = DataType('DATETIME', ExType(datetime.datetime))

# class Timestamp(datetime.datetime):
#     pass

# class Year(int):
#     pass
