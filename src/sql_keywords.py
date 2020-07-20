operators = [
    'AND',
    '&&',
    '=',
    ':=',
    'BETWEEN ... AND ...',
    'BINARY',
    '&',
    '~',
    '|',
    '^',
    'CASE',
    'DIV',
    '/',
    '=',
    '<=>',
    '>',
    '>=',
    'IS',
    'IS NOT',
    'IS NOT NULL',
    'IS NULL',
    '<<',
    '<',
    '<=',
    'LIKE',
    '-',
    '%',
    'MOD',
    'NOT',
    '!',
    'NOT BETWEEN ... AND ...',
    '!=',
    '<>',
    'NOT LIKE',
    'NOT REGEXP',
    '||',
    'OR',
    '+',
    'REGEXP',
    '>>',
    'RLIKE',
    'SOUNDS LIKE',
    '*',
    '-',
    'XOR',
]

functions = [
    'ABS',
    'ACOS',
    'ADDDATE',
    'ADDTIME',
    'AES_DECRYPT',
    'AES_ENCRYPT',
    'Area',
    'AsBinary',
    'AsWKB',
    'ASCII',
    'ASIN',
    'AsText',
    'AsWKT',
    'ASYMMETRIC_DECRYPT(',
    'ASYMMETRIC_DERIVE(',
    'ASYMMETRIC_ENCRYPT(',
    'ASYMMETRIC_SIGN(',
    'ASYMMETRIC_VERIFY(',
    'ATAN',
    'ATAN2',
    'ATAN',
    'AVG',
    'BENCHMARK',
    'BIN',
    'BIT_AND',
    'BIT_COUNT',
    'BIT_LENGTH',
    'BIT_OR',
    'BIT_XOR',
    'Buffer',
    'CAST',
    'CEIL',
    'CEILING',
    'Centroid',
    'CHAR',
    'CHAR_LENGTH',
    'CHARACTER_LENGTH',
    'CHARSET',
    'COALESCE',
    'COERCIBILITY',
    'COLLATION',
    'COMPRESS',
    'CONCAT',
    'CONCAT_WS',
    'CONNECTION_ID',
    'Contains',
    'CONV',
    'CONVERT',
    'CONVERT_TZ',
    'COS',
    'COT',
    'COUNT',
    'CRC32',
    'CREATE_ASYMMETRIC_PRIV_KEY(',
    'CREATE_ASYMMETRIC_PUB_KEY(',
    'CREATE_DH_PARAMETERS(',
    'CREATE_DIGEST(',
    'Crosses',
    'CURDATE',
    'CURRENT_DATE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'CURRENT_USER',
    'CURTIME',
    'DATABASE',
    'DATE',
    'DATE_ADD',
    'DATE_FORMAT',
    'DATE_SUB',
    'DATEDIFF',
    'DAY',
    'DAYNAME',
    'DAYOFMONTH',
    'DAYOFWEEK',
    'DAYOFYEAR',
    'DECODE',
    'DEFAULT',
    'DEGREES',
    'DES_DECRYPT',
    'DES_ENCRYPT',
    'Dimension',
    'Disjoint',
    'ELT',
    'ENCODE',
    'ENCRYPT',
    'EndPoint',
    'Envelope',
    'Equals',
    'EXP',
    'EXPORT_SET',
    'ExteriorRing',
    'EXTRACT',
    'ExtractValue',
    'FIELD',
    'FIND_IN_SET',
    'FLOOR',
    'FORMAT',
    'FOUND_ROWS',
    'FROM_BASE64',
    'FROM_DAYS',
    'FROM_UNIXTIME',
    'GeomCollFromText',
    'GeometryCollectionFromText',
    'GeomCollFromWKB',
    'GeometryCollectionFromWKB',
    'GeometryCollection',
    'GeometryN',
    'GeometryType',
    'GeomFromText',
    'GeometryFromText',
    'GeomFromWKB',
    'GET_FORMAT',
    'GET_LOCK',
    'GLength',
    'GREATEST',
    'GROUP_CONCAT',
    'GTID_SUBSET',
    'GTID_SUBTRACT',
    'HEX',
    'HOUR',
    'IF',
    'IFNULL',
    'IN',
    'INET_ATON',
    'INET_NTOA',
    'INET6_ATON(',
    'INET6_NTOA(',
    'INSERT',
    'INSTR',
    'InteriorRingN',
    'Intersects',
    'INTERVAL',
    'IS_FREE_LOCK',
    'IS_IPV4(',
    'IS_IPV4_COMPAT(',
    'IS_IPV4_MAPPED(',
    'IS_IPV6',
    'IS_USED_LOCK',
    'IsClosed',
    'IsEmpty',
    'ISNULL',
    'IsSimple',
    'LAST_INSERT_ID',
    'LCASE',
    'LEAST',
    'LEFT',
    'LENGTH',
    'LineFromText',
    'LineFromWKB',
    'LineStringFromWKB',
    'LineString',
    'LN',
    'LOAD_FILE',
    'LOCALTIME',
    'LOCALTIMESTAMP',
    'LOCATE',
    'LOG',
    'LOG10',
    'LOG2',
    'LOWER',
    'LPAD',
    'LTRIM',
    'MAKE_SET',
    'MAKEDATE',
    'MAKETIME',
    'MASTER_POS_WAIT',
    'MAX',
    'MBRContains',
    'MBRDisjoint',
    'MBREqual',
    'MBRIntersects',
    'MBROverlaps',
    'MBRTouches',
    'MBRWithin',
    'MD5',
    'MICROSECOND',
    'MID',
    'MIN',
    'MINUTE',
    'MLineFromText',
    'MultiLineStringFromText',
    'MLineFromWKB',
    'MultiLineStringFromWKB',
    'MOD',
    'MONTH',
    'MONTHNAME',
    'MPointFromText',
    'MultiPointFromText',
    'MPointFromWKB',
    'MultiPointFromWKB',
    'MPolyFromText',
    'MultiPolygonFromText',
    'MPolyFromWKB',
    'MultiPolygonFromWKB',
    'MultiLineString',
    'MultiPoint',
    'MultiPolygon',
    'NAME_CONST',
    'NOT IN',
    'NOW',
    'NULLIF',
    'NumGeometries',
    'NumInteriorRings',
    'NumPoints',
    'OCT',
    'OCTET_LENGTH',
    'OLD_PASSWORD',
    'ORD',
    'Overlaps',
    'PASSWORD',
    'PERIOD_ADD',
    'PERIOD_DIFF',
    'PI',
    'Point',
    'PointFromText',
    'PointFromWKB',
    'PointN',
    'PolyFromText',
    'PolygonFromText',
    'PolyFromWKB',
    'PolygonFromWKB',
    'Polygon',
    'POSITION',
    'POW',
    'POWER',
    'PROCEDURE ANALYSE',
    'QUARTER',
    'QUOTE',
    'RADIANS',
    'RAND',
    'RANDOM_BYTES',
    'RELEASE_LOCK',
    'REPEAT',
    'REPLACE',
    'REVERSE',
    'RIGHT',
    'ROUND',
    'ROW_COUNT',
    'RPAD',
    'RTRIM',
    'SCHEMA',
    'SEC_TO_TIME',
    'SECOND',
    'SESSION_USER',
    'SHA1',
    'SHA',
    'SHA2',
    'SIGN',
    'SIN',
    'SLEEP',
    'SOUNDEX',
    'SPACE',
    'SQL_THREAD_WAIT_AFTER_GTIDS',
    'SQRT',
    'SRID',
    'ST_Area',
    'ST_Centroid',
    'ST_Contains',
    'ST_Crosses',
    'ST_Difference',
    'ST_Disjoint',
    'ST_Distance',
    'ST_Envelope',
    'ST_Equals',
    'ST_Intersection',
    'ST_Intersects',
    'ST_Overlaps',
    'ST_SymDifference',
    'ST_Touches',
    'ST_Union',
    'ST_Within',
    'StartPoint',
    'STD',
    'STDDEV',
    'STDDEV_POP',
    'STDDEV_SAMP',
    'STR_TO_DATE',
    'STRCMP',
    'SUBDATE',
    'SUBSTR',
    'SUBSTRING',
    'SUBSTRING_INDEX',
    'SUBTIME',
    'SUM',
    'SYSDATE',
    'SYSTEM_USER',
    'TAN',
    'TIME',
    'TIME_FORMAT',
    'TIME_TO_SEC',
    'TIMEDIFF',
    'TIMESTAMP',
    'TIMESTAMPADD',
    'TIMESTAMPDIFF',
    'TO_BASE64',
    'TO_DAYS',
    'TO_SECONDS',
    'Touches',
    'TRIM',
    'TRUNCATE',
    'UCASE',
    'UNCOMPRESS',
    'UNCOMPRESSED_LENGTH',
    'UNHEX',
    'UNIX_TIMESTAMP',
    'UpdateXML',
    'UPPER',
    'USER',
    'UTC_DATE',
    'UTC_TIME',
    'UTC_TIMESTAMP',
    'UUID',
    'UUID_SHORT',
    'VALIDATE_PASSWORD_STRENGTH',
    'VALUES',
    'VAR_POP',
    'VAR_SAMP',
    'VARIANCE',
    'VERSION',
    'WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS',
    'WEEK',
    'WEEKDAY',
    'WEEKOFYEAR',
    'WEIGHT_STRING',
    'Within',
    'X',
    'Y',
    'YEAR',
    'YEARWEEK',
]
