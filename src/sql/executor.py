"""
    sql.executor - SQL query executor
"""
from typing import Any, Iterable, Iterator, List, Optional, Union
import mysql.connector
from sql.expression import Query

class Connector:
    """ SQL Executor Object (Database Cursor) """

    def __init__(self, *args, **kwargs) -> None:
        # TODO: Implementation
        self.con_args = args
        self.con_kwargs = kwargs

    def __enter__(self):
        return Connection(self.db, mysql.connector.connect(self.con_args, self.con_kwargs))


MySQLCon = mysql.connector.abstracts.MySQLConnectionAbstract

class Connection():

    def __init__(self, db, _con):
        self._con = _con
        self.closed = False

    @property
    def con(self):
        if self.closed:
            raise RuntimeError('This connection is already closed.')
        return self._con

    def __getattr__(self, name):
        return self.con.name

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if not self.closed:
            self._con.close()
            self.closed = True

    def operate(self, db) -> 'Operation':
        return Operation(db, self.con.cursor())


MySQLCur = mysql.connector.abstracts.MySQLCursorAbstract
OperationParamType = Optional[Union[bool, int, float, str]]

class Operation():

    def __init__(self, db, con:Connection):
        self.db = db
        self.con = con
        self._cur:MySQLCur
        self.closed = False

    @property
    def cur(self):
        if not hasattr(self, '_cur'):
            raise RuntimeError('Cursor is not created yet.')
        if self.closed:
            raise RuntimeError('This cursor is already closed.')
        return self._cur

    def __enter__(self):
        self._cur = self.con.con.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if not self.closed:
            self._cur.close()
            self.closed = True

    def __getattr__(self, name):
        return self.cur.name

    def execute(self,
        q:Query,
        params:Optional[Iterable[OperationParamType]] = None
    ) -> List[List[Any]]:
        print('Exec SQL:', q, 'Values:', list(params) if params else None)
        self.cur.execute(q.query(), params)
        return self.cur.fetchall()

    def execute_many(self,
        q:Query,
        params_itr:Iterable[Iterable[OperationParamType]]
    ) -> List[List[Any]]:
        print('Execmany SQL:', q, 'Values:', [list(params) for params in params_itr])
        self.cur.execute_many(q.query(), params_itr)
        return self.cur.fetchall()




        
