

class Select:
    """ The data object for the sql SELECT query """

    def __init__(self,
        db     : Database,
        columns: Sequence[Expr],
        *,
        where  : Optional[ExprLike] = None,
        group  : Optional[Sequence[Expr]] = None,
        having : Optional[ExprLike] = None,
        order  : Optional[Iterable[Tuple[Expr, str]]] = None,
        count  : Optional[int] = None,
        offset : Optional[int] = None,
    ) -> None:
        self.db: Database = db
        self.column_exprs: List[Expr] = list(columns)
        self.where_expr  : Optional[Expr] = to_expr(where) if where is not None else None
        self.group_exprs : Optional[Sequence[Expr]] = list(group) if group is not None else None
        self.having_expr : Optional[Expr] = to_expr(having) if having is not None else None

        self.order_exprs : Optional[List[Tuple[Expr, bool]]] = [
            (expr, self._is_asc_or_desc(ad_str)) for expr, ad_str in order
        ] if order is not None else None
        
        self.count : Optional[int] = count
        self.offset: Optional[int] = offset


    def sql_query(self) -> SQLQuery:
        """ Generate the sql SELECT query """
        q = SQLQuery('SELECT', self.column_exprs, 'FROM', self.tables_query())
        if self.where_expr  is not None:
            q += SQLQuery('WHERE', self.where_expr)
        if self.group_exprs is not None:
            q += SQLQuery('GROUP BY', self.group_exprs)
        if self.having_expr is not None:
            q += SQLQuery('HAVING', self.having_expr)
        if self.order_exprs is not None:
            q += SQLQuery('ORDER BY', [
                SQLQuery(column, ('ASC' if dstr else 'DESC'))
                for column, dstr in self.order_exprs
            ])
        if self.count  is not None:
            q += SQLQuery('LIMIT', self.count)
        if self.offset is not None:
            q += SQLQuery('OFFSET', self.offset)
        return q

    def exec(self):
        self.result = self.db.exec(self.sql_query())

    def __iter__(self):
        return self.result

    def next_block(self):
        self.offset += self.count

    def prev_block(self):
        self.offset -= self.count


    @staticmethod
    def _is_asc_or_desc(s:str) -> bool:
        """ Get True (asc) or False (desc) by the string which represents asc or desc """

        if s == '+' or s.upper() == 'A' or s.upper() == 'ASC':
            return True
        if s == '-' or s.upper() == 'D' or s.upper() == 'DESC':
            return False

        raise RuntimeError('Invalid string of asc or desc')


    @classmethod
    def extract_column_exprs(cls,
        expr:Union[Exprs, Iterable[Exprs]]
    ) -> Iterator[ColumnExpr]:
        """ Extract column schema expressions in the given expressions """
        
        if not isinstance(expr, str) and isinstance(expr, Iterable):
            for _expr in expr:
                yield from cls.extract_column_exprs(_expr)
            return
        
        if isinstance(expr, ColumnExpr):
            yield expr
            return

        if isinstance(expr, OpExpr):
            yield from cls.extract_column_exprs(expr.larg)
            yield from cls.extract_column_exprs(expr.rarg)
            return

        if isinstance(expr, FuncExpr):
            for _expr in expr.args:
                yield from cls.extract_column_exprs(_expr)
            return

        raise TypeError('Unexcepted type `{}`.'.format(type(expr)))


    def all_exprs(self) -> Iterator[Exprs]:
        """ Get the all expressions in this select object """
        yield from self.column_exprs
        if self.where_expr is not None:
            yield self.where_expr 
        if self.group_exprs is not None:
            yield from self.group_exprs
        if self.having_expr is not None:
            yield self.having_expr
        if self.order_exprs is not None:
            yield from (order_expr[0] for order_expr in self.order_exprs)


    def tables_query(self) -> SQLQuery:
        """ Generate the table-part of the sql query """

        all_exprs = list(self.all_exprs())
        all_column_exprs = list(self.extract_column_exprs(all_exprs))

        tables_column_connections:Dict[TableName, List[Tuple[Column, Column]]] = {}
        
        for column_expr in all_column_exprs:
            # print(repr(column_expr))

            column_cons = list(column_expr.column_connections())
            # print('column_cons:', column_cons)

            if not column_cons:
                continue
            
            c_table = column_cons[0][0].table 
            
            if c_table.name not in tables_column_connections:
                tables_column_connections[c_table.name] = []
            
            c_cons = tables_column_connections[c_table.name]
            for new_con in column_cons:
                if all(
                        (is_same(new_con[0], _con[0]) and is_same(new_con[1], _con[1]))
                        for _con in c_cons
                    ):
                    # print('add', column_con)
                    c_cons.append(new_con)


        tables_joins:List[SQLQuery] = []

        for table_name, column_connections in tables_column_connections.items():

            c_query = SQLQuery(self.db.table(table_name))

            for column_from, column_to in column_connections:

                if isinstance(column_to, ColumnInAliasedTable):
                    c_table_query = SQLQuery(column_to.aliased_table, use_full=True)
                else:
                    c_table_query = SQLQuery(column_to.table)

                c_query += SQLQuery(
                    'INNER JOIN', c_table_query,
                    'ON', column_from, '=', column_to
                )
                
            tables_joins.append(c_query)

        return SQLQuery(tables_joins)
