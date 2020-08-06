import sql.executor

sample_connector = sql.executor.Connector(
    host='hostname',
    port='3306',
    user='username',
    password='password',
    database='dbname'
)
