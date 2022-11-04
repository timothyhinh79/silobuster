import sqlalchemy

# gets sqlalchemy database engine and connection object for given connection string
def get_db_conn(conn_str):
    db = sqlalchemy.create_engine(conn_str, echo=True)
    conn = db.connect()
    return db, conn

# closes database
def close_db(db, conn):
    conn.close()
    db.dispose()

# get sorting statement for constructing SQL queries for given set of keys
# Sample output: "ORDER BY id1, id2"
def get_key_sorting_statement(keys):
    key_sorting_statement = ', '.join(keys)
    if key_sorting_statement:
        key_sorting_statement = 'order by '+key_sorting_statement
    return key_sorting_statement

# queries the given database for the given column and keys
def query_db(src_conn, table, column, keys, batch, offset):
    key_select = ", ".join(keys)
    key_sorting_statement = get_key_sorting_statement(keys)

    results = src_conn.execute(
        f'''
            select
                {column}, {key_select} 
            from
                {table}
            {key_sorting_statement}
            limit {batch}
            offset {offset}
        '''
    ).fetchall()  

    return results