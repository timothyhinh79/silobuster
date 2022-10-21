import sqlalchemy
from sanitization_code.pg_transfer import dump_table, restore_table # used to transfer source table in source database to destination table in destination database
import json # json.dumps() used to insert JSON data into Postgres data table in SQL statement

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

# gets SQL string for generating mapping table with sanitized data, to be used for updating data table in an UPDATE statement
def generate_mapping_tbl(key_schema, sanitized_field):
    return f"""
        DROP TABLE IF EXISTS mapping_temp;
        
        CREATE TABLE mapping_temp (
            {key_schema}
            , {sanitized_field} VARCHAR
        );

        INSERT INTO mapping_temp
        SELECT *
        FROM json_populate_recordset(NULL::mapping_temp, :sanitized_data_json);
    """


# setting up multiple SQL queries to create a mapping table with the keys and sanitized urls, then using that table to update the source table
def update_src_data(src_conn, source_table, source_column, keys, sanitized_data, sanitized_field):

    key_schema = ', '.join([key + ' VARCHAR' for key in keys])
    key_join_str = ' AND '.join([f'{source_table}.{key}=mapping_temp.{key}' for key in keys])

    sql_str = generate_mapping_tbl(key_schema, sanitized_field)

    sql_str += f"""
        UPDATE {source_table}
        SET {source_column} = mapping_temp.{sanitized_field}
        FROM mapping_temp
        WHERE {key_join_str};

        DROP TABLE mapping_temp;
    """

    src_conn.execute(
        sqlalchemy.sql.text(sql_str), sanitized_data_json = json.dumps(sanitized_data)
    )

# get column names from source table, except for dest_column
def get_source_cols_str(src_conn, source_table, dest_column, table_suffix = ''):
    source_cols = src_conn.execute(
        sqlalchemy.sql.text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{source_table}'
                AND column_name != '{dest_column}'
            ORDER BY ordinal_position;
        """)
    ).fetchall()

    return ','.join([f'{source_table}{table_suffix}.{column_name[0]}' for column_name in source_cols])

# creates the destination table with destination column in the destination database
def create_dest_table(src_conn, dest_conn, source_table, dest_table, source_column, dest_column):

    # if the source and destination databases are different, copy table from source DB to dest DB
    if dest_conn.engine.url != src_conn.engine.url:

        dest_conn.execute(
            sqlalchemy.sql.text(f"DROP TABLE IF EXISTS {source_table}; DROP TABLE IF EXISTS {source_table}_src;")
        )

        dump_table(
            host_name = src_conn.engine.url.host, 
            port = src_conn.engine.url.port, 
            database_name = src_conn.engine.url.database, 
            user_name = src_conn.engine.url.username,
            database_password = src_conn.engine.url.password,
            schema_name = 'public', 
            table_name = source_table, 
            dump_file_loc = './tmp/table.dmp'
        )

        restore_table(
            host_name = dest_conn.engine.url.host, 
            port = dest_conn.engine.url.port, 
            database_name = dest_conn.engine.url.database, 
            user_name = dest_conn.engine.url.username,
            database_password = dest_conn.engine.url.password,
            dump_file_loc = './tmp/table.dmp' 
        )

        dest_conn.execute(
            sqlalchemy.sql.text(f"ALTER TABLE {source_table} RENAME TO {source_table}_src;")
        )

        source_cols_str = get_source_cols_str(src_conn, source_table, dest_column, table_suffix = '_src')

        dest_conn.execute(sqlalchemy.sql.text(f"""
                DROP TABLE IF EXISTS {dest_table};

                CREATE TABLE {dest_table} AS
                SELECT {source_table}_src.{source_column} AS {dest_column}, {source_cols_str} 
                FROM {source_table}_src;

                DROP TABLE {source_table}_src;
            """
        ))


    # if the source and destination databases are the same, create the destination table if different from source table
    else: 
        source_cols_str = get_source_cols_str(src_conn, source_table, dest_column)
        if source_table != dest_table:
            dest_conn.execute(sqlalchemy.sql.text(f"""
                    DROP TABLE IF EXISTS {dest_table};

                    CREATE TABLE {dest_table} AS
                    SELECT {source_table}.{source_column} AS {dest_column}, {source_cols_str} 
                    FROM {source_table};
                """
            ))
        # if destination table and source table are the same but source and destination columns are different, add destination column to source table
        elif source_table == dest_table and source_column != dest_column:
            dest_conn.execute(sqlalchemy.sql.text(f"""
                    ALTER TABLE {source_table}
                    ADD {dest_column} VARCHAR
                """
            ))
        # else if source table and destination table are the same, and source and destination columns are also the same, do nothing
        # in this case, update_dest_data() should simply update the source/destination column in the source/destination table

# update the destination table (created by create_dest_table()) with the given sanitized data
def update_dest_data(dest_conn, dest_table, dest_column, keys, sanitized_data, sanitized_field):

    key_schema = ', '.join([key + ' VARCHAR' for key in keys])
    key_dest_update_str = ' AND '.join([f'{dest_table}.{key}=mapping_temp.{key}' for key in keys])

    sql_str = generate_mapping_tbl(key_schema, sanitized_field)

    sql_str += f"""
        UPDATE {dest_table}
        SET {dest_column} = mapping_temp.{sanitized_field}
        FROM mapping_temp
        WHERE {key_dest_update_str};

        DROP TABLE mapping_temp;
    """

    dest_conn.execute(
        sqlalchemy.sql.text(sql_str), sanitized_data_json = json.dumps(sanitized_data)
    )
