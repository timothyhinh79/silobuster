from subprocess import PIPE,Popen
# from sanitization_code.pg_transfer import dump_table, restore_table
import json
import sqlalchemy
from classes.infokind import InfoKind
from typing import List, Dict, Tuple, Union
import pprint

class Src2Dest(object):
    def __init__(self, kind: InfoKind, key: List[str], source_conn_str: str, dest_conn_str: str, logging_db: str, logging_table: str, 
                 source_table: str, source_column: str, dest_table: Union[str, None], dest_column: Union[str, None],
                 job_timestamp: str):
        self.kind = kind
        self.key = key
        self.source_conn_str = source_conn_str
        self.dest_conn_str = dest_conn_str
        self.logging_db = logging_db
        self.logging_table = logging_table
        self.source_table = source_table
        self.source_column = source_column
        self.dest_table = dest_table
        self.dest_column = dest_column
        self.job_timestamp = job_timestamp

        self.source_db = None
        self.source_conn = None
        self.dest_db = None
        self.dest_conn = None
        
    def __str__(self):
        return 'Src2Dest(\n  ' + pprint.pformat(self.__dict__) +')'

    def _open_source_conn(self):
        self.source_db = sqlalchemy.create_engine(self.source_conn_str, echo=True)
        self.source_conn = self.source_db.connect()

    def _close_source_conn(self):
        self.source_conn.close()
        self.source_db.dispose()

    def _open_dest_conn(self, dest_conn_str = ''):
        self.dest_db = sqlalchemy.create_engine(self.dest_conn_str, echo=True)
        self.dest_conn = self.dest_db.connect()

    def _close_dest_conn(self):
        self.dest_conn.close()
        self.dest_db.dispose()
    
    # get sorting statement for constructing SQL queries for given set of keys
    # Sample output: "ORDER BY id1, id2"
    def get_key_sorting_statement(self):
        key_sorting_statement = ', '.join(self.key)
        if key_sorting_statement:
            key_sorting_statement = 'order by '+key_sorting_statement
        return key_sorting_statement

    # queries the given database for the given column and keys
    def query_db(self, batch, offset):
        key_select = ", ".join(self.key)
        key_sorting_statement = self.get_key_sorting_statement()

        self._open_source_conn()
        results = self.source_conn.execute(
            f'''
                select
                    {self.source_column}, {key_select} 
                from
                    {self.source_table}
                {key_sorting_statement}
                limit {batch}
                offset {offset}
            '''
        ).fetchall()  
        self._close_source_conn()

        return results

    # gets SQL string for generating mapping table with sanitized data, to be used for updating data table in an UPDATE statement
    def generate_mapping_tbl(self):
        key_schema = ', '.join([key + ' VARCHAR' for key in self.key])
        return f"""
            DROP TABLE IF EXISTS mapping_temp;
            
            CREATE TABLE mapping_temp (
                {key_schema}
                , {self.kind} VARCHAR
            );

            INSERT INTO mapping_temp
            SELECT *
            FROM json_populate_recordset(NULL::mapping_temp, :sanitized_data_json);
        """

    # setting up multiple SQL queries to create a mapping table with the keys and sanitized urls, then using that table to update the source table
    def update_src_data(self, sanitized_data):

        key_join_str = ' AND '.join([f'{self.source_table}.{key}=mapping_temp.{key}' for key in self.key])
        sql_str = self.generate_mapping_tbl()
        sql_str += f"""
            UPDATE {self.source_table}
            SET {self.source_column} = mapping_temp.{self.kind}
            FROM mapping_temp
            WHERE {key_join_str};

            DROP TABLE mapping_temp;
        """

        self._open_source_conn()
        self.source_db.execute(
            sqlalchemy.sql.text(sql_str), sanitized_data_json = json.dumps(sanitized_data)
        )
        self._close_source_conn()

    # get column names from source table, except for dest_column
    def get_source_cols_str(self, table_suffix = ''):
        self._open_source_conn()
        source_cols = self.source_conn.execute(
            sqlalchemy.sql.text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{self.source_table}'
                    AND column_name != '{self.dest_column}'
                ORDER BY ordinal_position;
            """)
        ).fetchall()
        self._close_source_conn()

        return ','.join([f'{self.source_table}{table_suffix}.{column_name[0]}' for column_name in source_cols])

    # creates the destination table with destination column in the destination database
    def create_dest_table(self):

        self._open_dest_conn()
        # if the source and destination databases are different, copy table from source DB to dest DB
        if self.dest_conn.engine.url != self.source_conn.engine.url:

            self.dest_conn.execute(sqlalchemy.sql.text(f"DROP TABLE IF EXISTS {self.source_table}; DROP TABLE IF EXISTS {self.source_table}_src;"))
            
            self.dump_table(dump_file_loc = './tmp/table.dmp')
            self.restore_table(dump_file_loc = './tmp/table.dmp')
            self.dest_conn.execute(sqlalchemy.sql.text(f"ALTER TABLE {self.source_table} RENAME TO {self.source_table}_src;"))
            source_cols_str = self.get_source_cols_str(table_suffix = '_src')

            self.dest_conn.execute(sqlalchemy.sql.text(f"""
                    DROP TABLE IF EXISTS {self.dest_table};

                    CREATE TABLE {self.dest_table} AS
                    SELECT {self.source_table}_src.{self.source_column} AS {self.dest_column}, {source_cols_str} 
                    FROM {self.source_table}_src;

                    DROP TABLE {self.source_table}_src;
                """
            ))


        # if the source and destination databases are the same, create the destination table if different from source table
        else: 
            source_cols_str = self.get_source_cols_str(self.source_conn, self.source_table, self.dest_column)
            if self.source_table != self.dest_table:
                self.dest_conn.execute(sqlalchemy.sql.text(f"""
                        DROP TABLE IF EXISTS {self.dest_table};

                        CREATE TABLE {self.dest_table} AS
                        SELECT {self.source_table}.{self.source_column} AS {self.dest_column}, {source_cols_str} 
                        FROM {self.source_table};
                    """
                ))
            # if destination table and source table are the same but source and destination columns are different, add destination column to source table
            elif self.source_table == self.dest_table and self.source_column != self.dest_column:
                self.dest_conn.execute(sqlalchemy.sql.text(f"""
                        ALTER TABLE {self.source_table}
                        ADD {self.dest_column} VARCHAR
                    """
                ))
            # else if source table and destination table are the same, and source and destination columns are also the same, do nothing
            # in this case, update_dest_data() should simply update the source/destination column in the source/destination table
        
        self._close_dest_conn()

    # update the destination table (created by create_dest_table()) with the given sanitized data
    def update_dest_data(self, sanitized_data):

        key_dest_update_str = ' AND '.join([f'{self.dest_table}.{key}=mapping_temp.{key}' for key in self.key])

        sql_str = self.generate_mapping_tbl()

        sql_str += f"""
            UPDATE {self.dest_table}
            SET {self.dest_column} = mapping_temp.{self.kind}
            FROM mapping_temp
            WHERE {key_dest_update_str};

            DROP TABLE mapping_temp;
        """
        self._open_dest_conn()
        self.dest_conn.execute(
            sqlalchemy.sql.text(sql_str), sanitized_data_json = json.dumps(sanitized_data)
        )
        self._close_dest_conn()

    # creating .dmp file for specified table in specified database
    def dump_table(self, dump_file_loc):
        command = f'pg_dump -d postgres://{self.source_conn.engine.url.username}:{self.source_conn.engine.url.password}@{self.source_conn.engine.url.host}:{self.source_conn.engine.url.port}/{self.source_conn.engine.url.database} -t public.{self.source_table} -Fc -f {dump_file_loc}'
        p = Popen(command,shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE, encoding='utf8')
        return p.communicate()

    # use .dmp file created by dump_table() to re-create table in another database
    def restore_table(self, dump_file_loc):
        command = f'pg_restore -d postgres://{self.dest_conn.engine.url.username}:{self.dest_conn.engine.url.password}@{self.dest_conn.engine.url.host}:{self.dest_conn.engine.url.port}/{self.dest_conn.engine.url.database} {dump_file_loc}'
        p = Popen(command,shell=True,stdin=PIPE,encoding='utf8')
        return p.communicate()

    # insert given JSON into the logging table in source or dest db
    def insert_log_records(self, log_json):
        insert_query = f"""
            INSERT INTO {self.logging_table}
            SELECT *
            FROM json_populate_recordset(NULL::{self.logging_table}, :log_json);
        """

        if self.logging_db == 'source':
            self._open_source_conn()
            self.source_conn.execute(sqlalchemy.sql.text(insert_query), log_json = json.dumps(log_json))
            self._close_source_conn()
        elif self.logging_db == 'dest':
            self._open_dest_conn()
            self.dest_conn.execute(sqlalchemy.sql.text(insert_query), log_json = json.dumps(log_json))
            self._close_dest_conn()
        else:
            raise Exception('logging_db was not specified as "source" or "dest"')


