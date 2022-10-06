'''
Sanitizes contact info from a database and places the results in another database.
'''
import logging
import sqlalchemy
import json
from enum import Enum
import sys
import argparse
from collections import namedtuple
from typing import List, Dict, Tuple, Union
from collections import OrderedDict
import phonenumbers
from email_validator import validate_email
import re
import pprint

from sanitize_urls import sanitize_urls_parallel, get_sanitized_urls_as_string


this_module = sys.modules[__name__]

def main():
    argparser = argparse.ArgumentParser(description=this_module.__doc__)
    argparser.add_argument('--source-db', type=str, required=True,
        help="e.g. 'postgresql+psycopg2://foo:bar@example.com:42/defaultdb'")
    argparser.add_argument('--dest-db', type=str, default=None)
    argparser.add_argument('--source-dest-mapping', type=str,
        help='''
            A json list of objects to be mapped of the form:
            [
                {
                    "kind": "phone"|"email"|"url",
                    "key": [ string, ...]
                    "source_table": string,
                    "source_column": string,
                    "dest_table": string|null # null implies don't deposit anywhere
                    "dest_column": string|null # null implies don't deposit anywhere
                }
            ]
            
            e.g.
            
            [ 
                {
                    "kind": "email",
                    "key": ["location", "id"],
                    "source_table": "foo",
                    "source_column": "bar",
                    "dest_table": "baz",
                    "dest_column": "bap"
                }
            ]
        ''',
        default = """[
            {
                "kind": "url",
                "key": ["id"],
                "source_table": "service_copy",
                "source_column": "url",
                "dest_table": "",
                "dest_column": ""
            }
        ]
        """
    )
    
    argparser.add_argument('--batch-row-size',  type=positive_int, default=10000)
    argparser.add_argument('--max-rows-to-fetch', type=positive_int, default=int(1e6))
    argparser.add_argument('--write', action='store_true', help="Write the sanitized versions") 
    
    args = argparser.parse_args(sys.argv[1:])
    
    args.source_dest_mapping = [ Src2Dest(**obj) for obj in json.loads(args.source_dest_mapping) ]
        
    logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

    logger = logging.getLogger()
        
    src_db = sqlalchemy.create_engine(args.source_db, echo=True)
    src_conn = src_db.connect()
    dest_db = None
    dest_conn = None
    if args.dest_db:
        dest_db = sqlalchemy.create_engine(args.dest_db, echo=True)
        dest_conn = dest_db.connect()
        
    for s2d in args.source_dest_mapping:
        logger.info(f'Mapping \n{s2d}')
        
        if not len(s2d.key):
            raise Exception(f"keys not defined for table {s2d.source_table}") 
        
        key_select = ", ".join(s2d.key)
        key_sorting_statement = ', '.join(s2d.key)
        if key_sorting_statement:
            key_sorting_statement = 'order by '+key_sorting_statement
        
        for offset in range(0, args.max_rows_to_fetch, args.batch_row_size):
            logger.info(f"Fetching from {offset}")
            print(dict(
                    source_column=s2d.source_column,
                    source_table=s2d.source_table,
                    limit=args.batch_row_size,
                    offset=offset
                ))

            results = src_conn.execute(
                f'''
                    select
                        {s2d.source_column}, {key_select} 
                    from
                        {s2d.source_table}
                    {key_sorting_statement}
                    limit {args.batch_row_size}
                    offset {offset}
                '''
            ).fetchall()    
            
            if not len(results):
                break
            
            if s2d.kind == 'url':
                
                # separating keys from raw urls, which will be sanitized
                key_vals_rows = [result[1:] for result in results]
                raw_urls = [result[0] for result in results]

                # getting sanitized urls with keys
                sanitized_urls = sanitize_urls_parallel(raw_urls)
                sanitized_url_strings = [get_sanitized_urls_as_string(url_json) for url_json in sanitized_urls]
                sanitized_urls_w_keys = [{key:key_val for key, key_val in zip(s2d.key, key_vals)} for key_vals in key_vals_rows]
                for key_vals_dict, clean_url in zip(sanitized_urls_w_keys, sanitized_url_strings):
                    key_vals_dict['url'] = clean_url

                # TO-DO: add flexibility for different data types on key fields (e.g. INTEGER)..
                key_schema = ', '.join([key + ' VARCHAR' for key in s2d.key])
                key_join_str = ' AND '.join([f'{s2d.source_table}.{key}=urls_temp.{key}' for key in s2d.key])
                
                # setting up multiple SQL queries to create a mapping table with the keys and sanitized urls, then using that table to update the source table
                sql_str = f"""
                    DROP TABLE IF EXISTS urls_temp;
                    
                    CREATE TABLE urls_temp (
                        {key_schema}
                        , url VARCHAR
                    );

                    INSERT INTO urls_temp
                    SELECT *
                    FROM json_populate_recordset(NULL::urls_temp, :sanitized_urls_json);
                    """

                if not dest_conn:
                    sql_str += f"""
                        UPDATE {s2d.source_table}
                        SET {s2d.source_column} = urls_temp.url
                        FROM urls_temp
                        WHERE {key_join_str};
                    """
                else:
                    sql_str += f"""
                        DROP TABLE IF EXISTS {s2d.dest_table};

                        CREATE TABLE {s2d.dest_table} AS
                        SELECT urls_temp.url AS {s2d.dest_column}, {s2d.source_table}.* 
                        FROM {s2d.source_table}
                        JOIN urls_temp ON {key_join_str};

                    """
                
                sql_str += f"""
                    DROP TABLE urls_temp;
                """

                src_conn.execute(
                    sqlalchemy.sql.text(sql_str), sanitized_urls_json = json.dumps(sanitized_urls_w_keys)
                )
                

            else:
                for result in results:
                    src = result[0]
                    if not src:
                        continue
                    stripped_src = src.strip()  
                    key_vals = result[1:]
                    sanitized = None
                    if s2d.kind == InfoKind.phone:
                        try:
                            sanitized = phonenumbers.format_number(phonenumbers.parse(stripped_src, 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')
                        except Exception as e:
                            logger.error(
                                f"Unable to parse or find phone number in text '{src}' on table {s2d.source_table}.{s2d.source_column} with key (" +
                                    ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals) ] ) + 
                                ')  exception: {e}')
                        
                    elif s2d.kind == InfoKind.email:
                        try:
                            sanitized = validate_email(stripped_src, check_deliverability=False).email
                        except Exception as e:
                            logger.error(
                                f"Unable to parse or find email in text '{src}' on table {s2d.source_table}.{s2d.source_column} with key (" +
                                    ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals) ] ) + 
                                ')  exception: {e}')
                            
                    else:
                        logger.critical(f"Unknown kind={s2d.kind}")
                    
                    if not sanitized or sanitized == src:
                        continue
                        
                    logger.debug(f"Sanitized '{src}' to '{sanitized}'") 
                    if not args.write or dest_db or not s2d.dest_table or not s2d.dest_column:
                        continue
                
                    src_conn.execute(
                        sqlalchemy.sql.text(
                            f'''
                                update {s2d.dest_table}
                                set
                                    {s2d.dest_column} = :sanitized    
                                where
                            ''' +
                            'and\n  '.join( [f'{col}=:col_{i}__' for i,col in s2d.keys()]) 
                        ),
                        **{f'col_{i}__': v for i,v in enumerate(key_vals)}
                    )
                
    
    
        
class InfoKind(Enum):
    phone = "phone"
    email = "email"
    url = "url"
        
class Src2Dest(object):
    def __init__(self, kind: InfoKind, key: List[str], source_table: str, source_column: str, dest_table: Union[str, None], dest_column: Union[str, None]):
        self.kind = kind
        self.key = key
        self.source_table = source_table
        self.source_column = source_column
        self.dest_table = dest_table
        self.dest_column = dest_column
    def __str__(self):
        return 'Src2Dest(\n  ' + pprint.pformat(self.__dict__) +')'
        
def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue
    

if __name__ == '__main__':
    main()
