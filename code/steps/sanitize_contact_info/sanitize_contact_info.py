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


this_module = sys.modules[__name__]

def main():
    argparser = argparse.ArgumentParser(description=this_module.__doc__)
    argparser.add_argument('--source-db', type=str,
        help="e.g. 'postgresql+psycopg2://foo:bar@example.com:42/defaultdb'")
    argparser.add_argument('--dest-db', type=str, default=None)
    argparser.add_argument('--source-dest-mapping', type=str,
        help='''
            A json list of objects to be mapped of the form:
            [
                {
                    "kind": "phone"|"email",
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
        default=[
            Src2Dest(InfoKind.phone, ["id"], "phone", "phone_number", None, None),
            Src2Dest(InfoKind.email, ["id"], "service", "email", None, None)
        ]
    )
    
    argparser.add_argument('--batch-row-size',  type=positive_int, default=10000)
    argparser.add_argument('--max-rows-to-fetch', type=positive_int, default=int(1e6))
    argparser.add_argument('--write', action='store_true', help="Write the sanitized versions") 
    
    args = argparser.parse_args(sys.argv[1:])
    
    if type(args.source_dest_mapping) == str:
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
        logger.info(f'Mapping {s2d}')
        
        if not len(s2d.key):
            raise Exception(f"keys not defined for table {s2d.source_table}") 
        
        key_select = ", ".join(s2d.key)
        key_sorting_statement = ', '.join(s2d.key)
        if key_sorting_statement:
            key_sorting_statement = 'order by '+key_sorting_statement
        
        for offset in range(0, args.batch_row_size, args.max_rows_to_fetch):
            logger.info(f"Fetching from {offset}")
            print(dict(
                    source_column=s2d.source_column,
                    source_table=s2d.source_table,
                    limit=args.batch_row_size,
                    offset=offset
                ))
            for result in src_conn.execute(
                f'''
                    select
                        {s2d.source_column}, {key_select} 
                    from
                        {s2d.source_table}
                    {key_sorting_statement}
                    limit {args.batch_row_size}
                    offset {offset}
                '''
            ).fetchall():
                src = result[0]
                if not src:
                    continue
                stripped_src = src.strip()  
                key_vals = result[1:]
                sanitized = None
                if s2d.kind == InfoKind.phone:
                    try:
                        sanitized = phonenumbers.format_number(phonenumbers.parse(stripped_src, 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')
                    except:
                        logger.error(
                            f"Unable to parse or find phone number in text '{src}' on table {s2d.source_table}.{s2d.source_column} with key (" +
                                ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals) ] ) + 
                            ')')
                    
                elif s2d.kind == InfoKind.email:
                    try:
                        sanitized = validate_email(stripped_src, check_deliverability=False).email
                    except:
                        logger.error(
                            f"Unable to parse or find email in text '{src}' on table {s2d.source_table}.{s2d.source_column} with key (" +
                                ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals) ] ) + 
                            ')')
                        
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
        
class Src2Dest(object):
    def __init__(self, kind: InfoKind, key: List[str], source_table: str, source_column: str, dest_table: Union[str, None], dest_column: Union[str, None]):
        self.kind = kind
        self.key = key
        self.source_table = source_table
        self.source_column = source_column
        self.dest_table = dest_table
        self.dest_column = dest_column
        
def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue
    

if __name__ == '__main__':
    main()
