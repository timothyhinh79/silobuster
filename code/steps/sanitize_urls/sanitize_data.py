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
import pprint

from pg_sanitization import *
from sanitize_urls import get_sanitized_urls
from sanitize_phone_nums import get_sanitized_phone_nums
from sanitize_emails import get_sanitized_emails

this_module = sys.modules[__name__]

def main():
    # defining Bash arguments for sanitize_data command
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
        
    src_db, src_conn = get_src_db(args)
    dest_db, dest_conn = get_dest_db(args)
        
    for s2d in args.source_dest_mapping:
        logger.info(f'Mapping \n{s2d}')
        
        if not len(s2d.key):
            raise Exception(f"keys not defined for table {s2d.source_table}") 
        
        if s2d.kind not in ['url', 'email', 'phone']:
            raise Exception(f"Unknown kind={s2d.kind}")
        
        key_select = ", ".join(s2d.key)
        key_sorting_statement = get_key_sorting_statement(s2d.key)
        
        for offset in range(0, args.max_rows_to_fetch, args.batch_row_size):
            logger.info(f"Fetching from {offset}")
            print(dict(
                    source_column=s2d.source_column,
                    source_table=s2d.source_table,
                    limit=args.batch_row_size,
                    offset=offset
                ))

            results = query_db(
                src_conn, 
                table = s2d.source_table, 
                column = s2d.source_column, 
                key_select = key_select, 
                key_sorting_statement = key_sorting_statement, 
                batch = args.batch_row_size, 
                offset = offset
            )   
            
            if not len(results):
                break

            key_vals = [result[1:] for result in results]
            raw_data = [result[0] for result in results]

            if s2d.kind == 'url': 
                sanitized_data = get_sanitized_urls(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, infokind = s2d.kind, logger = logger)
            elif s2d.kind == 'phone':
                sanitized_data = get_sanitized_phone_nums(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, infokind = s2d.kind, logger = logger)
            elif s2d.kind == 'email':
                sanitized_data = get_sanitized_emails(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, infokind = s2d.kind, logger = logger)

            if not args.write:
                if not dest_conn:
                    update_src_data(
                        src_conn, 
                        source_table = s2d.source_table, 
                        source_column = s2d.source_column, 
                        keys = s2d.key, 
                        sanitized_data = sanitized_data,
                        sanitized_field=s2d.kind
                    )

                else:
                    if offset == 0: 
                        create_dest_table(
                            src_conn, 
                            dest_conn, 
                            source_table = s2d.source_table, 
                            dest_table = s2d.dest_table, 
                            source_column = s2d.source_column,
                            dest_column = s2d.dest_column
                        )
                    update_dest_data(
                        dest_conn, 
                        dest_table = s2d.dest_table, 
                        dest_column = s2d.dest_column, 
                        keys = s2d.key, 
                        sanitized_data = sanitized_data,
                        sanitized_field=s2d.kind
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
