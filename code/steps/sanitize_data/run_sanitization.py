'''
Sanitizes contact info from a database and places the results in another database.
'''
import logging
import json
import sys
import argparse

from sanitization_code.pg_sanitization import *
from sanitization_code.url_sanitization.get_sanitized_urls_for_update import get_sanitized_urls_for_update
from sanitization_code.sanitize_phone_nums import get_sanitized_phone_nums_for_update
from sanitization_code.sanitize_emails import get_sanitized_emails_for_update
from sanitization_code.helper_methods import positive_int
from classes.infokind import InfoKind
from classes.src2dest import Src2Dest

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
        
    src_db, src_conn = get_db_conn(args.source_db)
    dest_db = None
    dest_conn = None
    if args.dest_db:
        dest_db, dest_conn = get_db_conn(args.dest_db)
    
    # for each source-dest-mapping, sanitize data in source table/database and place into dest table/database
    for s2d in args.source_dest_mapping:
        logger.info(f'Mapping \n{s2d}')
        
        if not len(s2d.key):
            raise Exception(f"keys not defined for table {s2d.source_table}") 
        
        if s2d.kind not in [InfoKind.url.value, InfoKind.email.value, InfoKind.phone.value]:
            raise Exception(f"Unknown kind={s2d.kind}")
        
        # sanitizing data in batches defined by batch-row-size
        for offset in range(0, args.max_rows_to_fetch, args.batch_row_size):
            logger.info(f"Fetching from {offset}")
            print(dict(
                    source_column=s2d.source_column,
                    source_table=s2d.source_table,
                    limit=args.batch_row_size,
                    offset=offset
                ))

            # query data to be sanitized with the specified keys
            results = query_db(
                src_conn, 
                table = s2d.source_table, 
                column = s2d.source_column, 
                keys = s2d.key,
                batch = args.batch_row_size, 
                offset = offset
            )   
            
            # if no results, break loop because we have reached the end of the data table
            if not len(results):
                break

            key_vals = [result[1:] for result in results]
            raw_data = [result[0] for result in results]

            # get sanitized data JSONs that contain the IDs and sanitized urls/phone_nums/emails
            # these JSONs will be used to update the raw_data in a SQL UPDATE statement
            if s2d.kind == InfoKind.url.value: 
                sanitized_data = get_sanitized_urls_for_update(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, logger = logger)
            elif s2d.kind == InfoKind.phone.value:
                sanitized_data = get_sanitized_phone_nums_for_update(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, logger = logger)
            elif s2d.kind == InfoKind.email.value:
                sanitized_data = get_sanitized_emails_for_update(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, logger = logger)

            if not args.write:
                # if there is not destination table/database specified, then update raw data in the source table/database with sanitized output
                if not dest_conn:
                    update_src_data(
                        src_conn, 
                        source_table = s2d.source_table, 
                        source_column = s2d.source_column, 
                        keys = s2d.key, 
                        sanitized_data = sanitized_data,
                        sanitized_field=s2d.kind
                    )

                # if there is a destination table/database, then create the specified destination table (replacing table if it already exists)
                # and then update that destination table
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
                

if __name__ == '__main__':
    main()
