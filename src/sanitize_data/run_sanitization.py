'''
Sanitizes contact info from a database and places the results in another database.
'''
import logging
import json
import sys
import argparse
import datetime

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
                    "key": [ string, ...],
                    "source_table": string,
                    "source_column": string,
                    "dest_table": string|null, # null implies don't deposit anywhere
                    "dest_column": string|null, # null implies don't deposit anywhere
                    "logging_db": "source"|"dest",
                    "logging_table": string
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
                    "dest_column": "bap",
                    "logging_db": "source",
                    "logging_table": "logs"
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
                "dest_column": "",
                "logging_db": "source",
                "logging_table": "logs"
            }
        ]
        """
    )
    
    argparser.add_argument('--batch-row-size',  type=positive_int, default=10000)
    argparser.add_argument('--max-rows-to-fetch', type=positive_int, default=int(1e6))
    argparser.add_argument('--write', action='store_true', help="Write the sanitized versions") 
    
    args = argparser.parse_args(sys.argv[1:])
    
    # constructing src2dest instances which will hold necessary metadata for applying the sanitization logic to data in Postgres and creating log records
    src2dest_objs = []
    for src2dest_map in json.loads(args.source_dest_mapping):
        src2dest_dict = src2dest_map | {'source_conn_str': args.source_db, 'dest_conn_str': args.dest_db, 'job_timestamp': datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")} 
        src2dest_objs.append(Src2Dest(**src2dest_dict))
        
    logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

    logger = logging.getLogger()
    
    # for each source-dest-mapping, sanitize data in source table/database and place into dest table/database
    for s2d in src2dest_objs:
        
        logger.info(f'Mapping \n{s2d}')
        
        # error handling for arguments passed into run_sanitization
        if not len(s2d.key):
            raise Exception(f"keys not defined for table {s2d.source_table}") 
        
        if s2d.kind not in [InfoKind.url.value, InfoKind.email.value, InfoKind.phone.value]:
            raise Exception(f"Unknown kind={s2d.kind}")

        if s2d.logging_db not in ["source", "dest"]:
            raise Exception('logging_db was not specified as "source" or "dest"')
        
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
            results = s2d.query_db(batch = args.batch_row_size, offset = offset)
            
            # if no results, break loop because we have reached the end of the data table
            if not len(results):
                break

            key_vals = [result[2:] for result in results]
            contributor_vals = [result[1] for result in results]
            raw_data = [result[0] for result in results]

            # get sanitized data JSONs that contain the IDs and sanitized urls/phone_nums/emails
            # these JSONs will be used to update the raw_data in a SQL UPDATE statement
            if s2d.kind == InfoKind.url.value: 
                sanitized_data, log_records = get_sanitized_urls_for_update(raw_data, key_vals_rows = key_vals, contributor_vals = contributor_vals, src2dest=s2d, logger = logger)
                s2d.insert_log_records(log_records)
            elif s2d.kind == InfoKind.phone.value:
                sanitized_data = get_sanitized_phone_nums_for_update(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, logger = logger)
            elif s2d.kind == InfoKind.email.value:
                sanitized_data = get_sanitized_emails_for_update(raw_data, keys = s2d.key, key_vals = key_vals, source_table = s2d.source_table, source_column = s2d.source_column, logger = logger)

            if not args.write:
                # if there is no destination table/database specified, then update raw data in the source table/database with sanitized output
                if not args.dest_db:
                    s2d.update_src_data(sanitized_data)

                # if there is a destination table/database, then create the specified destination table (replacing table if it already exists)
                # and then update that destination table
                else:
                    if offset == 0: 
                        s2d.create_dest_table()
                    s2d.update_dest_data(sanitized_data)

                

if __name__ == '__main__':
    main()
