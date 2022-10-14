from sanitize_data import *


src_conn = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/defaultdb')
dest_conn = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/defaultdb')

create_dest_table(
    src_conn, 
    dest_conn, 
    source_table = 'service_copy_raw', 
    dest_table = 'service_copy_dest', 
    dest_column = 'url'
)

