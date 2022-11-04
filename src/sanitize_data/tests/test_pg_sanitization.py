from types import SimpleNamespace
from sanitization_code.url_sanitization.get_sanitized_urls_for_update import get_sanitized_urls_for_update
from classes.src2dest import Src2Dest
from classes.infokind import InfoKind
from sanitization_code.pg_sanitization import *
import pytest
import logging
import sys

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

args_test = SimpleNamespace(
    source_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source', 
    dest_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest', 
    source_dest_mapping=[Src2Dest('url', ['id'], 'data', 'url', 'data_dest', 'url')],
    batch_row_size=10000, 
    max_rows_to_fetch=1000000, 
    write=False
)

# for testing tables with multiple keys
args_test_mult_keys = SimpleNamespace(
    source_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source', 
    dest_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest', 
    source_dest_mapping=[Src2Dest('url', ['id', 'email'], 'data', 'url', 'data_dest', 'url')],
    batch_row_size=10000, 
    max_rows_to_fetch=1000000, 
    write=False
)

@pytest.fixture()
def db():
    src_db, src_conn = get_db_conn(args_test.source_db)

    # construct mock data table
    sql_str = f"""
        DROP TABLE IF EXISTS data;
        CREATE TABLE data (
            id VARCHAR,
            url VARCHAR,
            email VARCHAR,
            phone VARCHAR
        );

        INSERT INTO data (id, url, email, phone) VALUES
        ('1', 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'asdfadf@gmail.com', '111 111 1111'),
        ('2', 'this string has no urls', 'this string has no emails', 'this string has no phone number'),
        ('3', 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', 'qwerty@yahoo.com', '222-222-2222'),
        ('4', 'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'asdfadf@gmail.com', '111 111 1111');

    """

    src_conn.execute(sql_str)
    close_db(src_db, src_conn)

    yield # yield to test

    # tear down any mock data tables
    src_db, src_conn = get_db_conn(args_test.source_db)
    src_conn.execute("""
        DROP TABLE IF EXISTS data;
        DROP TABLE IF EXISTS mapping_temp;
    """)
    close_db(src_db, src_conn)

    dest_db, dest_conn = get_db_conn(args_test.dest_db)
    dest_conn.execute("""
        DROP TABLE IF EXISTS data_dest;
        DROP TABLE IF EXISTS mapping_temp;
    """)
    close_db(dest_db, dest_conn)

def test_get_key_sorting_statement():
    keys = ['id1', 'id2']
    key_sorting_statement = get_key_sorting_statement(keys)

    assert key_sorting_statement == 'order by id1, id2'


def test_query_db(db):
    src_db, src_conn = get_db_conn(args_test.source_db)

    results = query_db(
        src_conn = src_conn,
        table = 'data',
        column = 'url',
        keys = args_test.source_dest_mapping[0].key,
        batch = args_test.batch_row_size,
        offset= 0
    )

    assert results == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1'), 
        ('this string has no urls', '2'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', '3'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4')
    ]

def test_query_db_w_mult_keys(db):
    # email is being used as second key in addition to id in args_test_mult_keys
    src_db, src_conn = get_db_conn(args_test_mult_keys.source_db)

    results = query_db(
        src_conn = src_conn,
        table = 'data',
        column = 'url',
        keys = args_test_mult_keys.source_dest_mapping[0].key,
        batch = args_test_mult_keys.batch_row_size,
        offset= 0
    )

    assert results == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com'), 
        ('this string has no urls', '2', 'this string has no emails'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com')
    ]

def test_generate_mapping_tbl():
    keys = args_test.source_dest_mapping[0].key
    key_schema = ', '.join([key + ' VARCHAR' for key in keys])

    mapping_tbl_sql = generate_mapping_tbl(key_schema, InfoKind.url.value)

    assert mapping_tbl_sql == f"""
        DROP TABLE IF EXISTS mapping_temp;
        
        CREATE TABLE mapping_temp (
            id VARCHAR
            , url VARCHAR
        );

        INSERT INTO mapping_temp
        SELECT *
        FROM json_populate_recordset(NULL::mapping_temp, :sanitized_data_json);
    """

def test_generate_mapping_tbl_w_mult_keys():
    keys = args_test_mult_keys.source_dest_mapping[0].key
    key_schema = ', '.join([key + ' VARCHAR' for key in keys])

    mapping_tbl_sql = generate_mapping_tbl(key_schema, InfoKind.url.value)

    assert mapping_tbl_sql == f"""
        DROP TABLE IF EXISTS mapping_temp;
        
        CREATE TABLE mapping_temp (
            id VARCHAR, email VARCHAR
            , url VARCHAR
        );

        INSERT INTO mapping_temp
        SELECT *
        FROM json_populate_recordset(NULL::mapping_temp, :sanitized_data_json);
    """

def test_update_src_data(db):
    src_db, src_conn = get_db_conn(args_test.source_db)
    key_vals = ['1','2','3','4']
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]

    sanitized_urls = get_sanitized_urls_for_update(
        raw_urls,
        args_test.source_dest_mapping[0].key,
        key_vals,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].source_column,
        logger
    )

    update_src_data(
        src_conn, 
        args_test.source_dest_mapping[0].source_table, 
        args_test.source_dest_mapping[0].source_column, 
        args_test.source_dest_mapping[0].key, 
        sanitized_urls, 
        InfoKind.url.value
    )

    updated_data = query_db(
        src_conn, 
        args_test.source_dest_mapping[0].source_table, 
        args_test.source_dest_mapping[0].source_column, 
        args_test.source_dest_mapping[0].key,
        args_test.batch_row_size,
        offset = 0
    )

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1'), 
        ('this string has no urls', '2'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4')
    ]

def test_update_src_data_w_mult_keys(db):
    src_db, src_conn = get_db_conn(args_test_mult_keys.source_db)
    key_vals = [('1', 'asdfadf@gmail.com'), ('2', 'this string has no emails'), ('3', 'qwerty@yahoo.com'), ('4','asdfadf@gmail.com') ]
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]

    sanitized_urls = get_sanitized_urls_for_update(
        raw_urls,
        args_test_mult_keys.source_dest_mapping[0].key,
        key_vals,
        args_test_mult_keys.source_dest_mapping[0].source_table,
        args_test_mult_keys.source_dest_mapping[0].source_column,
        logger
    )

    update_src_data(
        src_conn, 
        args_test_mult_keys.source_dest_mapping[0].source_table, 
        args_test_mult_keys.source_dest_mapping[0].source_column, 
        args_test_mult_keys.source_dest_mapping[0].key, 
        sanitized_urls, 
        InfoKind.url.value
    )

    updated_data = query_db(
        src_conn, 
        args_test_mult_keys.source_dest_mapping[0].source_table, 
        args_test_mult_keys.source_dest_mapping[0].source_column, 
        args_test_mult_keys.source_dest_mapping[0].key,
        args_test_mult_keys.batch_row_size,
        offset = 0
    )

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com'), 
        ('this string has no urls', '2', 'this string has no emails'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com')
    ]


def test_get_source_cols_str_wo_suffix(db):
    src_db, src_conn = get_db_conn(args_test.source_db)
    source_cols_str = get_source_cols_str(
        src_conn,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].dest_column
    )

    assert source_cols_str == 'data.id,data.email,data.phone'

def test_get_source_cols_str_w_suffix(db):
    src_db, src_conn = get_db_conn(args_test.source_db)
    source_cols_str = get_source_cols_str(
        src_conn,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].dest_column,
        table_suffix = '_src'
    )

    assert source_cols_str == 'data_src.id,data_src.email,data_src.phone'

def test_create_dest_table(db):
    src_db, src_conn = get_db_conn(args_test.source_db)
    dest_db, dest_conn = get_db_conn(args_test.dest_db)

    create_dest_table(
        src_conn, 
        dest_conn,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].dest_table,
        args_test.source_dest_mapping[0].source_column,
        args_test.source_dest_mapping[0].dest_column,
    )

    dest_cols = dest_conn.execute(f"""
        SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{args_test.source_dest_mapping[0].dest_table}'
            ORDER BY ordinal_position;
    """).fetchall()

    assert dest_cols == [('url',), ('id',), ('email',), ('phone',)]

    dest_tbl = dest_conn.execute(f"""
        SELECT * FROM data_dest;
    """).fetchall()

    # dest_tbl should be the same as the source table, but with dest_column placed first
    assert dest_tbl == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111'),
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111')        
    ]

def test_update_dest_data(db):
    src_db, src_conn = get_db_conn(args_test.source_db)
    dest_db, dest_conn = get_db_conn(args_test.dest_db)
    key_vals = ['1','2','3','4']
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]

    sanitized_urls = get_sanitized_urls_for_update(
        raw_urls,
        args_test.source_dest_mapping[0].key,
        key_vals,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].source_column,
        logger
    )

    create_dest_table(
        src_conn, 
        dest_conn,
        args_test.source_dest_mapping[0].source_table,
        args_test.source_dest_mapping[0].dest_table,
        args_test.source_dest_mapping[0].source_column,
        args_test.source_dest_mapping[0].dest_column,
    )

    update_dest_data(
        dest_conn, 
        args_test.source_dest_mapping[0].dest_table, 
        args_test.source_dest_mapping[0].dest_column, 
        args_test.source_dest_mapping[0].key, 
        sanitized_urls, 
        sanitized_field = InfoKind.url.value
    )

    updated_data = dest_conn.execute("SELECT * FROM data_dest;").fetchall()

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111'), 
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111')
    ]

def test_update_dest_data_w_mult_keys(db):
    src_db, src_conn = get_db_conn(args_test_mult_keys.source_db)
    dest_db, dest_conn = get_db_conn(args_test_mult_keys.dest_db)
    key_vals = [('1', 'asdfadf@gmail.com'), ('2', 'this string has no emails'), ('3', 'qwerty@yahoo.com'), ('4','asdfadf@gmail.com') ]
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]

    sanitized_urls = get_sanitized_urls_for_update(
        raw_urls,
        args_test_mult_keys.source_dest_mapping[0].key,
        key_vals,
        args_test_mult_keys.source_dest_mapping[0].source_table,
        args_test_mult_keys.source_dest_mapping[0].source_column,
        logger
    )

    create_dest_table(
        src_conn, 
        dest_conn,
        args_test_mult_keys.source_dest_mapping[0].source_table,
        args_test_mult_keys.source_dest_mapping[0].dest_table,
        args_test_mult_keys.source_dest_mapping[0].source_column,
        args_test_mult_keys.source_dest_mapping[0].dest_column,
    )

    update_dest_data(
        dest_conn, 
        args_test_mult_keys.source_dest_mapping[0].dest_table, 
        args_test_mult_keys.source_dest_mapping[0].dest_column, 
        args_test_mult_keys.source_dest_mapping[0].key, 
        sanitized_urls, 
        sanitized_field = InfoKind.url.value
    )

    updated_data = dest_conn.execute("SELECT * FROM data_dest;").fetchall()

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111'), 
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111')
    ]
