import sqlalchemy
from types import SimpleNamespace
from sanitization_code.url_sanitization.get_sanitized_urls_for_update import get_sanitized_urls_for_update
from classes.src2dest import Src2Dest
import pytest
import logging
import sys
import datetime

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

singlekey_src2dest = Src2Dest(kind = 'url', key = ['id'], 
    source_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source',
    dest_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest',
    logging_db = 'dest', logging_table="logs", source_table = 'data', source_column='url', dest_table='data_dest', dest_column='url',
    job_timestamp=datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
)
multikey_src2dest = Src2Dest(kind = 'url', key = ['id', 'email'], 
    source_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source',
    dest_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest',
    logging_db = 'dest', logging_table="logs", source_table = 'data', source_column='url', dest_table='data_dest', dest_column='url',
    job_timestamp=datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")    
)

args_test = SimpleNamespace(
    source_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source', 
    dest_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest', 
    source_dest_mapping=[singlekey_src2dest],
    batch_row_size=10000, 
    max_rows_to_fetch=1000000, 
    write=False
)

# for testing tables with multiple keys
args_test_mult_keys = SimpleNamespace(
    source_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source', 
    dest_db='postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest', 
    source_dest_mapping=[multikey_src2dest],
    batch_row_size=10000, 
    max_rows_to_fetch=1000000, 
    write=False
)

@pytest.fixture()
def db():
    
    src_db = sqlalchemy.create_engine(args_test.source_db)
    src_conn = src_db.connect()

    # construct mock data table
    sql_str = f"""
        DROP TABLE IF EXISTS data;
        CREATE TABLE data (
            id VARCHAR,
            url VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            contributor VARCHAR
        );

        INSERT INTO data (id, url, email, phone, contributor) VALUES
        ('1', 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'asdfadf@gmail.com', '111 111 1111', 'whatcom'),
        ('2', 'this string has no urls', 'this string has no emails', 'this string has no phone number', 'whatcom'),
        ('3', 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', 'qwerty@yahoo.com', '222-222-2222', 'whatcom'),
        ('4', 'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'asdfadf@gmail.com', '111 111 1111', 'whatcom');

        CREATE TABLE IF NOT EXISTS logs
        (
            id character varying(255) NOT NULL,
            job_id character varying(255),
            iteration_id character varying(255),
            step_name character varying(255),
            contributor_name character varying(255),
            log_message json,
            CONSTRAINT logs_pkey PRIMARY KEY (id)
        );

    """

    src_conn.execute(sql_str)

    dest_db = sqlalchemy.create_engine(args_test.dest_db)
    dest_conn = dest_db.connect()

    dest_conn.execute("""
        CREATE TABLE IF NOT EXISTS logs
        (
            id character varying(255) NOT NULL,
            job_id character varying(255),
            iteration_id character varying(255),
            step_name character varying(255),
            contributor_name character varying(255),
            log_message json,
            CONSTRAINT logs_pkey PRIMARY KEY (id)
        );
    """)

    # closing database
    src_conn.close()
    src_db.dispose()

    yield # yield to test

    # tear down any mock data tables
    src_db = sqlalchemy.create_engine(args_test.source_db)
    src_conn = src_db.connect()
    src_conn.execute("""
        DROP TABLE IF EXISTS data;
        DROP TABLE IF EXISTS mapping_temp;
        DROP TABLE IF EXISTS logs;
    """)
    src_conn.close()
    src_db.dispose()

    dest_db = sqlalchemy.create_engine(args_test.dest_db)
    dest_conn = dest_db.connect()
    dest_conn.execute("""
        DROP TABLE IF EXISTS data_dest;
        DROP TABLE IF EXISTS mapping_temp;
        DROP TABLE IF EXISTS logs;
    """)
    dest_conn.close()
    dest_db.dispose()

def test_get_key_sorting_statement():

    assert singlekey_src2dest.get_key_sorting_statement() == 'order by id'
    assert multikey_src2dest.get_key_sorting_statement() == 'order by id, email'

def test_query_db(db):

    results = singlekey_src2dest.query_db(batch = args_test.batch_row_size, offset = 0)

    assert results == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','1'), 
        ('this string has no urls', 'whatcom','2'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', 'whatcom','3'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','4')
    ]

def test_query_db_w_mult_keys(db):
    # email is being used as second key in addition to id in args_test_mult_keys

    results = multikey_src2dest.query_db(batch = args_test_mult_keys.batch_row_size, offset = 0)

    assert results == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','1', 'asdfadf@gmail.com'), 
        ('this string has no urls', 'whatcom','2', 'this string has no emails'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', 'whatcom','3', 'qwerty@yahoo.com'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','4', 'asdfadf@gmail.com')
    ]

def test_generate_mapping_tbl():

    mapping_tbl_sql = singlekey_src2dest.generate_mapping_tbl()

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

    mapping_tbl_sql = multikey_src2dest.generate_mapping_tbl()

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

    key_vals = ['1','2','3','4']
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]
    contributor_vals = ['whatcom'] * 4

    sanitized_urls, log_records = get_sanitized_urls_for_update(
        raw_urls,
        key_vals,
        contributor_vals,
        singlekey_src2dest,
        logger
    )

    singlekey_src2dest.update_src_data(sanitized_urls)

    updated_data = singlekey_src2dest.query_db(batch = args_test.batch_row_size, offset = 0)

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','1'), 
        ('this string has no urls', 'whatcom','2'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', 'whatcom','3'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','4')
    ]

def test_update_src_data_w_mult_keys(db):
    key_vals = [('1', 'asdfadf@gmail.com'), ('2', 'this string has no emails'), ('3', 'qwerty@yahoo.com'), ('4','asdfadf@gmail.com') ]
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]
    contributor_vals = ['whatcom'] * 4

    sanitized_urls, log_records = get_sanitized_urls_for_update(
        raw_urls,
        key_vals,
        contributor_vals,
        multikey_src2dest,
        logger
    )

    multikey_src2dest.update_src_data(sanitized_urls)

    updated_data = multikey_src2dest.query_db(batch = args_test_mult_keys.batch_row_size, offset = 0)

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','1', 'asdfadf@gmail.com'), 
        ('this string has no urls', 'whatcom','2', 'this string has no emails'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', 'whatcom','3', 'qwerty@yahoo.com'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'whatcom','4', 'asdfadf@gmail.com')
    ]


def test_get_source_cols_str_wo_suffix(db):

    source_cols_str = singlekey_src2dest.get_source_cols_str()

    assert source_cols_str == 'data.id,data.email,data.phone,data.contributor'

def test_get_source_cols_str_w_suffix(db):

    source_cols_str = singlekey_src2dest.get_source_cols_str(table_suffix='_src')

    assert source_cols_str == 'data_src.id,data_src.email,data_src.phone,data_src.contributor'

def test_create_dest_table(db):

    singlekey_src2dest.create_dest_table()

    singlekey_src2dest._open_dest_conn()
    dest_cols = singlekey_src2dest.dest_conn.execute(f"""
        SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{args_test.source_dest_mapping[0].dest_table}'
            ORDER BY ordinal_position;
    """).fetchall()

    assert dest_cols == [('url',), ('id',), ('email',), ('phone',), ('contributor',)]

    dest_tbl = singlekey_src2dest.dest_conn.execute(f"""
        SELECT * FROM data_dest;
    """).fetchall()

    singlekey_src2dest._close_dest_conn()

    # dest_tbl should be the same as the source table, but with dest_column placed first
    assert dest_tbl == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111', 'whatcom'),
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number', 'whatcom'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222', 'whatcom'),
        ('this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111', 'whatcom')        
    ]

def test_update_dest_data(db):
    key_vals = ['1','2','3','4']
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]
    contributor_vals = ['whatcom'] * 4

    sanitized_urls, log_records = get_sanitized_urls_for_update(
        raw_urls,
        key_vals,
        contributor_vals,
        singlekey_src2dest,
        logger
    )

    singlekey_src2dest.create_dest_table()
    singlekey_src2dest.update_dest_data(sanitized_urls)

    singlekey_src2dest._open_dest_conn()
    updated_data = singlekey_src2dest.dest_conn.execute("SELECT * FROM data_dest;").fetchall()
    singlekey_src2dest._close_dest_conn()

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111', 'whatcom'), 
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number', 'whatcom'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222', 'whatcom'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111', 'whatcom')
    ]

def test_update_dest_data_w_mult_keys(db):
    key_vals = [('1', 'asdfadf@gmail.com'), ('2', 'this string has no emails'), ('3', 'qwerty@yahoo.com'), ('4','asdfadf@gmail.com') ]
    raw_urls = [
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program',
        'this string has no urls', 
        'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program https://www.kidsinmotionclinic.org',
        'this part should be removed: https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'
    ]
    contributor_vals = ['whatcom'] * 4

    sanitized_urls, log_records = get_sanitized_urls_for_update(
        raw_urls,
        key_vals,
        contributor_vals,
        multikey_src2dest,
        logger
    )

    multikey_src2dest.create_dest_table()
    multikey_src2dest.update_dest_data(sanitized_urls)

    multikey_src2dest._open_dest_conn()
    updated_data = multikey_src2dest.dest_conn.execute("SELECT * FROM data_dest;").fetchall()
    multikey_src2dest._close_dest_conn()

    assert updated_data == [
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '1', 'asdfadf@gmail.com', '111 111 1111', 'whatcom'), 
        ('this string has no urls', '2', 'this string has no emails', 'this string has no phone number', 'whatcom'), 
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org', '3', 'qwerty@yahoo.com', '222-222-2222', 'whatcom'),
        ('https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', '4', 'asdfadf@gmail.com', '111 111 1111', 'whatcom')
    ]

def test_insert_log_records(db):
    log_json = [
        {
            "id": 'test1', 
            "job_id": 'test1',
            "iteration_id": 1,
            "step_name": "test1",
            "contributor_name": "test1", 
            "log_message": {'test1': 'test1'}
        },
        {
            "id": 'test2', 
            "job_id": 'test2',
            "iteration_id": 2,
            "step_name": "test2",
            "contributor_name": "test2", 
            "log_message": {'test2': 'test2'}
        },
    ]

    singlekey_src2dest.insert_log_records(log_json)

    singlekey_src2dest._open_dest_conn()
    log_records = singlekey_src2dest.dest_conn.execute('SELECT * FROM logs;').fetchall()
    singlekey_src2dest._close_dest_conn()

    assert log_records == [
        (
            'test1', 
            'test1',
            '1', 
            'test1',
            'test1', 
            {'test1': 'test1'}
        ),
        (
            'test2', 
            'test2',
            '2', 
            'test2',
            'test2', 
            {'test2': 'test2'}
        )
    ]