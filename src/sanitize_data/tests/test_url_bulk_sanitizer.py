from classes.src2dest import Src2Dest
from sanitization_code.url_sanitization.url_bulk_sanitizer import URL_BulkSanitizer
import datetime
import logging
import sys
import uuid
from classes.infokind import InfoKind

singlekey_src2dest = Src2Dest(kind = 'url', key = ['id'], 
    source_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source',
    dest_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest',
    logging_db = 'dest', logging_table="logs", source_table = 'data', source_column='url', dest_table='data_dest', dest_column='url',
    job_timestamp=datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
)

logging.basicConfig(
                stream = sys.stdout, 
                filemode = "w",
                format = "%(levelname)s %(asctime)s - %(message)s", 
                level = logging.DEBUG)

logger = logging.getLogger()

# for testing, not practical to test if timestamp value is equal to a hard-coded value
# so we remove timestamp from comparisons
def remove_timestamp_from_json(sanitized_url_json):
    return {k:v for k, v in sanitized_url_json.items() if k != 'timestamp'}

def test_sanitize_single_url():
    test_string = 'https://www.w3.org/Addressing/URL/url-spec.txt'

    sanitized_url_json = URL_BulkSanitizer.sanitize_single_url(test_string)

    assert remove_timestamp_from_json(sanitized_url_json) == {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}

def test_combine_sanitized_urls():
    urls = '''  https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program
                https://www.kidsinmotionclinic.org
                https://www.maxhigbee.org
            '''  
    sanitized_urls_json = {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }

    result = URL_BulkSanitizer.combine_sanitized_urls(sanitized_urls_json)

    assert result == 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org, https://www.maxhigbee.org'

def test_construct_json():
    sanitized_url_str = 'https://www.kidsinmotionclinic.org'
    key = ['id']
    key_val = [1]

    combined_json = URL_BulkSanitizer.construct_json(sanitized_url_str, key, key_val)
    assert combined_json == {'id': 1, InfoKind.url.value: sanitized_url_str}

def test_get_sanitized_url_jsons():
    urls = ['https://www.w3.org/Addressing/URL/url-spec.txt'
            ,'https://www.w3.org/Addressing/URL/url-spec.txtasdf' # additional characters that invalidate full URL match
            ,'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt' # extra characters outside of URL
            ,'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf']

    key_vals = [1,2,3,4]
    contributor_vals = ['whatcom'] * 4

    sanitizer = URL_BulkSanitizer(strings = urls,
                                  key_val_rows = key_vals,
                                  contributor_values=contributor_vals,
                                  src2dest=singlekey_src2dest,
                                  logger=logger)

    results = sanitizer.get_sanitized_url_jsons()

    results_wo_timestamps = [remove_timestamp_from_json(json) for json in results]
    
    assert results_wo_timestamps == [
      {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    , {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    , {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    , {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    ]

def test_get_jsons_for_update():
    urls = ['https://www.w3.org/Addressing/URL/url-spec.txt'
            ,'https://www.w3.org/Addressing/URL/url-spec.txtasdf'
            ,'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt' # extra characters outside of URL
            ,'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf'
            ,'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program'] # returns a 404 error

    # {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200}
    key_vals = [(1,),(2,),(3,),(4,),(5,)]
    contributor_vals = ['whatcom'] * 5

    sanitizer = URL_BulkSanitizer(strings = urls,
                                  key_val_rows = key_vals,
                                  contributor_values=contributor_vals,
                                  src2dest=singlekey_src2dest,
                                  logger=logger)

    rows, logs = sanitizer.get_jsons_for_update()

    assert rows == [
        {
            'id': 3, 
            InfoKind.url.value: 'https://www.w3.org/Addressing/URL/url-spec.txt'
        },
        {
            'id': 4, 
            InfoKind.url.value: 'https://www.w3.org/Addressing/URL/url-spec.txtasdf'
        }
    ]

    assert [log['log_message']['link_id'] for log in logs] == [3,4,5]