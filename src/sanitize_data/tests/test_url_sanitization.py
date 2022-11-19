import pandas as pd
import time
import logging
import sys

from sanitization_code.url_sanitization.url_regex import url_regex
from sanitization_code.url_sanitization.sanitize_url import *
from sanitization_code.url_sanitization.url_sanitization_params import num_threads_default, requests_timeout_default, retry_after_default, max_attempts_default
from sanitization_code.url_sanitization.parallelize_url_sanitization import *
from sanitization_code.url_sanitization.get_sanitized_urls_for_update import *
from classes.src2dest import Src2Dest
from classes.infokind import InfoKind

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

# Potential Issue to investigate:
# interestingly, http://nonsenseurlthatdoesntexistz.com/ still returns a 200 status code...
# but if the scheme is https instead of http, it expectedly errors out

# for testing, not practical to test if timestamp value is equal to a hard-coded value
# so we remove timestamp from comparisons
def remove_timestamp_from_json(sanitized_url_json):
    return {k:v for k, v in sanitized_url_json.items() if k != 'timestamp'}

def test_get_url_status():
    extra_characters = 'asdf'
    test_valid_url = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_invalid_url = test_valid_url + extra_characters
    nonexistent_url = 'https://nonsenseurlthatdoesntexistz.com/'

    valid_status_code = get_url_status(test_valid_url, requests_timeout_default, retry_after_default, max_attempts_default)
    invalid_status_code = get_url_status(test_invalid_url, requests_timeout_default, retry_after_default, max_attempts_default)
    nonexistent_status_code = get_url_status(nonexistent_url, requests_timeout_default, retry_after_default, max_attempts_default)

    assert valid_status_code == 200
    assert invalid_status_code == 300
    assert nonexistent_status_code == -1

def test_extract_root_url():
    test_url = 'https://www.w3.org/Addressing/URL/url-spec.txt'

    assert extract_root_url(test_url) == 'https://www.w3.org'

def test_assign_url_status():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf'
    test_string_3 = 'https://nonsenseurlthatdoesntexistz.com/'
    root_url_1_2 = extract_root_url(test_string_1)
    root_url_3 = extract_root_url(test_string_3)

    assert assign_url_status(test_string_1, root_url_1_2, requests_timeout_default, retry_after_default, max_attempts_default) == {'URL_status': 200, 'root_URL_status': 200}
    assert assign_url_status(test_string_2, root_url_1_2, requests_timeout_default, retry_after_default, max_attempts_default) == {'URL_status': 300, 'root_URL_status': 200}
    assert assign_url_status(test_string_3, root_url_3, requests_timeout_default, retry_after_default, max_attempts_default) == {'URL_status': -1, 'root_URL_status': -1}

def test_assign_string_condition():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_1_matches = re.findall(url_regex, test_string_1)
    test_string_2 = 'URL: ' + test_string_1
    test_string_2_matches = re.findall(url_regex, test_string_2)
    test_string_3 = 'random_characters'
    test_string_3_matches = re.findall(url_regex, test_string_3)
    test_string_4 = 'https://www.w3.org/Addressing/URL/url-spec.txt   https://gist.github.com/dperini/729294'
    test_string_4_matches = re.findall(url_regex, test_string_4)

    assert assign_string_condition(test_string_1, test_string_1_matches) == 'String is URL'
    assert assign_string_condition(test_string_2, test_string_2_matches) == 'String is not URL but contains one'
    assert assign_string_condition(test_string_3, test_string_3_matches) == 'String contains no URLs'
    assert assign_string_condition(test_string_4, test_string_4_matches) == 'String contains multiple URLs'

def test_sanitize_url_with_no_urls():
    test_string = ''

    sanitized_url_json = sanitize_url(test_string, requests_timeout_default, retry_after_default, max_attempts_default)
    assert 'timestamp' in sanitized_url_json
    assert remove_timestamp_from_json(sanitized_url_json) == {'raw_string': '', 'condition': 'String contains no URLs', 'URLs': []}

def test_sanitize_url_with_only_one_url():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf' # additional characters that invalidate full URL match
    test_string_3 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt' # extra characters outside of URL
    test_string_4 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf'

    assert remove_timestamp_from_json(sanitize_url(test_string_1, requests_timeout_default, retry_after_default, max_attempts_default)) == {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitize_url(test_string_2, requests_timeout_default, retry_after_default, max_attempts_default))== {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitize_url(test_string_3, requests_timeout_default, retry_after_default, max_attempts_default)) == {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitize_url(test_string_4, requests_timeout_default, retry_after_default, max_attempts_default)) == {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}

def test_sanitize_url_with_multiple_URLs():
    test_string = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf https://gist.github.com/dperini/729294 https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert remove_timestamp_from_json(sanitize_url(test_string, requests_timeout_default, retry_after_default, max_attempts_default)) == {
        'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf https://gist.github.com/dperini/729294 https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python',
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200},
            {'URL': 'https://gist.github.com/dperini/729294', 'root_URL': 'https://gist.github.com', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python', 'root_URL': 'https://miguendes.me', 'URL_status': 200, 'root_URL_status': 200}
        ]
    }

def test_sample_urls_from_within_service(): 
    urls = '''  https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program
                https://www.kidsinmotionclinic.org
                https://www.maxhigbee.org
            '''  

    assert remove_timestamp_from_json(sanitize_url(urls, requests_timeout_default, retry_after_default, max_attempts_default)) == {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }

def test_speed_of_sanitize_urls_parallel_with_all_urls_from_within_service_csv():
    within_service_df = pd.read_csv('tests/data_for_testing/within_service.csv')
    urls = within_service_df['url'].tolist() # 139 url strings
    urls = [url if type(url) == str else '' for url in urls] # substituting empty string for NaNs

    start = time.time()
    output = sanitize_urls_parallel(urls, 200)
    end = time.time()

    elapsed_time = start - end
    print(f'Time to process {len(urls)} urls: {elapsed_time} seconds')
    assert len(output) == len(urls)
    assert elapsed_time < 60

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
    
    urls_string = combine_sanitized_urls(sanitized_urls_json)

    assert urls_string == 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org, https://www.maxhigbee.org'

def test_get_sanitized_urls_for_update():
    urls = [
        'https://www.kidsinmotionclinic.org',
        'https://www.kidsinmotionclinic.org' + ' this part should be removed'
    ]

    sanitized_urls_json, log_records = get_sanitized_urls_for_update(
        raw_urls = urls,
        key_vals_rows = ['1','2'],
        contributor_vals=['whatcom'] * 2,
        src2dest = singlekey_src2dest,
        logger = logger
    )

    # only the unclean url should be sanitized and included in sanitized_urls_json
    assert sanitized_urls_json == [{
        'id': '2',
        InfoKind.url.value: 'https://www.kidsinmotionclinic.org'
    }]
    
    assert len(log_records) == 1
    assert log_records[0]['step_name'] == 'sanitize_url'


