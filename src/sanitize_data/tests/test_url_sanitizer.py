import datetime
import logging
import sys
import re

from sanitization_code.url_sanitization.url_regex import url_regex
from classes.src2dest import Src2Dest

from sanitization_code.url_sanitization.url_sanitizer import URL_Sanitizer

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

    valid_status_code = URL_Sanitizer.get_url_status(test_valid_url)
    invalid_status_code = URL_Sanitizer.get_url_status(test_invalid_url)
    nonexistent_status_code = URL_Sanitizer.get_url_status(nonexistent_url)

    assert valid_status_code == 200
    assert invalid_status_code == 300
    assert nonexistent_status_code == -1

def test_extract_root_url():
    test_url = 'https://www.w3.org/Addressing/URL/url-spec.txt'

    assert URL_Sanitizer.extract_root_url(test_url) == 'https://www.w3.org'

def test_assign_url_status():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf'
    test_string_3 = 'https://nonsenseurlthatdoesntexistz.com/'
    root_url_1_2 = URL_Sanitizer.extract_root_url(test_string_1)
    root_url_3 = URL_Sanitizer.extract_root_url(test_string_3)

    assert URL_Sanitizer.assign_url_status(test_string_1, root_url_1_2) == {'URL_status': 200, 'root_URL_status': 200}
    assert URL_Sanitizer.assign_url_status(test_string_2, root_url_1_2) == {'URL_status': 300, 'root_URL_status': 200}
    assert URL_Sanitizer.assign_url_status(test_string_3, root_url_3) == {'URL_status': -1, 'root_URL_status': -1}

def test_assign_string_condition():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_1_matches = re.findall(url_regex, test_string_1)
    test_string_2 = 'URL: ' + test_string_1
    test_string_2_matches = re.findall(url_regex, test_string_2)
    test_string_3 = 'random_characters'
    test_string_3_matches = re.findall(url_regex, test_string_3)
    test_string_4 = 'https://www.w3.org/Addressing/URL/url-spec.txt   https://gist.github.com/dperini/729294'
    test_string_4_matches = re.findall(url_regex, test_string_4)

    assert URL_Sanitizer.assign_string_condition(test_string_1, test_string_1_matches) == 'String is URL'
    assert URL_Sanitizer.assign_string_condition(test_string_2, test_string_2_matches) == 'String is not URL but contains one'
    assert URL_Sanitizer.assign_string_condition(test_string_3, test_string_3_matches) == 'String contains no URLs'
    assert URL_Sanitizer.assign_string_condition(test_string_4, test_string_4_matches) == 'String contains multiple URLs'

def test_sanitize_url_with_no_urls():
    test_string = ''

    sanitizer = URL_Sanitizer(test_string)
    sanitized_url_json = sanitizer.sanitize_url()
    assert 'timestamp' in sanitized_url_json
    assert remove_timestamp_from_json(sanitized_url_json) == {'raw_string': '', 'condition': 'String contains no URLs', 'URLs': []}

def test_sanitize_url_with_only_one_url():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf' # additional characters that invalidate full URL match
    test_string_3 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt' # extra characters outside of URL
    test_string_4 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf'

    sanitizer_1 = URL_Sanitizer(test_string_1)
    sanitizer_2 = URL_Sanitizer(test_string_2)
    sanitizer_3 = URL_Sanitizer(test_string_3)
    sanitizer_4 = URL_Sanitizer(test_string_4)

    assert remove_timestamp_from_json(sanitizer_1.sanitize_url()) == {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitizer_2.sanitize_url())== {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitizer_3.sanitize_url()) == {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert remove_timestamp_from_json(sanitizer_4.sanitize_url()) == {'raw_string': 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}

def test_sanitize_url_with_multiple_URLs():
    test_string = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf https://gist.github.com/dperini/729294 https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    sanitizer = URL_Sanitizer(test_string)
    assert remove_timestamp_from_json(sanitizer.sanitize_url()) == {
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

    sanitizer = URL_Sanitizer(urls)
    assert remove_timestamp_from_json(sanitizer.sanitize_url()) == {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }


