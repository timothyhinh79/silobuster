from url_regex import url_regex
import pandas as pd
from sanitize_urls import *
import time
import numpy as np

def test_get_url_status():
    extra_characters = 'asdf'
    test_valid_url = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_invalid_url = test_valid_url + extra_characters
    nonexistent_url = 'https://nonsenseurlthatdoesntexistz.com/'

    valid_status_code = get_url_status(test_valid_url)
    invalid_status_code = get_url_status(test_invalid_url)
    nonexistent_status_code = get_url_status(nonexistent_url)

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

    assert assign_url_status(test_string_1, root_url_1_2) == {'URL_status': 200, 'root_URL_status': 200}
    assert assign_url_status(test_string_2, root_url_1_2) == {'URL_status': 300, 'root_URL_status': 200}
    assert assign_url_status(test_string_3, root_url_3) == {'URL_status': -1, 'root_URL_status': -1}

def test_assign_status():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_1_matches = re.findall(url_regex, test_string_1)
    test_string_2 = 'URL: ' + test_string_1
    test_string_2_matches = re.findall(url_regex, test_string_2)
    test_string_3 = 'random_characters'
    test_string_3_matches = re.findall(url_regex, test_string_3)
    test_string_4 = 'https://www.w3.org/Addressing/URL/url-spec.txt   https://gist.github.com/dperini/729294'
    test_string_4_matches = re.findall(url_regex, test_string_4)

    assert assign_status(test_string_1, test_string_1_matches) == 'Entire string is URL'
    assert assign_status(test_string_2, test_string_2_matches) == 'String contains one URL'
    assert assign_status(test_string_3, test_string_3_matches) == 'String contains no URLs'
    assert assign_status(test_string_4, test_string_4_matches) == 'String contains multiple URLs'

def test_sanitize_urls_with_no_urls():
    test_string = ''

    assert sanitize_urls(test_string) == {'status': 'String contains no URLs', 'URLs': []}

def test_sanitize_urls_with_only_one_url():
    test_string_1 = 'https://www.w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf' # additional characters that invalidate full URL match
    test_string_3 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txt' # extra characters outside of URL
    test_string_4 = 'URL Specification https://www.w3.org/Addressing/URL/url-spec.txtasdf'

    assert sanitize_urls(test_string_1) == {'status': 'Entire string is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_2) == {'status': 'Entire string is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_3) == {'status': 'String contains one URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_4) == {'status': 'String contains one URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://www.w3.org', 'URL_status': 300, 'root_URL_status': 200}]}

def test_sanitize_urls_with_multiple_URLs():
    test_string = 'https://www.w3.org/Addressing/URL/url-spec.txtasdf https://gist.github.com/dperini/729294 https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert sanitize_urls(test_string) == {
        'status': 'String contains multiple URLs', 
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
                https://www.blainefoodbank.org
                https://www.facesnorthwest.com
                http://squalicum.bellinghamschools.org
                https://sehome.bellinghamschools.org
                https://www.nv.k12.wa.us/site/default.aspx?PageType=3&DomainID=1&ModuleInstanceID=591&ViewID=047E6BE3-6D87-4130-8424-D8E4E9ED6C2A&RenderLoc=0&FlexDataID=3866&PageID=1
                http://www.co.whatcom.wa.us/360/Health-Department
                http://www.whatcomcounty.us/1570/Nurse-Family-Partnership-NFP
            '''  

    assert sanitize_urls(urls) == {'status': 'String contains multiple URLs', 'URLs': [
        {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
        {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 301, 'root_URL_status': 301},
        {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1},
        {'URL': 'https://www.blainefoodbank.org', 'root_URL': 'https://www.blainefoodbank.org', 'URL_status': 200, 'root_URL_status': 200},
        {'URL': 'https://www.facesnorthwest.com', 'root_URL': 'https://www.facesnorthwest.com', 'URL_status': -1, 'root_URL_status': -1},
        {'URL': 'http://squalicum.bellinghamschools.org', 'root_URL': 'http://squalicum.bellinghamschools.org', 'URL_status': 403, 'root_URL_status': 403},
        {'URL': 'https://sehome.bellinghamschools.org', 'root_URL': 'https://sehome.bellinghamschools.org', 'URL_status': 403, 'root_URL_status': 403},
        {'URL': 'https://www.nv.k12.wa.us/site/default.aspx?PageType=3&DomainID=1&ModuleInstanceID=591&ViewID=047E6BE3-6D87-4130-8424-D8E4E9ED6C2A&RenderLoc=0&FlexDataID=3866&PageID=1', 'root_URL': 'https://www.nv.k12.wa.us', 'URL_status': 301, 'root_URL_status': 200},
        {'URL': 'http://www.co.whatcom.wa.us/360/Health-Department', 'root_URL': 'http://www.co.whatcom.wa.us', 'URL_status': 302, 'root_URL_status': 302},
        {'URL': 'http://www.whatcomcounty.us/1570/Nurse-Family-Partnership-NFP', 'root_URL': 'http://www.whatcomcounty.us', 'URL_status': 302, 'root_URL_status': 302}
    ] }

def test_sanitize_urls_parallel_with_all_urls_from_within_service_csv():
    within_service_df = pd.read_csv('../../../source_data/within_reach_csv/data/within_service.csv')
    urls = within_service_df['url'].tolist() # 139 url strings
    urls = [url if type(url) == str else '' for url in urls] # substituting empty string for NaNs

    start = time.time()
    output = sanitize_urls_parallel(urls, 200)
    end = time.time()

    elapsed_time = start - end
    print(f'Time to process {len(urls)} urls: {elapsed_time} seconds')
    assert len(output) == len(urls)
    assert elapsed_time < 30


# INVESTIGATE!!
# interestingly, http://nonsenseurlthatdoesntexistz.com/ still returns a 200 status code...
# it the scheme is https instead of http, the requests.get expectedly errors out
# def test_sanitize_urls_with_valid_URL_string_that_does_not_exist():
#     test_url = 'https://nonsenseurlthatdoesntexistz.com/'

#     assert sanitize_urls(test_url) == []

