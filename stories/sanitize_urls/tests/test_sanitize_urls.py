from url_regex import url_regex
from sanitize_urls import *

def test_get_url_status():
    extra_characters = 'asdf'
    test_valid_url = 'https://w3.org/Addressing/URL/url-spec.txt'
    test_invalid_url = test_valid_url + extra_characters
    nonexistent_url = 'https://nonsenseurlthatdoesntexistz.com/'

    valid_status_code = get_url_status(test_valid_url)
    invalid_status_code = get_url_status(test_invalid_url)
    nonexistent_status_code = get_url_status(nonexistent_url)

    assert valid_status_code == 200
    assert invalid_status_code == 300
    assert nonexistent_status_code == -1

def test_extract_root_url():
    test_url = 'https://w3.org/Addressing/URL/url-spec.txt'

    assert extract_root_url(test_url) == 'https://w3.org'

# Note, should add assertion for invalid URLs when I figure out how to determine invalid root URLs..
def test_assign_url_status():
    test_string_1 = 'https://w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://w3.org/Addressing/URL/url-spec.txtasdf'
    root_url = extract_root_url(test_string_1)

    assert assign_url_status(test_string_1, root_url) == {'URL_status': 200, 'root_URL_status': 200}
    assert assign_url_status(test_string_2, root_url) == {'URL_status': 300, 'root_URL_status': 200}

def test_assign_status():
    test_string_1 = 'https://w3.org/Addressing/URL/url-spec.txt'
    test_string_1_matches = re.findall(url_regex, test_string_1)
    test_string_2 = 'URL: ' + test_string_1
    test_string_2_matches = re.findall(url_regex, test_string_2)
    test_string_3 = 'random_characters'
    test_string_3_matches = re.findall(url_regex, test_string_3)
    test_string_4 = 'https://w3.org/Addressing/URL/url-spec.txt   https://gist.github.com/dperini/729294'
    test_string_4_matches = re.findall(url_regex, test_string_4)

    assert assign_status(test_string_1, test_string_1_matches) == 'Entire string is URL'
    assert assign_status(test_string_2, test_string_2_matches) == 'String contains one URL'
    assert assign_status(test_string_3, test_string_3_matches) == 'String contains no URLs'
    assert assign_status(test_string_4, test_string_4_matches) == 'String contains multiple URLs'


def test_sanitize_urls_with_only_one_url():
    test_string_1 = 'https://w3.org/Addressing/URL/url-spec.txt'
    test_string_2 = 'https://w3.org/Addressing/URL/url-spec.txtasdf'
    test_string_3 = 'URL Specification https://w3.org/Addressing/URL/url-spec.txt'
    test_string_4 = 'URL Specification https://w3.org/Addressing/URL/url-spec.txtasdf'

    assert sanitize_urls(test_string_1) == {'status': 'Entire string is URL', 'URLs': [{'URL': 'https://w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_2) == {'status': 'Entire string is URL', 'URLs': [{'URL': 'https://w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://w3.org', 'URL_status': 300, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_3) == {'status': 'String contains one URL', 'URLs': [{'URL': 'https://w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    assert sanitize_urls(test_string_4) == {'status': 'String contains one URL', 'URLs': [{'URL': 'https://w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://w3.org', 'URL_status': 300, 'root_URL_status': 200}]}

def test_sanitize_urls_with_multiple_URLs():
    test_string = 'https://w3.org/Addressing/URL/url-spec.txtasdf https://gist.github.com/dperini/729294 https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert sanitize_urls(test_string) == {
        'status': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://w3.org/Addressing/URL/url-spec.txtasdf', 'root_URL': 'https://w3.org', 'URL_status': 300, 'root_URL_status': 200},
            {'URL': 'https://gist.github.com/dperini/729294', 'root_URL': 'https://gist.github.com', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python', 'root_URL': 'https://miguendes.me', 'URL_status': 200, 'root_URL_status': 200}
        ]
    }

# INVESTIGATE!!
# interestingly, http://nonsenseurlthatdoesntexistz.com/ still returns a 200 status code...
# it the scheme is https instead of http, the requests.get expectedly errors out
def test_sanitize_urls_with_valid_URL_string_that_does_not_exist():
    test_url = 'https://nonsenseurlthatdoesntexistz.com/'

    assert sanitize_urls(test_url) == []

