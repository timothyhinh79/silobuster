from sanitize_urls import extract_urls, validate_url

def test_validate_url():
    extra_characters = 'asdf'
    test_url = 'https://w3.org/Addressing/URL/url-spec.txt' + extra_characters

    valid_url = validate_url(test_url)

    assert valid_url == 'https://w3.org/Addressing/URL/url-spec.txt'

def test_extract_urls():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt'
    test2 = 'Possible headstart for validation in JS (https://gist.github.com/dperini/729294'
    test3 = 'Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert extract_urls(test1) == ['https://w3.org/Addressing/URL/url-spec.txt']
    assert extract_urls(test2) == ['https://gist.github.com/dperini/729294']
    assert extract_urls(test3) == ['https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python']

def test_extract_urls_with_multiple_URLs():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt'
    test2 = 'Possible headstart for validation in JS (https://gist.github.com/dperini/729294'
    test3 = 'Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert set(extract_urls(test1 + ' ' + test2 + ' ' + test3)) == set([
        'https://w3.org/Addressing/URL/url-spec.txt',
        'https://gist.github.com/dperini/729294',
        'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'])

def test_extract_urls_with_extraneous_characters():
    extra_characters = 'asdf'
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt' + extra_characters
    assert extract_urls(test1) == ['https://w3.org/Addressing/URL/url-spec.txt']

def test_extract_urls_with_multiple_URLs_with_extraneous_characters():
    extra_characters = 'asdf'
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt' + extra_characters
    test2 = 'Possible headstart for validation in JS (https://gist.github.com/dperini/729294' + extra_characters
    test3 = 'Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python' + extra_characters

    assert set(extract_urls(test1 + ' ' + test2 + ' ' + test3)) == set([
        'https://w3.org/Addressing/URL/url-spec.txt',
        'https://gist.github.com/dperini/729294',
        'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'])

# INVESTIGATE!!
# interestingly, http://nonsenseurlthatdoesntexistz.com/ still returns a 200 status code...
def test_extract_urls_with_valid_URL_string_that_does_not_exist():
    test_url = 'http://nonsenseurlthatdoesntexistz.com/'

    assert extract_urls(test_url) == []

