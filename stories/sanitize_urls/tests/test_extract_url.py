from sanitize_urls import extract_url

def test_extract_url():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt'
    test2 = 'Possible headstart for validation in JS (https://gist.github.com/dperini/729294'
    test3 = 'Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert extract_url(test1) == ['https://w3.org/Addressing/URL/url-spec.txt']
    assert extract_url(test2) == ['https://gist.github.com/dperini/729294']
    assert extract_url(test3) == ['https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python']

def test_extract_url_with_multiple_URLs():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txt'
    test2 = ' Possible headstart for validation in JS (https://gist.github.com/dperini/729294'
    test3 = ' Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'

    assert set(extract_url(test1 + test2 + test3)) == set([
        'https://w3.org/Addressing/URL/url-spec.txt',
        'https://gist.github.com/dperini/729294',
        'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'])

def test_extract_url_with_extraneous_characters():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txtadfasdfasdfasdf)'
    assert extract_url(test1) == ['https://w3.org/Addressing/URL/url-spec.txt']

def test_extract_url_with_multiple_URLs_with_extraneous_characters():
    test1 = 'URL Specification (https://w3.org/Addressing/URL/url-spec.txtasdfasdf'
    test2 = ' Possible headstart for validation in JS (https://gist.github.com/dperini/729294asdfasdf'
    test3 = ' Implements the above in Python (https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-pythonasdfasdf'

    assert set(extract_url(test1 + test2 + test3)) == set([
        'https://w3.org/Addressing/URL/url-spec.txt',
        'https://gist.github.com/dperini/729294',
        'https://miguendes.me/how-to-check-if-a-string-is-a-valid-url-in-python'])

