import re
import requests
from url_regex import url_regex

# extracts all valid URLs from given string
# valid URLs satisfy the below conditions:
    # they fit the regular expression (defined in url_regex.py) for identifying URLs
    # they return a valid 200 status_code response
def extract_urls(string):
    url_strings = re.findall(url_regex, string)
    
    # extract valid URLs that return a 200 status code out of each matched URL string
    valid_urls = []
    for url_string in url_strings:
        valid_url = validate_url(url_string)
        if valid_url: valid_urls.append(valid_url)

    return valid_urls

# test if given url_string is a valid URL that returns a 200 status code
# if not, remove characters at the end of the string until we have a valid URL 
def validate_url(url_string):
    status_code = requests.get(url_string).status_code

    while re.match(url_regex, url_string) and status_code != 200:
        url_string = url_string[:-1]
        status_code = requests.get(url_string).status_code
    
    if status_code == 200:
        return url_string
    else:
        return None