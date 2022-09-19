import re
import requests
from url_regex import url_regex

# extracts all valid URLs from given string
# valid URLs satisfy the below conditions:
    # they fit the regular expression (defined in url_regex.py) for identifying URLs
    # they return a valid 200 status_code response
def extract_url(string):
    urls = re.findall(url_regex, string)
    for i in range(len(urls)):
        while re.match(url_regex, urls[i]) and requests.get(urls[i]).status_code != 200:
            urls[i] = urls[i][:-1]
    return urls



