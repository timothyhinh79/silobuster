import re
import requests
from url_regex import url_regex
from urllib.parse import urlparse

# TO-DO's
# look into changing number of retries for requests.get() to speed it up for non-existent URLs

# extracts all valid URLs from given string
def sanitize_urls(string):
    url_strings = re.findall(url_regex, string)
    status = assign_status(string, url_strings)
    
    # extract valid URLs that return a 200 status code out of each matched URL string
    url_output = []
    for url_string in url_strings:
        root_url = extract_root_url(url_string)
        url_status = assign_url_status(url_string, root_url)
        # should root URL be in output?
        url_output.append({'URL': url_string, 'root_URL': root_url})
        url_output[-1].update(url_status)
        
    return {'status': status, 'URLs': url_output}

def assign_status(string, url_strings):
    if url_strings == []: return 'String contains no URLs'

    if url_strings[0] == string:
        return 'Entire string is URL'
    elif len(url_strings) == 1:
        return 'String contains one URL'
    else:
        return 'String contains multiple URLs'

def assign_url_status(url_string, root_url):
    url_status_code = get_url_status(url_string)
    output = {'URL_status': url_status_code}
    if url_status_code == 200:
        root_url_status_code = 200
    else:
        root_url_status_code = get_url_status(root_url)
    output['root_URL_status'] = root_url_status_code
    return output


# test if given url_string is a valid URL that returns a 200 status code
def get_url_status(url_string):
    try:
        status_code = requests.get(url_string).status_code
    except:
        return -1

    return status_code

def extract_root_url(url_string):
    url_parts = urlparse(url_string)
    return url_parts.scheme + '://' + url_parts.netloc