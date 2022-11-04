import requests # used to validate if URLs exist
from urllib.parse import urlparse # used to extract root URLs

import re # Regex used to identify valid URL patterns
from sanitization_code.url_sanitization.url_regex import url_regex # used to identify valid URL strings
import time # to allow subsequent validation attempts on URLs returning 429 or 503

def sanitize_url(string, timeout, retry_after, max_attempts):
    """Extract URLs from given string (using Regex) and logs each URL's status code
    
    Parameters:
        string (str): Raw string with URL(s) 

    Returns:
        json_output (dict): JSON-like object with following information:
            raw_string (str): raw string to be sanitized
            'condition' (str): indicates if string contains URLs
            'URLs' (list): nested-JSON with following attributes on each URL:
                'URL' (str): full URL match
                'root_URL' (str): root URL within the matched URL
                'URL_status' (int): status code (e.g. 200, 404, etc.)
                'root_URL_status' (int): status code of root URL (e.g. 200, 404, etc.)
    """

    url_strings = re.findall(url_regex, string)
    condition = assign_string_condition(string, url_strings)
    
    # construct JSON with each URL regex match, root URL, and status codes
    url_output = []
    for url_string in url_strings:
        root_url = extract_root_url(url_string)
        url_status = assign_url_status(url_string, root_url, timeout, retry_after, max_attempts)
        url_output.append({'URL': url_string, 'root_URL': root_url})
        url_output[-1].update(url_status)
        
    json_output = {'raw_string': string, 'condition': condition, 'URLs': url_output}
    return json_output

def assign_string_condition(string, url_strings):
    """Assigns appropriate condition to given string based on URL Regex matches 
    
    Parameters:
        string (str): Raw string with URL(s) 
        url_strings (list): List of URL Regex matches

    Returns:
        String description indicating if the provided string contains 1 or more URLs
    """
    if url_strings == []: return 'String contains no URLs'

    if url_strings[0] == string:
        return 'String is URL'
    elif len(url_strings) == 1:
        return 'String is not URL but contains one'
    else:
        return 'String contains multiple URLs'


def get_url_status(url_string, timeout, retry_after, max_attempts):
    """Retrieves status code for the given URL, re-attempts validation of URLs returning 429, 503
    
    Parameters:
        url_string (str): URL

    Returns:
        status_code (int): status code of given URL; -1 if URL does not exist
    """

    try:
        status_code = requests.get(url_string, timeout = timeout).status_code
        num_attempts = 1
        while status_code in (429,503) and num_attempts < max_attempts:
            num_attempts += 1
            time.sleep(retry_after)
            status_code = requests.get(url_string, timeout = timeout).status_code
    except:
        return -1

    return status_code

def assign_url_status(url_string, root_url, timeout, retry_after, max_attempts):
    """Retrieves status codes for the given URL and the embedded root URL
    
    Parameters:
        url_string (str): URL
        root_url (str): root URL found within url_string

    Returns:
        output (dict): Dictionary containing the following:
            'URL_status': status code of full URL
            'root_URL_status': status code of root URL within full URL
    """

    url_status_code = get_url_status(url_string, timeout, retry_after, max_attempts)
    output = {'URL_status': url_status_code}

    # if full URL returns 200, then we assume root URL returns 200
    if url_status_code == 200:
        root_url_status_code = 200
    else:
        root_url_status_code = get_url_status(root_url, timeout, retry_after, max_attempts)
    output['root_URL_status'] = root_url_status_code

    return output

def extract_root_url(url_string):
    """Extracts root URL from given URL string
    
    Parameters:
        url_string (str): URL

    Returns:
        string representing root URL within given string
    """

    url_parts = urlparse(url_string)
    return url_parts.scheme + '://' + url_parts.netloc

