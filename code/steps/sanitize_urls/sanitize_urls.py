import re
import requests
from urllib.parse import urlparse
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

from url_regex import url_regex

def sanitize_urls_parallel(strings, num_threads):
    """Runs sanitize_urls() on each string in a given list in a parallelized procedure
    
    Parameters:
        strings (list): List of raw strings with URL(s) 
        num_threads (int): number of workers to run sanitize_urls simultaneously 
            (e.g. if num_threads = 100, this method will process 100 strings at a time)

    Returns:
        List of JSONs returned for each string by the sanitize_urls() method 
    """

    n_threads = min(num_threads, len(strings))
    executor = ThreadPoolExecutor(n_threads)
    
    # establishing parallel sessions/processes to run the sanitize_urls method, assigned to an index number to keep track of the ordering of each result
    futures = {executor.submit(sanitize_urls, string):index for index, string in zip(range(len(strings)), strings)}

    responses = [] # responses are appended more or less randomly depending on when the corresponding session completes
    indices = [] # used to keep track of original ordering of each response
    for future in as_completed(futures):
        responses.append(future.result())
        indices.append(futures[future])

    # sorting the responses based on the original ordering of the corresponding strings
    return [response for _, response in sorted(zip(indices, responses))]
    

def sanitize_urls(string):
    """Extract URLs from given string (using Regex) and logs each URL's status code
    
    Parameters:
        string (str): Raw string with URL(s) 

    Returns:
        json_output (dict): JSON-like object with following information:
            'status' (str): indicates if string contains URLs
            'URLs' (list): nested-JSON with following attributes on each URL:
                'URL' (str): full URL match
                'root_URL' (str): root URL within the matched URL
                'URL_status' (int): status code (e.g. 200, 404, etc.)
                'root_URL_status' (int): status code of root URL (e.g. 200, 404, etc.)
    """

    url_strings = re.findall(url_regex, string)
    status = assign_status(string, url_strings)
    
    # construct JSON with each URL regex match, root URL, and status codes
    url_output = []
    for url_string in url_strings:
        root_url = extract_root_url(url_string)
        url_status = assign_url_status(url_string, root_url)
        url_output.append({'URL': url_string, 'root_URL': root_url})
        url_output[-1].update(url_status)
        
    json_output = {'status': status, 'URLs': url_output}
    return json_output

def assign_status(string, url_strings):
    """Assigns appropriate status to given string based on URL Regex matches 
    
    Parameters:
        string (str): Raw string with URL(s) 
        url_strings (list): List of URL Regex matches

    Returns:
        String description indicating if the provided string contains 1 or more URLs
    """
    if url_strings == []: return 'String contains no URLs'

    if url_strings[0] == string:
        return 'Entire string is URL'
    elif len(url_strings) == 1:
        return 'String contains one URL'
    else:
        return 'String contains multiple URLs'


def get_url_status(url_string):
    """Retrieves status code for the given URL
    
    Parameters:
        url_string (str): URL

    Returns:
        status_code (int): status code of given URL; -1 if URL does not exist
    """

    try:
        status_code = requests.head(url_string, timeout = 5).status_code
    except:
        return -1

    return status_code

def assign_url_status(url_string, root_url):
    """Retrieves status codes for the given URL and the embedded root URL
    
    Parameters:
        url_string (str): URL
        root_url (str): root URL found within url_string

    Returns:
        output (dict): Dictionary containing the following:
            'URL_status': status code of full URL
            'root_URL_status': status code of root URL within full URL
    """

    url_status_code = get_url_status(url_string)
    output = {'URL_status': url_status_code}

    # if full URL returns 200, then we assume root URL returns 200
    if url_status_code == 200:
        root_url_status_code = 200
    else:
        root_url_status_code = get_url_status(root_url)
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

