import requests # used to validate if URLs exist
from urllib.parse import urlparse # used to extract root URLs
# as_completed and ThreadPoolExecutor used to run sanitize_urls() method on multiple strings in parallel for efficiency
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
import re # Regex used to identify valid URL patterns
from url_regex import url_regex # used to identify valid URL strings
import time # to allow subsequent validation attempts on URLs returning 429 or 503

from helper_methods import jsonify

num_threads_default = 200 # number of threads to run sanitize URLs in parallel
requests_timeout_default = 20 # allowing 1 seconds for requests.get() to validate if a given URL exists
retry_after_default = 10 # of seconds to reattempt validation of URLs that initially return 429 or 503
max_attempts_default = 3 # of maximum attempts to retrieve a valid status code for URLs initially returnning 429 or 503

def get_sanitized_urls(raw_urls, keys, key_vals, source_table, source_column, infokind, logger):
    """Sanitizes the given raw_urls, and returns a JSON with the given keys and sanitized URLs
       Also logs certain events for sub-optimal situations where the given string is not a valid URL
    
    Parameters:
        raw_urls (list): array of raw URL strings from table to sanitize
        keys (list): array of column names for the keys (e.g. ID)
        key_vals (list): array of tuples, each one have the values of each key field for a specific record
        source_table (str): name of source table containing un-sanitized data
        source_column (str): name of column in source table with the un-sanitized data
        infokind (str): one of [phone, email, url]
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 

    Returns:
        sanitized_url_jsons (dict): JSONs summarizing key values and sanitized URLs for each record
    """
    sanitized_urls = sanitize_urls_parallel(raw_urls)
    sanitized_url_jsons = []
    for key_vals_tuple, raw_url, sanitized_url in zip(key_vals, raw_urls, sanitized_urls):

        error_location_str = f"{source_table}.{source_column} with key ({', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) })"

        if sanitized_url['condition'] != 'String is URL':
            logger.error(f"{sanitized_url['condition']} in {error_location_str}")

        for url in sanitized_url['URLs']:
            if url['URL_status'] == -1:
                logger.error(f"Invalid URL ({url['URL']}) found in {error_location_str})")
            elif url['URL_status'] > 400:
                logger.error(f"URL ({url['URL']}) returns {url['URL_status']} response in {error_location_str})")

        sanitized_url_str = get_sanitized_urls_as_string(sanitized_url)

        if sanitized_url_str != raw_url:
            logger.debug(f"Sanitized '{raw_url}' to '{sanitized_url_str}'") 
            sanitized_url_jsons.append({key:key_val for key, key_val in zip(keys, key_vals_tuple)} | {infokind: sanitized_url_str})

    return sanitized_url_jsons


def get_sanitized_urls_as_string(sanitized_urls_json):
    """Extracts the sanitized URL string from JSON output of sanitize_urls_parallel(); 
        if there are multiple URLs in a record, this method combines them into a single string separated by a comma
    
    Parameters:
        sanitized_urls_json (dict): JSONs summarizing the URLs and root URLs found in each record

    Returns:
        single string with the sanitized URL, or multiple URLs separated by a comma
    """
    if not sanitized_urls_json['URLs']:
        return sanitized_urls_json['raw_string']

    clean_urls = [url['URL'] for url in sanitized_urls_json['URLs']]
    return ', '.join(clean_urls)

def sanitize_urls_parallel(strings, num_threads = num_threads_default, timeout = requests_timeout_default, retry_after = retry_after_default, max_attempts = max_attempts_default):
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
    futures = {executor.submit(sanitize_urls, string, timeout, retry_after, max_attempts):index for index, string in zip(range(len(strings)), strings)}

    responses = [] # responses are appended more or less randomly depending on when the corresponding session completes
    indices = [] # used to keep track of original ordering of each response
    for future in as_completed(futures):
        responses.append(future.result())
        indices.append(futures[future])

    # sorting the responses based on the original ordering of the corresponding strings
    return [response for _, response in sorted(zip(indices, responses))]
    

def sanitize_urls(string, timeout, retry_after, max_attempts):
    """Extract URLs from given string (using Regex) and logs each URL's status code
    
    Parameters:
        string (str): Raw string with URL(s) 

    Returns:
        json_output (dict): JSON-like object with following information:
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

