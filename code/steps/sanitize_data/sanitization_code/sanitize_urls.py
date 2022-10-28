import requests # used to validate if URLs exist
from urllib.parse import urlparse # used to extract root URLs
# as_completed and ThreadPoolExecutor used to run sanitize_urls() method on multiple strings in parallel for efficiency
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
import re # Regex used to identify valid URL patterns
from sanitization_code.url_regex import url_regex # used to identify valid URL strings
import time # to allow subsequent validation attempts on URLs returning 429 or 503

num_threads_default = 200 # number of threads to run sanitize URLs in parallel
requests_timeout_default = 20 # allowing 1 seconds for requests.get() to validate if a given URL exists
retry_after_default = 10 # of seconds to reattempt validation of URLs that initially return 429 or 503
max_attempts_default = 3 # of maximum attempts to retrieve a valid status code for URLs initially returnning 429 or 503


def get_sanitized_urls_for_update(raw_urls, keys, key_vals, source_table, source_column, infokind, logger):
    """Sanitizes the given raw_urls, and returns a JSON with the given keys and sanitized URLs
       This JSON is used to generate the mapping table in Postgres for updating the raw URLs
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
    sanitized_url_jsons = sanitize_urls_parallel(raw_urls)
    
    rows_w_sanitized_url = []
    for key_vals_tuple, sanitized_url_json in zip(key_vals, sanitized_url_jsons):

        # for logging
        table_row_id_str = get_error_location_str_for_logging(source_table, source_column, keys, key_vals_tuple)

        # add row if sanitization changed raw URL string, and log the change
        raw_url = sanitized_url_json['raw_string']
        sanitized_url_str = combine_sanitized_urls(sanitized_url_json)
        if sanitized_url_str != raw_url:
            rows_w_sanitized_url.append(get_row_w_sanitized_url(sanitized_url_str, keys, key_vals_tuple, infokind))
            log_sanitization_change(logger, raw_url, sanitized_url_str, table_row_id_str)

        # log errors in URL sanitization
        log_url_sanitization_errors(logger, sanitized_url_json, table_row_id_str)

    return rows_w_sanitized_url

def get_row_w_sanitized_url(sanitized_url_str, keys, key_vals, infokind):
    sanitized_url_json = {infokind: sanitized_url_str}
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    combined_json = keys_json | sanitized_url_json
    return combined_json

def log_url_sanitization_errors(logger, sanitized_url_json, table_row_id_str):
    log_url_sanitization_error(logger, sanitized_url_json['condition'], table_row_id_str)
    log_bad_url_status_codes(logger, sanitized_url_json['URLs'], table_row_id_str)  

def log_url_sanitization_error(logger, condition, table_row_id_str):
    error_msg = f"{condition} in {table_row_id_str}"
    logger.error(error_msg)

def log_bad_url_status_codes(logger, urls_w_status, table_row_id_str):
    for url in urls_w_status:
        if url['URL_status'] == -1:
            logger.error(f"Invalid URL ({url['URL']}) found in {table_row_id_str})")
        elif url['URL_status'] > 400:
            logger.error(f"URL ({url['URL']}) returns {url['URL_status']} response in {table_row_id_str})")

def log_sanitization_change(logger, raw_url, sanitized_url_str, table_row_id_str):
    sanitization_msg = f"Sanitized '{raw_url}' to '{sanitized_url_str}' in {table_row_id_str}"
    logger.debug(sanitization_msg) 

def get_error_location_str_for_logging(source_table, source_column, keys, key_vals):
    key_value_pairs_str = ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals) ] ) 
    error_location_str = f"{source_table}.{source_column} with key ({key_value_pairs_str})"
    return error_location_str

def combine_sanitized_urls(sanitized_urls_json):
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

    executor = setup_executor(strings, num_threads)  
    sessions = run_executor(executor, strings, timeout, retry_after, max_attempts)
    sanitized_urls = get_results(sessions)
    return sanitized_urls
    
def setup_executor(strings, num_threads):
    n_threads = min(num_threads, len(strings))
    executor = ThreadPoolExecutor(n_threads)
    return executor

def run_executor(executor, strings, timeout, retry_after, max_attempts):
    sessions = {}

    # establishing parallel sessions/processes to run the sanitize_urls method, assigned to an index number to keep track of the ordering of each result
    for index, string in enumerate(strings):
        session = executor.submit(sanitize_urls, string, timeout, retry_after, max_attempts)
        sessions[session] = index

    return sessions

def get_results(sessions):
    responses = [] # responses are appended more or less randomly depending on when the corresponding session completes
    indices = [] # used to keep track of original ordering of each response
    for session in as_completed(sessions):
        responses.append(session.result())
        indices.append(sessions[session])

    # sorting the responses based on the original ordering of the corresponding strings
    return [response for _, response in sorted(zip(indices, responses))] 


def sanitize_urls(string, timeout, retry_after, max_attempts):
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

