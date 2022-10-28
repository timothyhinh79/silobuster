
from sanitization_code.url_sanitization.parallelize_url_sanitization import sanitize_urls_parallel
from sanitization_code.url_sanitization.url_sanitization_logging import *

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