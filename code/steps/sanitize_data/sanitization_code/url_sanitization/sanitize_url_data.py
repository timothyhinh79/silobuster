from sanitization_code.url_sanitization.parallelize_url_sanitization import sanitize_urls_parallel
from sanitization_code.url_sanitization.url_sanitization_logging import *

def get_sanitized_urls_for_update(raw_urls, keys, key_vals, source_table, source_column, logger):
    """Sanitizes the given raw_urls, and returns a JSON with the given keys and sanitized URLs
       This JSON is used to generate the mapping table in Postgres for updating the raw URLs
       Also logs certain events for sub-optimal situations where the given string is not a valid URL
    
    Parameters:
        raw_urls (list): array of raw URL strings from table to sanitize
        keys (list): array of column names for the keys (e.g. ID)
        key_vals (list): array of tuples, each one have the values of each key field for a specific record
        source_table (str): name of source table containing un-sanitized data
        source_column (str): name of column in source table with the un-sanitized data
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
        new_row = get_row_w_sanitized_url(sanitized_url_json, keys, key_vals_tuple, table_row_id_str, logger)
        if new_row: rows_w_sanitized_url.append(new_row)

        # log errors in URL sanitization
        log_url_sanitization_errors(logger, sanitized_url_json, table_row_id_str)

    return rows_w_sanitized_url

# if sanitization actually changed the raw stirng, get JSON with sanitized URL and key values, 
# output JSON will be inserted into a mapping table in Postgres
def get_row_w_sanitized_url(sanitized_url_json, keys, key_vals, table_row_id_str, logger):
    raw_url = sanitized_url_json['raw_string']
    sanitized_url_str = combine_sanitized_urls(sanitized_url_json)
    if sanitized_url_str != raw_url:
        log_sanitization_change(logger, raw_url, sanitized_url_str, table_row_id_str)
        return get_sanitized_url_w_keys(sanitized_url_str, keys, key_vals)
    else:
        return None  

def get_sanitized_url_w_keys(sanitized_url_str, keys, key_vals):
    sanitized_url_json = {'URL': sanitized_url_str}
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    combined_json = keys_json | sanitized_url_json
    return combined_json

# if multiple URLs in string, combine them into one string separated by a comma
def combine_sanitized_urls(sanitized_urls_json):
    if not sanitized_urls_json['URLs']:
        return sanitized_urls_json['raw_string']

    clean_urls = [url['URL'] for url in sanitized_urls_json['URLs']]
    return ', '.join(clean_urls)