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

def log_url_sanitization_errors(logger, sanitized_url_json, table_row_id_str):
    log_url_sanitization_error(logger, sanitized_url_json['condition'], table_row_id_str)
    log_bad_url_status_codes(logger, sanitized_url_json['URLs'], table_row_id_str)