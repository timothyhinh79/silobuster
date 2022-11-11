# determine messaging for different url status codes
def report_url_status(url, url_status, url_type):
    if url_status == -1:
        message = f"{url_type} URL {url} is invalid"
    
    elif url_status > 400:
        message = f"{url_type} URL {url} returns {url_status}"

    else:
        message = f"{url_type} URL {url} is valid"

    return message 

# create single message to summarize sanitization result
def create_message(sanitized_url_json, prompts):
    message = sanitized_url_json['condition']
    raw_string = sanitized_url_json['raw_string']
    sanitized_string = ', '.join([url_dict['URL'] for url_dict in sanitized_url_json['URLs']])
    if sanitized_string != raw_string:
        message += f'\nSanitized "{raw_string}" to "{sanitized_string}"'

    if len(prompts) > 0:
        message += f'\nInvalid or bad URLs found. Please review.'

    return message

# determine what status codes are valid
# NOTE: may want to classify 403s and 406s as valid
def valid_status(status_code):
    if status_code == -1 or status_code >= 400:
        return False
    return True

# construct prompt for log_message based on status code of a specific URL (and its root URL)
def url_prompt(url_status_json):
    if not valid_status(url_status_json['URL_status']) and valid_status(url_status_json['root_URL_status']):
        return {
            'description': f"Full URL {url_status_json['URL']} is not valid, but root URL {url_status_json['root_URL']} is valid.",
            'suggested_value' : url_status_json['root_URL']
        }

    elif not valid_status(url_status_json['URL_status']) and not valid_status(url_status_json['root_URL_status']):
        return {
            'description': f"Neither full URL {url_status_json['URL']} or root URL {url_status_json['root_URL']} are valid. Please double-check URL."
        }

    else:
        return None

# create prompts for each URL in a string
def create_url_prompts(sanitized_url_json):
    prompts = []
    for url_status_json in sanitized_url_json['URLs']:
        prompt =  url_prompt(url_status_json)
        if prompt: prompts.append(prompt)
    return prompts

# determine if all URLs found in a string are valid
def pass_or_fail(sanitized_url_json):
    all_urls_valid = all([valid_status(url['URL_status']) for url in sanitized_url_json['URLs']])
    url_string_valid = sanitized_url_json['condition'] == 'String is URL'
    return all_urls_valid and url_string_valid

# create log message identifying table, row, field, with prompts to suggest corrections for any errors
def create_log_message(sanitized_url_json, keys, key_vals, source_db, source_table, source_col):
    url_prompts = create_url_prompts(sanitized_url_json)
    message = create_message(sanitized_url_json, url_prompts)
    prompts = {'description': message, 'suggested_value': ''} | url_prompts

    json = {
        # "id": , # is there a need for id if there's only one log message per row in the log table?
        "link_entity": f"{source_db}.{source_table}", #?
        "link_id": {key:key_val for key, key_val in zip(keys, key_vals)},
        "link_column": source_col,
        "prompts": prompts, # JSON array
    }
    return json

### LOGGER METHODS FOR CONSOLE MESSAGING/DEBUGGING

# create a log record with the job/iteration ID and the log_message
def create_log_record(sanitized_url_json, keys, key_vals, source_db, source_table, source_col):
    log_message = create_log_message(sanitized_url_json, keys, key_vals, source_db, source_table, source_col)
    return {
        "id": "uuid", # if log table has id has SERIAL, it should automatically increment on its own? or do we want to hash ID based on table/row id and time of sanitization?
        "job_id": "string", # how do we get job_id? could be generated automatically in separate task run at beginning of DAG, and then it would be passed as an argument to command to run dockerized container?
        "iteration_id": "string", # how do we get iteration number? would developer pass an iteration # somewhere?
        "step_name": "sanitize_url",
        "contributor_name": "string", # how do we get the contributor? query it from table (assuming it has contributor column)?
        "log_message": log_message
    }

# log if entire string consists of one URL
def log_url_sanitization_error(logger, condition, table_row_id_str):
    if condition != 'String is URL':
        error_msg = f"{condition} in {table_row_id_str}"
        logger.error(error_msg)

# log any URLs that returned a bad status code
def log_bad_url_status_codes(logger, urls_w_status, table_row_id_str):
    for url in urls_w_status:
        if url['URL_status'] == -1:
            logger.error(f"Invalid URL ({url['URL']}) found in {table_row_id_str})")
        elif url['URL_status'] > 400:
            logger.error(f"URL ({url['URL']}) returns {url['URL_status']} response in {table_row_id_str})")

# log instances where sanitization changed the original string
def log_sanitization_change(logger, raw_url, sanitized_url_str, table_row_id_str):
    sanitization_msg = f"Sanitized '{raw_url}' to '{sanitized_url_str}' in {table_row_id_str}"
    logger.debug(sanitization_msg) 

# retrieve the table, row and id where sanitization is being applied
def get_error_location_str_for_logging(source_table, source_column, keys, key_vals):
    key_value_pairs_str = ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals) ] ) 
    error_location_str = f"{source_table}.{source_column} with key ({key_value_pairs_str})"
    return error_location_str

# log any URL sanitization errors
def log_url_sanitization_errors(logger, sanitized_url_json, table_row_id_str):
    log_url_sanitization_error(logger, sanitized_url_json['condition'], table_row_id_str)
    log_bad_url_status_codes(logger, sanitized_url_json['URLs'], table_row_id_str)