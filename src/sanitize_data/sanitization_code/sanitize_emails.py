import re
from email_validator import validate_email
from classes.infokind import InfoKind
import uuid

email_regex = "[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"

def get_sanitized_emails_for_update(raw_emails, contributor_vals, key_vals, s2d, logger):
    """Sanitizes the given raw_emails, and returns a JSON with the given keys and sanitized emails
       This JSON is used to generate the mapping table in Postgres for updating the raw emails
       Also logs cases when no valid email is found in the raw string
    
    Parameters:
        raw_emails (list): array of raw email strings from table to sanitize
        keys (list): array of column names for the keys (e.g. ID)
        key_vals (list): array of tuples, each one have the values of each key field for a specific record
        source_table (str): name of source table containing un-sanitized data
        source_column (str): name of column in source table with the un-sanitized data
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 

    Returns:
        rows_w_sanitized_emails (dict): JSONs summarizing key values and sanitized emails for each record
    """
    
    rows_w_sanitized_emails = []
    log_records = []
    for key_vals_tuple, raw_email, contributor in zip(key_vals, raw_emails, contributor_vals):
        
        # for logging
        table_row_id_str = f"{s2d.source_table}.{s2d.source_column} with key (" + ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals_tuple) ] ) 

        # sanitize email and log any errors or changes resulting from sanitization
        found_emails, sanitized_email = sanitize_email(raw_email, email_regex)

        # create log record (in JSON format) and output messages to console
        log_records.append(generate_log_json(raw_email, found_emails, sanitized_email, key_vals_tuple, s2d, contributor, logger))

        # add row if sanitization changed raw URL string
        if sanitized_email != raw_email:
            row_w_sanitized_email = get_row_w_sanitized_email(sanitized_email, s2d.key, key_vals_tuple)
            rows_w_sanitized_emails.append(row_w_sanitized_email)
            
    return rows_w_sanitized_emails, log_records


def sanitize_email(raw_email_str, regex):
    """Extracts valid emails from given string, and logs any errors or changes. 
       Combines multiple emails into one string separated by commas.

    Parameters:
        raw_email_str (str): raw email string from data table
        regex (str): pattern for identifying email addresses
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 
        table_row_id_str (str): string identifying which row in what table is being sanitized

    Returns: list of found emails matching regex pattern, and a string combining all found emails
    """
    sanitized_emails = re.findall(regex, raw_email_str)
    if not sanitized_emails: 
        sanitized_email_str = raw_email_str
    else: 
        sanitized_email_str = ', '.join(sanitized_emails)

    return sanitized_emails, sanitized_email_str

# get JSON with key values and sanitized email, to be inserted as a row into table on Postgres
def get_row_w_sanitized_email(sanitized_email, keys, key_vals):
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    sanitized_email_json = {InfoKind.email.value: sanitized_email}
    combined_json = keys_json | sanitized_email_json
    return combined_json

# log sanitization errors in console, and generate prompts for log record
def log_email_sanitization_errors(raw_email_str, sanitized_emails, logger, table_row_id_str = ''):
    prompts = []
    # if no email patterns were found, generate corresponding prompt for log record and output msg to console
    if not sanitized_emails: 
        message = f"Unable to find email in text '{raw_email_str}'"
        prompts += [{'description': message}]
        logger.error(message + f" in {table_row_id_str}")

    # for each email regex match, validate if email is appropriate using email_validator
    for email_string in sanitized_emails:
        try:
            validate_email(email_string.strip(), check_deliverability=False).email
        except:
            message = f"Invalid email address '{email_string}'"
            prompts += [{'description': message}]
            logger.error(message + f" found in {table_row_id_str}")
    
    return prompts


# if sanitization changed raw string, log it and generate prompt for log record
def log_sanitization_change(raw_email_str, sanitized_email_str, logger, table_row_id_str = ''):
    prompt = []
    if raw_email_str != sanitized_email_str:
        sanitization_msg = f"Sanitized '{raw_email_str}' to '{sanitized_email_str}'"
        prompt += [{'description': sanitization_msg}]
        logger.debug(sanitization_msg + f" in {table_row_id_str}") 

    return prompt

# generate log message JSON for log record, and also outputs msgs to console
def generate_log_message(raw_email, found_emails, sanitized_email, key_vals_tuple, logger, s2d):
    table_row_id_str = f"{s2d.source_table}.{s2d.source_column} with key (" + ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals_tuple) ] ) 
    prompts = [] # first, generate prompts for log message
    prompts += log_email_sanitization_errors(raw_email, found_emails, logger, table_row_id_str)
    prompts += log_sanitization_change(raw_email, sanitized_email, logger, table_row_id_str)

    return  {
        "link_entity": f"{s2d.source_table}", #?
        "link_id": key_vals_tuple[0], 
        "link_column": s2d.source_column,
        "prompts": prompts, # JSON array
    }

# generate log JSON that will be inserted into logs table
def generate_log_json(raw_email, found_emails, sanitized_email, key_vals_tuple, s2d, contributor, logger):
    log_message = generate_log_message(raw_email, found_emails, sanitized_email, key_vals_tuple, logger, s2d)
    key_vals_dict = {key_col:key_val for key_col, key_val in zip(s2d.key, key_vals_tuple)}
    return {
        "id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_email-{s2d.source_table}-{key_vals_dict}-{s2d.job_timestamp}")), 
        "job_id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_email-{s2d.job_timestamp}")), 
        "job_timestamp": s2d.job_timestamp,
        "iteration_id": 1,
        "step_name": "sanitize_email",
        "contributor_name": contributor, 
        "log_message": log_message
    }

