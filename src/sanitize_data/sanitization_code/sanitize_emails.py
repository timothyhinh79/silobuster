import re
from email_validator import validate_email
from classes.infokind import InfoKind

email_regex = "[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"

def get_sanitized_emails_for_update(raw_emails, keys, key_vals, source_table, source_column, logger):
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
    for key_vals_tuple, raw_email in zip(key_vals, raw_emails):
        
        # for logging
        table_row_id_str = f"{source_table}.{source_column} with key (" + ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) 

        # sanitize email and log any errors or changes resulting from sanitization
        sanitized_email = sanitize_email(raw_email, email_regex, logger, table_row_id_str)

        # add row if sanitization changed raw URL string
        if sanitized_email != raw_email:
            row_w_sanitized_email = get_row_w_sanitized_email(sanitized_email, keys, key_vals_tuple)
            rows_w_sanitized_emails.append(row_w_sanitized_email)
            
    return rows_w_sanitized_emails

def sanitize_email(raw_email_str, regex, logger, table_row_id_str = ''):
    """Extracts valid emails from given string, and logs any errors or changes. 
       Combines multiple emails into one string separated by commas.

    Parameters:
        raw_email_str (str): raw email string from data table
        regex (str): pattern for identifying email addresses
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 
        table_row_id_str (str): string identifying which row in what table is being sanitized

    Returns: string
    """
    sanitized_emails = re.findall(regex, raw_email_str)
    if not sanitized_emails: return raw_email_str

    sanitized_email_str = ', '.join(sanitized_emails)

    log_email_sanitization_errors(raw_email_str, sanitized_emails, logger, table_row_id_str)
    log_sanitization_change(raw_email_str, sanitized_email_str, logger, table_row_id_str)

    return sanitized_email_str

# get JSON with key values and sanitized email, to be inserted as a row into table on Postgres
def get_row_w_sanitized_email(sanitized_email, keys, key_vals):
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    sanitized_email_json = {InfoKind.email.value: sanitized_email}
    combined_json = keys_json | sanitized_email_json
    return combined_json

def log_email_sanitization_errors(raw_email_str, sanitized_emails, logger, table_row_id_str = ''):
    if not sanitized_emails: 
        logger.error(f"Unable to find email in text {raw_email_str} in {table_row_id_str}")

    for email_string in sanitized_emails:
        log_email_validation(email_string, logger, table_row_id_str)

# second layer of validation using validate_email() from email_validator library
def log_email_validation(sanitized_email, logger, table_row_id_str = ''):
    try:
        validate_email(sanitized_email.strip(), check_deliverability=False).email
    except:
        logger.error(f"Invalid email address {sanitized_email} found in {table_row_id_str}")

# if sanitization changed raw string, log it
def log_sanitization_change(raw_email_str, sanitized_email_str, logger, table_row_id_str = ''):
    if raw_email_str != sanitized_email_str:
        sanitization_msg = f"Sanitized '{raw_email_str}' to '{sanitized_email_str}' in {table_row_id_str}"
        logger.debug(sanitization_msg) 


