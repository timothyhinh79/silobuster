import re
from email_validator import validate_email
from classes.infokind import InfoKind
import uuid

email_regex = "[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"

def get_sanitized_emails_for_update(raw_emails, contributor_vals, key_vals, s2d, logger):
    """Sanitizes the given raw_emails, and returns a JSON with the given keys and sanitized emails
       This JSON is used to generate the mapping table in Postgres for updating the raw emails
       Also logs cases when no valid email is found in the raw string
    """
    
    rows_w_sanitized_emails = []
    log_records = []
    for key_vals_tuple, raw_email, contributor in zip(key_vals, raw_emails, contributor_vals):        

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
    """Extracts valid emails from given string, and logs any errors or changes, 
       If there are multiple emails, they are combined into one string separated by commas.
    """
    sanitized_emails = re.findall(regex, raw_email_str)
    if not sanitized_emails: 
        sanitized_email_str = raw_email_str
    else: 
        sanitized_email_str = ', '.join(sanitized_emails)

    return sanitized_emails, sanitized_email_str

def get_row_w_sanitized_email(sanitized_email, keys, key_vals):
    """ Get JSON with key values and sanitized email, to be inserted as a row into table on Postgres """
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    sanitized_email_json = {InfoKind.email.value: sanitized_email}
    combined_json = keys_json | sanitized_email_json
    return combined_json

def log_email_sanitization_errors(raw_email_str, sanitized_emails, logger, table_row_id_str = ''):
    """ Logs sanitization errors in console, and generate prompts for log record """
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


def log_sanitization_change(raw_email_str, sanitized_email_str, logger, table_row_id_str = ''):
    """ Logs changes resulting from sanitization, and generates prompt for log record """
    prompt = []
    if raw_email_str != sanitized_email_str:
        sanitization_msg = f"Sanitized '{raw_email_str}' to '{sanitized_email_str}'"
        prompt += [{'description': sanitization_msg}]
        logger.debug(sanitization_msg + f" in {table_row_id_str}") 

    return prompt

def generate_log_status(raw_email_str, sanitized_email_str, found_emails, prompts):
    """ Categorizes logs for reporting purposes """
    status = []

    if not found_emails:
        status.append('No emails found')
    elif len(found_emails) == 1:
        status.append('Single email found')
    else:
        status.append('Multiple emails found')

    if any(['Invalid email address' in prompt['description'] for prompt in prompts]):
        status.append('Invalid emails found')

    if raw_email_str != sanitized_email_str:
        status.append('Sanitization change')

    return ';'.join(status)

def generate_log_message(raw_email, found_emails, sanitized_email, key_vals_tuple, logger, s2d):
    """ Generates log message JSON for log record, and also outputs msgs to console """
    table_row_id_str = f"{s2d.source_table}.{s2d.source_column} with key (" + ', '.join( [f'{col}={val}' for col, val, in zip(s2d.key, key_vals_tuple) ] ) 
    prompts = [] # first, generate prompts for log message
    prompts += log_email_sanitization_errors(raw_email, found_emails, logger, table_row_id_str)
    prompts += log_sanitization_change(raw_email, sanitized_email, logger, table_row_id_str)

    return  {
        "link_entity": f"{s2d.source_table}",
        "link_id": key_vals_tuple[0], 
        "link_column": s2d.source_column,
        "prompts": prompts, # JSON array
    }

def generate_log_json(raw_email, found_emails, sanitized_email, key_vals_tuple, s2d, contributor, logger):
    """ Generates log JSON that will be inserted into logs table """
    log_message = generate_log_message(raw_email, found_emails, sanitized_email, key_vals_tuple, logger, s2d)
    key_vals_dict = {key_col:key_val for key_col, key_val in zip(s2d.key, key_vals_tuple)}
    log_status = generate_log_status(raw_email, sanitized_email, found_emails, log_message['prompts'])
    return {
        "id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_email-{s2d.source_table}-{key_vals_dict}-{s2d.job_timestamp}")), 
        "job_id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_email-{s2d.job_timestamp}")), 
        "job_timestamp": s2d.job_timestamp,
        "iteration_id": 1,
        "step_name": "sanitize_email",
        "contributor_name": contributor, 
        "status": log_status,
        "log_message": log_message
    }

