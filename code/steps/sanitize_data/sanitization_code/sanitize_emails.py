from email_validator import validate_email

def get_sanitized_emails_for_update(raw_emails, keys, key_vals, source_table, source_column, infokind, logger):
    """Sanitizes the given raw_emails, and returns a JSON with the given keys and sanitized emails
       This JSON is used to generate the mapping table in Postgres for updating the raw emails
       Also logs cases when no valid email is found in the raw string
    
    Parameters:
        raw_emails (list): array of raw email strings from table to sanitize
        keys (list): array of column names for the keys (e.g. ID)
        key_vals (list): array of tuples, each one have the values of each key field for a specific record
        source_table (str): name of source table containing un-sanitized data
        source_column (str): name of column in source table with the un-sanitized data
        infokind (str): one of [phone, email, url]
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 

    Returns:
        sanitized_email_jsons (dict): JSONs summarizing key values and sanitized emails for each record
    """
    
    sanitized_emails = []
    for key_vals_tuple, raw_email in zip(key_vals, raw_emails):
        try: 
            sanitized_email = sanitize_email(raw_email)
            if sanitized_email != raw_email:
                sanitized_emails.append({key:key_val for key, key_val in zip(keys, key_vals_tuple)} | {infokind: sanitized_email})
                logger.debug(f"Sanitized '{raw_email}' to '{sanitized_email}'") 
        except Exception as e:
            logger.error(
                f"Unable to parse or find email in text '{raw_email}' on {source_table}.{source_column} with key (" +
                    ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) + 
                ')'
            )
            
    return sanitized_emails

def sanitize_email(raw_email):
    """Validates if raw_email (after removing leading/trailing spaces) is a valid email. 

    Parameters:
        raw_email (str): raw email string from data table

    Returns:
         Email string if raw_email is valid, otherwise raises an error
    """
    return validate_email(raw_email.strip(), check_deliverability=False).email


