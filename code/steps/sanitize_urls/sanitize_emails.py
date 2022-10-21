from email_validator import validate_email
from helper_methods import jsonify

def get_sanitized_emails_for_update(raw_emails, keys, key_vals, source_table, source_column, infokind, logger):
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
    return validate_email(raw_email.strip(), check_deliverability=False).email


