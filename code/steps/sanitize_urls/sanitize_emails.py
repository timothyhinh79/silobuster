from email_validator import validate_email
from helper_methods import jsonify

def get_sanitized_emails(raw_emails, keys, key_vals, source_table, source_column, infokind, logger):
    sanitized_emails = []
    for raw_email in raw_emails:
        try: 
            sanitized_email = sanitize_email(raw_email)
            sanitized_emails.append(sanitized_email)
            if sanitized_email != raw_email:
                logger.debug(f"Sanitized '{raw_email}' to '{sanitized_email}'") 
        except Exception as e:
            sanitized_emails.append(raw_email)
            logger.error(
                f"Unable to parse or find email in text '{raw_email}' on table {source_table}.{source_column} with key (" +
                    ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals) ] ) + 
                ') exception: {e}'
            )
            
    return jsonify(keys, key_vals, infokind, sanitized_emails)

def sanitize_email(raw_email):
    return validate_email(raw_email.strip(), check_deliverability=False).email


