import phonenumbers

def get_sanitized_phone_nums_for_update(raw_phone_nums, keys, key_vals, source_table, source_column, infokind, logger):
    """Sanitizes the given raw_phone_nums, and returns a JSON with the given keys and sanitized phone numbers
       This JSON is used to generate the mapping table in Postgres for updating the raw phone numbers
       Also logs cases when no valid phone number is found in the raw string
    
    Parameters:
        raw_phone_nums (list): array of raw phone number strings from table to sanitize
        keys (list): array of column names for the keys (e.g. ID)
        key_vals (list): array of tuples, each one have the values of each key field for a specific record
        source_table (str): name of source table containing un-sanitized data
        source_column (str): name of column in source table with the un-sanitized data
        infokind (str): one of [phone, email, url]
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 

    Returns:
        sanitized_phone_num_jsons (dict): JSONs summarizing key values and sanitized phone numbers for each record
    """
    
    sanitized_phone_nums = []
    for key_vals_tuple, raw_phone_num in zip(key_vals, raw_phone_nums):
        try: 
            sanitized_phone_num = sanitize_phone_num(raw_phone_num)
            if sanitized_phone_num != raw_phone_num:
                sanitized_phone_nums.append({key:key_val for key, key_val in zip(keys, key_vals_tuple)} | {infokind: sanitized_phone_num})
                logger.debug(f"Sanitized '{raw_phone_num}' to '{sanitized_phone_num}'") 
        except Exception as e:
            logger.error(
                f"Unable to parse or find phone number in text '{raw_phone_num}' on {source_table}.{source_column} with key (" +
                    ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) + 
                ')')
    
    return sanitized_phone_nums

def sanitize_phone_num(raw_phone_num):
    """sanitizes raw phone number to format "(XXX) XXX XXXX"

    Parameters:
        raw_phone_num (str): raw phone number string from data table

    Returns:
         Phone number in format "(XXX) XXX XXXX" if valid phone number is found.
         If no phone number is found in raw_phone_num, raises an error
    """
    return phonenumbers.format_number(phonenumbers.parse(raw_phone_num.strip(), 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')

