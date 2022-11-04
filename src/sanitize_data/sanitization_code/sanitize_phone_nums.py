import re
import phonenumbers
from classes.infokind import InfoKind

# TO-DOs: 
#   use regex for identifying multiple phone numbers
#   split out multiple phone numbers into different rows
#   fill out phone extension if appropriate

# placeholder for now...will spend more time figuring out a better Regex that accurately can identify multiple numbers in a string
phone_regex = "\s*(?:\+?(\d{1,3}))?([-. (]*(\d{3})[-. )]*)?((\d{3})[-. ]*(\d{2,4})(?:[-.x ]*(\d+))?)\s*"

def get_sanitized_phone_nums_for_update(raw_phone_nums, keys, key_vals, source_table, source_column, logger):
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
    
    rows_w_sanitized_phone = []
    for key_vals_tuple, raw_phone_num in zip(key_vals, raw_phone_nums):
        # for logging
        table_row_id_str = f"{source_table}.{source_column} with key (" + ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) 

        # sanitize phone and log any errors or changes resulting from sanitization
        sanitized_phone = sanitize_phone_num(raw_phone_num, phone_regex, logger, table_row_id_str)

        # add row if sanitization changed raw URL string
        if sanitized_phone != raw_phone_num:
            row_w_sanitized_phone = get_row_w_sanitized_phone(sanitized_phone, keys, key_vals_tuple)
            rows_w_sanitized_phone.append(row_w_sanitized_phone)
    
    return rows_w_sanitized_phone

def sanitize_phone_num(raw_phone_num, regex, logger, table_row_id_str = ''):
    """sanitizes raw phone number to format "(XXX) XXX XXXX"

    Parameters:
        raw_phone_num (str): raw phone number string from data table
        regex (str): pattern for identifying phone numbers
        logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 
        table_row_id_str (str): string identifying which row in what table is being sanitized

    Returns:
         Phone number in format "(XXX) XXX XXXX" if valid phone number is found.
         If no phone number is found in raw_phone_num, returns original string
    """
    try:
        sanitized_phone = phonenumbers.format_number(phonenumbers.parse(raw_phone_num.strip(), 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')
        if sanitized_phone != raw_phone_num:
            logger.debug(f"Sanitized '{raw_phone_num}' to '{sanitized_phone}' in {table_row_id_str}")
        return sanitized_phone
    except:
        logger.error(f"Unable to parse or find phone number in text '{raw_phone_num}' on {table_row_id_str}")
        return raw_phone_num


# get JSON with key values and sanitized phone number, to be inserted as a row into table on Postgres
def get_row_w_sanitized_phone(sanitized_phone, keys, key_vals):
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    sanitized_phone_json = {InfoKind.phone.value: sanitized_phone}
    combined_json = keys_json | sanitized_phone_json
    return combined_json

