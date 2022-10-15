import phonenumbers
from helper_methods import jsonify

def get_sanitized_phone_nums(raw_phone_nums, keys, key_vals, source_table, source_column, infokind, logger):
    sanitized_phone_nums = []
    for key_vals_tuple, raw_phone_num in zip(key_vals, raw_phone_nums):
        try: 
            sanitized_phone_num = sanitize_phone_num(raw_phone_num)
            sanitized_phone_nums.append({key:key_val for key, key_val in zip(keys, key_vals_tuple)} | {infokind: sanitized_phone_num})
            if sanitized_phone_num != raw_phone_num:
                logger.debug(f"Sanitized '{raw_phone_num}' to '{sanitized_phone_num}'") 
        except Exception as e:
            sanitized_phone_nums.append({key:key_val for key, key_val in zip(keys, key_vals_tuple)} | {infokind: raw_phone_num})
            logger.error(
                f"Unable to parse or find phone number in text '{raw_phone_num}' on {source_table}.{source_column} with key (" +
                    ', '.join( [f'{col}={val}' for col, val, in zip(keys, key_vals_tuple) ] ) + 
                ')')
    
    return sanitized_phone_nums

def sanitize_phone_num(raw_phone_num):
    return phonenumbers.format_number(phonenumbers.parse(raw_phone_num.strip(), 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')

