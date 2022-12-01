import re
import phonenumbers
from classes.infokind import InfoKind
import datetime
import psycopg2

# TO-DOs: 
#   use regex for identifying multiple phone numbers
#   split out multiple phone numbers into different rows
#   fill out phone extension if appropriate

# placeholder for now...will spend more time figuring out a better Regex that accurately can identify multiple numbers in a string
phone_regex = "\s*(?:\+?(?P<country_code>1{1}))?([-. (]*(?P<area_code>\d{3})[-. )]*)?((?P<first_three>\d{3})[-. ]*(?:[-.x ]*(?P<last_four>\d+))?)\s*"


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
        sanitized_phone = cleaning_engine(phone_regex, raw_phone_num)

        # add row if sanitization changed raw URL string
        
        if sanitized_phone != raw_phone_num:
            row_w_sanitized_phone = get_row_w_sanitized_phone(sanitized_phone, keys, key_vals_tuple)
            rows_w_sanitized_phone.append(row_w_sanitized_phone)
    
    return rows_w_sanitized_phone

# def sanitize_phone_num(raw_phone_num, regex, logger, table_row_id_str = ''):
#     """sanitizes raw phone number to format "(XXX) XXX XXXX"

#     Parameters:
#         raw_phone_num (str): raw phone number string from data table
#         regex (str): pattern for identifying phone numbers
#         logger (logging.RootLogger): logger to output messages on success of sanitization to the terminal for debugging 
#         table_row_id_str (str): string identifying which row in what table is being sanitized

#     Returns:
#          Phone number in format "(XXX) XXX XXXX" if valid phone number is found.
#          If no phone number is found in raw_phone_num, returns original string
#     """
#     try:
#         sanitized_phone = phonenumbers.format_number(phonenumbers.parse(raw_phone_num.strip(), 'US'), phonenumbers.PhoneNumberFormat.NATIONAL).replace('-', ' ')
#         if sanitized_phone != raw_phone_num:
#             logger.debug(f"Sanitized '{raw_phone_num}' to '{sanitized_phone}' in {table_row_id_str}")
#         return sanitized_phone
#     except:
#         logger.error(f"Unable to parse or find phone number in text '{raw_phone_num}' on {table_row_id_str}")
#         return raw_phone_num

def pluck_phone_num(regex, raw_phone_str, logger):
    if raw_phone_str:
        sanitized_phones = re.findall(regex, raw_phone_str)
    # if not sanitized_phones: return raw_phone_str
    # sanitized_phone_str = ', '.join(sanitized_phones) 
    numbers = []
    if  sanitized_phones[0][0] == '':
        numbers.extend([sanitized_phones[0][2], sanitized_phones[0][4], sanitized_phones[0][5]])
    else: numbers.extend([sanitized_phones[0][0], sanitized_phones[0][2], sanitized_phones[0][4], sanitized_phones[0][5]])
#
#
    phone_number_groups = re.findall(regex, raw_phone_str)
    number_list = []
    for groups in phone_number_groups:
        if groups[0] == '' and groups[2] != '':
            numbers = ' '.join([groups[2], groups[4], groups[5]])
        else: 
            numbers = ' '.join([groups[0], groups[2], groups[4], groups[5]])
        number_list.append(numbers)

    if len(number_list) > 1:
        logger.debug(f"multiple phone numbers inside of entry")
        

    return number_list 



def format_phone_numbers(regex, raw_phone_num, logger):
    raw_list = pluck_phone_num(regex, raw_phone_num, logger)
    sanitized_numbers = []
    for phone_number in raw_list:
        if phone_number[0] == '1': 
            added_plus = f'+{phone_number}'
            sanitized_numbers.append(added_plus)
        else: 
            sanitized_numbers.append(phone_number)

        return sanitized_numbers

# def log_email_sanitization_errors(raw_email_str, sanitized_emails, logger, table_row_id_str = ''):
#     if not sanitized_emails: 
#         logger.error(f"Unable to find email in text {raw_email_str} in {table_row_id_str}")

#This step checks the final highlighted regex boxes for stray numbers like extensions and only lets full phone numbers pass through
def phone_number_sieve(formatted_phone_list):
    strained_phone_numbers = [phone_number for phone_number in formatted_phone_list if len(phone_number) >= 12]
    
    return strained_phone_numbers

    # if len(sanitized_numbers) == 1:
    #         return sanitized_numbers[0]
    # else: return sanitized_numbers
    # if raw_list[0] == '1':
    #     sanitized_phone_number = f'+{raw_list[0]} {raw_list[1]} {raw_list[2]} {raw_list[3]}'
    # else:
    #     sanitized_phone_number = f'{raw_list[0]} {raw_list[1]} {raw_list[2]}'
    # return sanitized_phone_number
        

# get JSON with key values and sanitized phone number, to be inserted as a row into table on Postgres
def get_row_w_sanitized_phone(sanitized_phone, keys, key_vals):
    keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
    sanitized_phone_json = {InfoKind.phone.value: sanitized_phone}
    combined_json = keys_json | sanitized_phone_json
    return combined_json 

# make a function that takes in all the arguments and strings together the previous three 

def cleaning_engine(phone_regex, raw_phone_str, logger):
    raw_phone_num = pluck_phone_num(phone_regex, raw_phone_str, logger)
    formatted_phone_list = format_phone_numbers(phone_regex, raw_phone_num, logger )
    sanitized_phone_numbers = phone_number_sieve(formatted_phone_list)
    return sanitized_phone_numbers


#1. write a function to find phone number rows with two phone numbers in them (maybe use the sanitize phone number function since it tells you if theres more than one number)
#2. insert an additional row into the table using pymsql
#3. make it work for the "(360) 715-9170 (office) or (360)778-2036 (HomeStore)" example 


def crowd_disperser(plucked_phones, raw_phone_str): 
    conn = psycopg2.connect(dbname = 'defaultdb', user = 'postgres', password = 'postgres')
    cursor = conn.cursor()
    if len(plucked_phones) > 1: 
        
    # insert a new phone into the phones table #
    # insert all the same values in every row but add each phone number sequentially from the list of plucked phones 
    # loop through the list of plucked phones 
    # for each phone in phones duplicate the original row and place each phone in the number column 
        sql = """select id from phone where number = %s"""

        cursor.execute(sql, (raw_phone_str,))

        original_id = cursor.fetchone()[0]

        for index, phone in enumerate(plucked_phones):
            new_id = f'{original_id}-{index}'
            sql = """ insert into phone (
                id,
                location_id,
                service_id,
                organization_id,
                contact_id,
                service_at_location_id, 
                number,
                extension,
                type,
                language,
                description

            ),

            select %s,
                location_id,
                service_id,
                organization_id,
                contact_id,
                service_at_location_id, 
                %s,
                extension,
                type,
                language,
                description 
            from phone where id = %s
            
            """
            cursor.execute(sql, (new_id, phone, original_id,))

        sql = """delete from phone where id = %s"""
        cursor.execute(sql, (original_id,))

            
        
        
        return id
