import logging
import sys
from sanitization_code.sanitize_phone_nums import *
import datetime
logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

def test_format_phone_numbers():
    assert format_phone_numbers( phone_regex, '811 111 1111') == '811 111 1111'
    assert format_phone_numbers(phone_regex, '2222222222') == '222 222 2222'
    assert format_phone_numbers(phone_regex, '(333) 333 3333') == '333 333 3333'
    assert format_phone_numbers(phone_regex, '1-800-787-3224 (TTY)') =='+1 800 787 3224' 
    assert format_phone_numbers(phone_regex, '1-800-787-3224 (TTY) (360) 676-1521 (360) 676-6470 ext. 4432') == []

def test_get_sanitized_phone_nums_for_update():
    phone_nums = ['811 111 1111', '2222222222', '(333) 333 3333']
    sanitized_phone_nums_json = get_sanitized_phone_nums_for_update(
        raw_phone_nums= phone_nums,
        keys = ['id'],
        key_vals = ['1','2','3'],
        source_table = 'source_tbl',
        source_column = 'source_col',
        logger = logger
    )

    # only the unclean phone numbers should be sanitized and included in sanitized_phone_nums_json
    assert sanitized_phone_nums_json == [
        {'id': '2', 'phone': '222 222 2222'},
        {'id': '3', 'phone': '333 333 3333'}
    ]
