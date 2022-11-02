import logging
import sys
from sanitization_code.sanitize_phone_nums import *

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

def test_sanitize_phone_num():
    assert sanitize_phone_num('811 111 1111', phone_regex, logger) == '(811) 111 1111'
    assert sanitize_phone_num('2222222222', phone_regex, logger) == '(222) 222 2222'
    assert sanitize_phone_num('(333) 333 3333', phone_regex, logger) == '(333) 333 3333'

def test_get_sanitized_phone_nums_for_update():
    phone_nums = ['811 111 1111', '2222222222', '(333) 333 3333']
    sanitized_phone_nums_json = get_sanitized_phone_nums_for_update(
        raw_phone_nums= phone_nums,
        keys = ['id'],
        key_vals = ['1','2','3'],
        source_table = 'source_tbl',
        source_column = 'source_col',
        infokind = 'phone',
        logger = logger
    )

    # only the unclean phone numbers should be sanitized and included in sanitized_phone_nums_json
    assert sanitized_phone_nums_json == [
        {'id': '1', 'phone': '(811) 111 1111'},
        {'id': '2', 'phone': '(222) 222 2222'}
    ]
