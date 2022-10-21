import logging
import sys
from sanitization_code.sanitize_emails import *

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

def test_sanitize_email():
    assert sanitize_email('   thisisanemail@gmail.com ') == 'thisisanemail@gmail.com'
    assert sanitize_email('legitemail@yahoo.com') == 'legitemail@yahoo.com'
    try:
        bad_email = sanitize_email('no email here') 
        assert False
    except:
        assert True

def test_get_sanitized_emails_for_update():
    emails = ['   thisisanemail@gmail.com ', 'legitemail@yahoo.com', 'no email here']
    sanitized_emails_json = get_sanitized_emails_for_update(
        raw_emails= emails,
        keys = ['id'],
        key_vals = ['1','2','3'],
        source_table = 'source_tbl',
        source_column = 'source_col',
        infokind = 'email',
        logger = logger
    )
    
    # only the unclean emails should be sanitized and included in sanitized_emails_json
    assert sanitized_emails_json == [
        {'id': '1', 'email': 'thisisanemail@gmail.com'}, # extra leading/trailing spaces removed
    ]
