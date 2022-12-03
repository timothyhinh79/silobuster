import logging
import sys
from sanitization_code.sanitize_emails import *
from classes.src2dest import Src2Dest
import datetime
import uuid

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

singlekey_src2dest = Src2Dest(kind = 'url', key = ['id'], 
    source_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source',
    dest_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest',
    logging_db = 'dest', logging_table="logs", source_table = 'data', source_column='email', dest_table='data_dest', dest_column='email',
    job_timestamp=datetime.datetime(2022, 12, 3).strftime("%m/%d/%Y %H:%M:%S")
)

def test_sanitize_email():
    assert sanitize_email('   thisisanemail@gmail.com ', email_regex) == (['thisisanemail@gmail.com'], 'thisisanemail@gmail.com')
    assert sanitize_email('legitemail@yahoo.com', email_regex) == (['legitemail@yahoo.com'], 'legitemail@yahoo.com')
    assert sanitize_email('no email here', email_regex) == ([], 'no email here')

def test_sanitize_emails_when_string_has_multiple_emails():
    string_w_multiple_emails = '   thisisanemail@gmail.com   legitemail@yahoo.com   random_text'
    assert sanitize_email(string_w_multiple_emails, email_regex) == (['thisisanemail@gmail.com', 'legitemail@yahoo.com'], 'thisisanemail@gmail.com, legitemail@yahoo.com')

def test_get_sanitized_emails_for_update():
    emails = ['   thisisanemail@gmail.com ', 'legitemail@yahoo.com', 'no email here']
    sanitized_emails_json = get_sanitized_emails_for_update(
        raw_emails= emails,
        contributor_vals=['whatcom'] * 3,
        key_vals = ['1','2','3'],
        s2d = singlekey_src2dest,
        logger = logger
    )
    
    # only the unclean emails should be sanitized and included in sanitized_emails_json
    assert sanitized_emails_json[0] == [{'id': '1', 'email': 'thisisanemail@gmail.com'}]# extra leading/trailing spaces removed
    
    assert sanitized_emails_json[1] == [
        {'id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-data-{'id': '1'}-12/03/2022 00:00:00")), 
         'job_id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-12/03/2022 00:00:00")), 
         'job_timestamp': '12/03/2022 00:00:00', 
         'iteration_id': 1, 
         'step_name': 'sanitize_email', 
         'contributor_name': 'whatcom', 
         'log_message': {'link_entity': 'data', 'link_id': '1', 'link_column': 'email', 'prompts': [{'description': "Sanitized '   thisisanemail@gmail.com ' to 'thisisanemail@gmail.com'"}]}
         }, 
         {'id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-data-{'id': '2'}-12/03/2022 00:00:00")), 
          'job_id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-12/03/2022 00:00:00")), 
          'job_timestamp': '12/03/2022 00:00:00', 
          'iteration_id': 1,
          'step_name': 'sanitize_email',
          'contributor_name': 'whatcom',
          'log_message': {'link_entity': 'data', 'link_id': '2', 'link_column': 'email', 'prompts': []}
          }, 
          {'id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-data-{'id': '3'}-12/03/2022 00:00:00")), 
           'job_id': str(uuid.uuid3(uuid.NAMESPACE_DNS, "sanitize_email-12/03/2022 00:00:00")), 
           'job_timestamp': '12/03/2022 00:00:00', 
           'iteration_id': 1,
           'step_name': 'sanitize_email',
           'contributor_name': 'whatcom', 
           'log_message': {'link_entity': 'data', 'link_id': '3', 'link_column': 'email', 'prompts': [{'description': "Unable to find email in text 'no email here'"}]}
           }]

