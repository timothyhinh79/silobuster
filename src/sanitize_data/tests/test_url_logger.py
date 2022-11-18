from classes.src2dest import Src2Dest
from sanitization_code.url_sanitization.url_logger import URL_Logger
import datetime
import uuid

singlekey_src2dest = Src2Dest(kind = 'url', key = ['id'], 
    source_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_source',
    dest_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost:5432/silobuster_testing_dest',
    logging_db = 'dest', logging_table="logs", source_table = 'data', source_column='url', dest_table='data_dest', dest_column='url',
    job_timestamp=datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
)

key_vals = (1,)

def test_valid_status():
    status_code_1 = 200
    status_code_2 = 400
    status_code_3 = 403
    status_code_4 = -1

    assert URL_Logger._valid_status(status_code_1) == True
    assert URL_Logger._valid_status(status_code_2) == False
    assert URL_Logger._valid_status(status_code_3) == True
    assert URL_Logger._valid_status(status_code_4) == False

def test_url_prompt():
    url_status_json_1 = {
        'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 
        'root_URL': 'https://www.w3.org', 
        'URL_status': 300, 
        'root_URL_status': 200
    }

    url_status_json_2 = {
        'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 
        'root_URL': 'https://www.w3.org', 
        'URL_status': 400, 
        'root_URL_status': 200
    }

    url_status_json_3 = {
        'URL': 'https://www.w3.org/Addressing/URL/url-spec.txtasdf', 
        'root_URL': 'https://www.w3.org', 
        'URL_status': 400, 
        'root_URL_status': 400
    }

    assert URL_Logger._url_prompt(url_status_json_1) == None
    assert URL_Logger._url_prompt(url_status_json_2) == {
                'description': f"Full URL 'https://www.w3.org/Addressing/URL/url-spec.txtasdf' is not valid, but root URL 'https://www.w3.org' is valid.",
                'suggested_value' : 'https://www.w3.org'
            }
    assert URL_Logger._url_prompt(url_status_json_3) == {
                'description': f"Neither full URL 'https://www.w3.org/Addressing/URL/url-spec.txtasdf' or root URL 'https://www.w3.org' are valid. Please double-check URL."
            }

def test_is_clean():
    sanitized_url_json_clean = {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    urls = '''  https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program
                https://www.kidsinmotionclinic.org
                https://www.maxhigbee.org
            '''  
    sanitized_url_json_unclean = {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }

    url_logger_clean = URL_Logger(sanitized_url_json_clean, singlekey_src2dest, key_vals)
    url_logger_unclean = URL_Logger(sanitized_url_json_unclean, singlekey_src2dest, key_vals)
    assert url_logger_clean.is_clean() == True
    assert url_logger_unclean.is_clean() == False

def test_create_url_prompts():
    urls = '''  https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program
                https://www.kidsinmotionclinic.org
                https://www.maxhigbee.org
            '''  
    sanitized_url_json_unclean = {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }

    url_logger_unclean = URL_Logger(sanitized_url_json_unclean, singlekey_src2dest, key_vals)    
    url_logger_unclean.create_url_prompts() == [
        {
            'description': f"Full URL 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program' is not valid, but root URL 'https://www.mtbaker.wednet.edu' is valid.",
            'suggested_value' : 'https://www.mtbaker.wednet.edu'
        },
        {
            'description': f"Neither full URL 'https://www.maxhigbee.org' or root URL 'https://www.maxhigbee.org' are valid. Please double-check URL."
        }
    ]

def test_create_message():
    sanitized_url_json_almost_clean = {'raw_string': 'removethispart: https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    urls = '''  https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program
                https://www.kidsinmotionclinic.org
                https://www.maxhigbee.org
            '''  
    sanitized_url_json_unclean = {
        'raw_string': urls,
        'condition': 'String contains multiple URLs', 
        'URLs': [
            {'URL': 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program', 'root_URL': 'https://www.mtbaker.wednet.edu', 'URL_status': 404, 'root_URL_status': 200},
            {'URL': 'https://www.kidsinmotionclinic.org', 'root_URL': 'https://www.kidsinmotionclinic.org', 'URL_status': 200, 'root_URL_status': 200},
            {'URL': 'https://www.maxhigbee.org', 'root_URL': 'https://www.maxhigbee.org', 'URL_status': -1, 'root_URL_status': -1}
        ] 
    }

    unclean_url_prompts = [
        {
            'description': f"Full URL 'https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program' is not valid, but root URL 'https://www.mtbaker.wednet.edu' is valid.",
            'suggested_value' : 'https://www.mtbaker.wednet.edu'
        },
        {
            'description': f"Neither full URL 'https://www.maxhigbee.org' or root URL 'https://www.maxhigbee.org' are valid. Please double-check URL."
        }
    ]

    url_logger_almost_clean = URL_Logger(sanitized_url_json_almost_clean, singlekey_src2dest, key_vals)
    url_logger_unclean = URL_Logger(sanitized_url_json_unclean, singlekey_src2dest, key_vals)
    assert url_logger_almost_clean.create_message([]) == 'String is not URL but contains one\nSanitized "removethispart: https://www.w3.org/Addressing/URL/url-spec.txt" to "https://www.w3.org/Addressing/URL/url-spec.txt"'
    assert url_logger_unclean.create_message(unclean_url_prompts) == f'String contains multiple URLs\nSanitized "{urls}" to "https://www.mtbaker.wednet.edu/o/erc/page/play-and-learn-program, https://www.kidsinmotionclinic.org, https://www.maxhigbee.org"\nInvalid or bad URLs found. Please review.'


def test_create_log_message():
    sanitized_url_json_clean = {'raw_string': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is URL', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    url_logger_clean = URL_Logger(sanitized_url_json_clean, singlekey_src2dest, key_vals)

    url_prompts = url_logger_clean.create_url_prompts()
    prompts = [{'description': url_logger_clean.create_message(url_prompts), 'suggested_value': ''}] + url_prompts
    
    assert url_logger_clean.create_log_message() == {
            "link_entity": f"{singlekey_src2dest.source_table}", #?
            "link_id": {key:key_val for key, key_val in zip(singlekey_src2dest.key, key_vals)},
            "link_column": singlekey_src2dest.source_column,
            "prompts": prompts, # JSON array
        }

def test_create_log_json():
    sanitized_url_json_unclean = {'timestamp': '11/14/2022 00:00:00', 'raw_string': 'removethispart: https://www.w3.org/Addressing/URL/url-spec.txt', 'condition': 'String is not URL but contains one', 'URLs': [{'URL': 'https://www.w3.org/Addressing/URL/url-spec.txt', 'root_URL': 'https://www.w3.org', 'URL_status': 200, 'root_URL_status': 200}]}
    url_logger_unclean = URL_Logger(sanitized_url_json_unclean, singlekey_src2dest, key_vals)

    url_prompts = url_logger_unclean.create_url_prompts()
    prompts = [{'description': url_logger_unclean.create_message(url_prompts), 'suggested_value': ''}] + url_prompts
    log_message = url_logger_unclean.create_log_message()

    key_vals_dict = {key_col:key_val for key_col, key_val in zip(singlekey_src2dest.key, key_vals)}

    assert url_logger_unclean.create_log_json() == {
            "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"Table: {singlekey_src2dest.source_table},  Row: {key_vals_dict}, Sanitization Timestamp: {url_logger_unclean.sanitized_url_json['timestamp']}")), 
            "job_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"Job: sanitize_url,  Job Timestamp: {singlekey_src2dest.job_timestamp}")), # how do we get job_id? could be generated automatically in separate task run at beginning of DAG, and then it would be passed as an argument to command to run dockerized container?
            "iteration_id": 1, # how do we get iteration number? would developer pass an iteration # somewhere?
            "step_name": "sanitize_url",
            "contributor_name": "test", # how do we get the contributor? query it from table (assuming it has contributor column)?
            "log_message": log_message
        }