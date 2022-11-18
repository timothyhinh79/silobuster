import json # needed to load JSON into logging table
import uuid # to hash IDs for log records
from classes.src2dest import Src2Dest # Src2Dest instances store information needed for logging
import sqlalchemy # to insert log record into database

class URL_Logger:

    def __init__(self, sanitized_url_json: dict, src2dest: Src2Dest, key_vals: tuple):

        self._sanitized_url_json = sanitized_url_json
        self._src2dest = src2dest
        self._key_vals = key_vals

    @property
    def sanitized_url_json(self):
        return self._sanitized_url_json

    @property
    def src2dest(self):
        return self._src2dest

    @property
    def key_vals(self):
        return self._key_vals
    
    # determine what status codes are valid
    # NOTE: may want to classify 403s and 406s as valid
    @classmethod
    def _valid_status(cls, status_code):
        if status_code == -1 or (status_code >= 400 and status_code not in (403,406)):
            return False
        return True

    # construct prompt for log_message based on status code of a specific URL (and its root URL)
    @classmethod
    def _url_prompt(cls, url_status_json):
        if not URL_Logger._valid_status(url_status_json['URL_status']) and URL_Logger._valid_status(url_status_json['root_URL_status']):
            return {
                'description': f"Full URL '{url_status_json['URL']}' is not valid, but root URL '{url_status_json['root_URL']}' is valid.",
                'suggested_value' : url_status_json['root_URL']
            }

        elif not URL_Logger._valid_status(url_status_json['URL_status']) and not URL_Logger._valid_status(url_status_json['root_URL_status']):
            return {
                'description': f"Neither full URL '{url_status_json['URL']}' or root URL '{url_status_json['root_URL']}' are valid. Please double-check URL."
            }

        else:
            return None

    # determine if we need to output a log record, based on whether all URLs found in the string are valid
    def is_clean(self):
        all_urls_valid = all([URL_Logger._valid_status(url['URL_status']) for url in self.sanitized_url_json['URLs']])
        url_string_valid = self.sanitized_url_json['condition'] == 'String is URL'
        return all_urls_valid and url_string_valid

    # create prompts for each URL in a string
    def create_url_prompts(self):
        prompts = []
        for url_status_json in self.sanitized_url_json['URLs']:
            prompt =  URL_Logger._url_prompt(url_status_json)
            if prompt: prompts.append(prompt)
        return prompts

    # create single message to summarize sanitization result
    def create_message(self, url_prompts):
        message = self.sanitized_url_json['condition']
        raw_string = self.sanitized_url_json['raw_string']
        sanitized_string = ', '.join([url_dict['URL'] for url_dict in self.sanitized_url_json['URLs']])
        if sanitized_string != raw_string:
            message += f'\nSanitized "{raw_string}" to "{sanitized_string}"'

        if len(url_prompts) > 0:
            message += f'\nInvalid or bad URLs found. Please review.'

        return message

    # create log message identifying table, row, field, with prompts to suggest corrections for any errors
    def create_log_message(self):
        url_prompts = self.create_url_prompts()
        message = self.create_message(url_prompts)
        prompts = [{'description': message, 'suggested_value': ''}] + url_prompts

        json = {
            # "id": , # is there a need for id if there's only one log message per row in the log table?
            "link_entity": f"{self.src2dest.source_table}", #?
            "link_id": {key:key_val for key, key_val in zip(self.src2dest.key, self.key_vals)},
            "link_column": self.src2dest.source_column,
            "prompts": prompts, # JSON array
        }
        return json

    # create a log JSON with IDs and the log_message if needed
    def create_log_json(self):
        if self.is_clean(): return None

        log_message = self.create_log_message()
        key_vals_dict = {key_col:key_val for key_col, key_val in zip(self.src2dest.key, self.key_vals)}
        
        return {
            "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"Table: {self.src2dest.source_table},  Row: {key_vals_dict}, Sanitization Timestamp: {self.sanitized_url_json['timestamp']}")), 
            "job_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"Job: sanitize_url,  Job Timestamp: {self.src2dest.job_timestamp}")), # how do we get job_id? could be generated automatically in separate task run at beginning of DAG, and then it would be passed as an argument to command to run dockerized container?
            "iteration_id": 1, # how do we get iteration number? would developer pass an iteration # somewhere?
            "step_name": "sanitize_url",
            "contributor_name": "test", # how do we get the contributor? query it from table (assuming it has contributor column)?
            "log_message": log_message
        }
        

    # insert log JSON into logging table if necessary
    def insert_log_record(self):
        if self.is_clean(): return
        log_json = [self.create_log_json()]

        # insert log record into logging table in source or dest db
        insert_query = f"""
            INSERT INTO {self.src2dest.logging_table}
            SELECT *
            FROM json_populate_recordset(NULL::{self.src2dest.logging_table}, :log_json);
        """

        if self.src2dest.logging_db == 'source':
            self.src2dest._open_source_conn()
            self.src2dest.source_conn.execute(sqlalchemy.sql.text(insert_query), log_json = json.dumps(log_json))
            self.src2dest._close_source_conn()
        elif self.src2dest.logging_db == 'dest':
            self.src2dest._open_dest_conn()
            self.src2dest.dest_conn.execute(sqlalchemy.sql.text(insert_query), log_json = json.dumps(log_json))
            self.src2dest._close_dest_conn()
        else:
            raise Exception('logging_db was not specified as "source" or "dest"')