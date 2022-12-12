import json # needed to load JSON into logging table
import uuid # to hash IDs for log records
from classes.src2dest import Src2Dest # Src2Dest instances store information needed for logging
import sqlalchemy # to insert log record into database
import datetime

class URL_Logger:

    def __init__(self, sanitized_url_json: dict, src2dest: Src2Dest, key_vals: tuple, contributor: str):

        self._sanitized_url_json = sanitized_url_json
        self._src2dest = src2dest
        self._key_vals = key_vals
        self._contributor = contributor

    @property
    def sanitized_url_json(self):
        return self._sanitized_url_json

    @property
    def src2dest(self):
        return self._src2dest

    @property
    def key_vals(self):
        return self._key_vals

    @property
    def contributor(self):
        return self._contributor
    
    @classmethod
    def _valid_status(cls, status_code):
        """ Determines what status codes are valid """
        # 403 (forbidden) and 406 (not acceptable) imply a valid URL
        if status_code == -1 or (status_code >= 400 and status_code not in (403,406)):
            return False
        return True

    @classmethod
    def _status_message(cls, status_code):
        """ Generates log message based on URL status code for log records """
        if cls._valid_status(status_code): return ''
        if status_code == -1:
            return 'generates no response'
        else:
            return f'returns a {status_code} status code'

    @classmethod
    def _url_prompt(cls, url_status_json):
        """ Constructs prompt for log_message based on status code of a specific URL (and its root URL) """
        full_URL = url_status_json['URL']
        root_URL = url_status_json['root_URL']
        full_URL_status = url_status_json['URL_status']
        root_URL_status = url_status_json['root_URL_status']

        if full_URL_status in (429, 503):
            return {
                'description': f"Received error code {full_URL_status} likely because too many requests were made to URL '{full_URL}' during validation. Please double-check URL."
            }

        if not cls._valid_status(full_URL_status):
            if root_URL_status in (429, 503):
                return {
                    'description': f"Received error code {root_URL_status} likely because too many requests were made to URL '{root_URL}' during validation. Please double-check URL."
                }

            if cls._valid_status(root_URL_status):
                return {
                    'description': f"Full URL '{full_URL}' is not valid ({cls._status_message(full_URL_status)}), but root URL '{root_URL}' is valid.",
                    'suggested_value' : root_URL
                }

            if not cls._valid_status(root_URL_status):
                return {
                    'description': f"Neither full URL '{full_URL}' ({cls._status_message(full_URL_status)}) or root URL '{root_URL}' ({cls._status_message(root_URL_status)}) are valid. Please double-check URL."
                }

        return None

    def log_status(self):
        """ Categorizes logs for reporting purposes """
        all_urls_valid = all([URL_Logger._valid_status(url['URL_status']) for url in self.sanitized_url_json['URLs']])
        sanitization_change = self.sanitized_url_json['sanitized_string'] != self.sanitized_url_json['raw_string']

        status = []
        status.append(self.sanitized_url_json['condition'])
        if sanitization_change:
            status.append('Sanitization change')
        if not all_urls_valid:
            status.append('Invalid URLs found')
        
        return ';'.join(status)

    def create_url_prompts(self):
        """ Creates prompts for each URL in a string """
        prompts = []
        for url_status_json in self.sanitized_url_json['URLs']:
            prompt =  URL_Logger._url_prompt(url_status_json)
            if prompt: prompts.append(prompt)
        return prompts

    def create_message(self, url_prompts):
        """ Creates single message to summarize sanitization result """
        message = self.sanitized_url_json['condition']
        raw_string = self.sanitized_url_json['raw_string']
        if not self.sanitized_url_json['URLs']:
            sanitized_string = self.sanitized_url_json['raw_string']
        else:
            sanitized_string = ', '.join([url_dict['URL'] for url_dict in self.sanitized_url_json['URLs']])

        if sanitized_string != raw_string:
            message += f"\nSanitized '{raw_string}' to '{sanitized_string}'"
        else:
            message += f"\nNo change to '{raw_string}'"

        if len(url_prompts) > 0:
            message += f"\nInvalid or bad URLs found. Please review."

        return message

    def create_log_message(self):
        """ Create log message identifying table, row, field, with prompts to suggest corrections for any errors """
        url_prompts = self.create_url_prompts()
        message = self.create_message(url_prompts)
        prompts = [{'description': message}] + url_prompts

        json = {
            "link_entity": f"{self.src2dest.source_table}", 
            "link_id": self.key_vals[0], # assuming key_vals has only one value for the "id" field (primary key of the table)
            "link_column": self.src2dest.source_column,
            "prompts": prompts, # JSON array
        }
        return json

    def create_log_json(self):
        """ Create a log JSON with IDs and the log_message if needed """
        log_message = self.create_log_message()
        log_status = self.log_status()
        key_vals_dict = {key_col:key_val for key_col, key_val in zip(self.src2dest.key, self.key_vals)}
        
        return {
            "id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_url-{self.src2dest.source_table}-{key_vals_dict}-{self.src2dest.job_timestamp}")), 
            "job_id": str(uuid.uuid3(uuid.NAMESPACE_DNS, f"sanitize_url-{self.src2dest.job_timestamp}")), # how do we get job_id? could be generated automatically in separate task run at beginning of DAG, and then it would be passed as an argument to command to run dockerized container?
            "job_timestamp": self.src2dest.job_timestamp,
            "iteration_id": 1, # how do we get iteration number? would developer pass an iteration # somewhere?
            "step_name": "sanitize_url",
            "contributor_name": self.contributor, 
            "status": log_status,
            "log_message": log_message
        }
        