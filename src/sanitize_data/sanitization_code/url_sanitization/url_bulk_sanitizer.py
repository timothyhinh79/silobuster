from sanitization_code.url_sanitization.url_console_logging import *
from sanitization_code.url_sanitization.url_logger import URL_Logger
from classes.infokind import InfoKind
from classes.multithreader import Multithreader
from sanitization_code.url_sanitization.url_sanitizer import URL_Sanitizer

class URL_BulkSanitizer:

    url_sanitization_num_threads = 500

    def __init__(self, strings, key_val_rows, contributor_values, src2dest, logger):
        self._strings = strings
        self._key_values = key_val_rows
        self._contributor_values = contributor_values
        self._src2dest = src2dest
        self._logger = logger

    @property
    def strings(self):
        return self._strings

    @property
    def key_values(self):
        return self._key_values

    @property
    def contributor_values(self):
        return self._contributor_values

    @property
    def src2dest(self):
        return self._src2dest

    @property
    def logger(self):
        return self._logger

    ### HELPER METHODS FOR APPLYING SANITIZATION TO GIVEN STRINGS AND GENERATING FINAL JSON FOR UPDATE QUERY
    @classmethod
    def sanitize_single_url(cls, string):
        """Sanitizes single URL string"""
        url_sanitizer = URL_Sanitizer(string)
        sanitized_url_json = url_sanitizer.sanitize_url()
        return sanitized_url_json

    @classmethod
    def combine_sanitized_urls(cls, sanitized_urls_json):
        """Combines multiple URLs in sanitized_urls_json into single string separated by a comma"""
        if not sanitized_urls_json['URLs']:
            return sanitized_urls_json['raw_string']

        clean_urls = [url['URL'] for url in sanitized_urls_json['URLs']]
        return ', '.join(clean_urls)

    @classmethod
    def construct_json(cls, sanitized_url_str, keys, key_vals):
        """Generates JSON with key values and sanitized value"""
        url_json = {InfoKind.url.value: sanitized_url_str}
        keys_json = {key:key_val for key, key_val in zip(keys, key_vals)}
        combined_json = keys_json | url_json
        return combined_json

    def get_sanitized_url_jsons(self):
        """Runs sanitize_url to generate JSON with sanitized URL and other information needed for logging"""
        url_multithreader = Multithreader(method = self.sanitize_single_url,
                                          inputs = self.strings,
                                          num_threads = self.url_sanitization_num_threads)
        sanitized_url_jsons = url_multithreader.run()
        return sanitized_url_jsons


    def get_row_w_sanitized_url(self, sanitized_url_json, key_vals, table_row_id_str):
        """Generates JSON with key values and sanitized URL from sanitized_url_json if sanitization resulted in a change"""
        raw_url = sanitized_url_json['raw_string']
        sanitized_url_str = self.combine_sanitized_urls(sanitized_url_json)

        if sanitized_url_str != raw_url:
            log_sanitization_change(self.logger, raw_url, sanitized_url_str, table_row_id_str)
            return self.construct_json(sanitized_url_str, self.src2dest.key, key_vals)
        else:
            return None  

    ### MAIN METHOD TO RUN SANITIZATION ON GIVEN STRINGS AND GENERATE JSON FOR UPDATING RAW DATA
    def get_jsons_for_update(self):
        """Sanitizes the given raw_urls, and returns a JSON with the given keys and sanitized URLs"""
        sanitized_url_jsons = self.get_sanitized_url_jsons()
        
        rows_w_sanitized_url = []
        log_records = []
        for contributor_val, key_vals_row, sanitized_url_json in zip(self.contributor_values, self.key_values, sanitized_url_jsons):

            # for console logging
            table_row_id_str = get_error_location_str_for_logging(self.src2dest.source_table, self.src2dest.source_column, self.src2dest.key, key_vals_row)
            
            # add row if sanitization changed raw URL string, and log the change
            new_row = self.get_row_w_sanitized_url(sanitized_url_json, key_vals_row, table_row_id_str)
            if new_row: rows_w_sanitized_url.append(new_row)

            # log errors in URL sanitization
            log_url_sanitization_errors(self.logger, sanitized_url_json, table_row_id_str)

            # insert log record into logs table if there is an issue/error
            url_logger = URL_Logger(sanitized_url_json, self.src2dest, key_vals_row, contributor_val)
            log_json = url_logger.create_log_json()
            if log_json: log_records.append(log_json)

        return rows_w_sanitized_url, log_records


    

    

    
    
