import requests # used to validate if URLs exist
from urllib.parse import urlparse # used to extract root URLs

import re # Regex used to identify valid URL patterns
from sanitization_code.url_sanitization.url_regex import url_regex # used to identify valid URL strings
import time # to allow subsequent validation attempts on URLs returning 429 or 503
import datetime # to timestamp when each URL was sanitized

class URL_Sanitizer:

    ### PARAMETERS FOR URL SANITIZATION/VALIDATION
    url_validation_timeout = 60 # number of seconds for requests.get() to validate if a given URL exists
    retry_wait = 60 # of seconds before reattempting validation of URLs that initially return 429 or 503
    max_attempts = 3 # of maximum attempts to retrieve a valid status code for URLs initially returnning 429 or 503

    ### CONSTRUCTOR
    def __init__(self, string):
        self._string = string
    
    @property
    def string(self):
        return self._string

    ### HELPER CLASS METHODS FOR SANITIZING RAW URL STRING
    @classmethod
    def extract_root_url(cls, url_string):
        """Extracts root URL from given URL string"""

        url_parts = urlparse(url_string)
        return url_parts.scheme + '://' + url_parts.netloc

    @classmethod
    def get_url_status(cls, url_string):
        """Retrieves status code for the given URL, with a retry mechanism for URLs returning 429, 503"""

        ## IMPORTANT NOTE (Tim): as of 12/3/2022, temporarily disabling below code that makes requests.get() call 
        ## and instead returning 200 (so that all URLs are assumed to be valid). This will effectively remove the  validation
        ## component from log reports to quickly meet deadlines in December 2022. PLEASE NOTE THAT TESTS RELATING TO URL VALIDATION IN 
        ## tests/ FOLDER  WILL FAIL AS A RESULT. Remove the 'return 200' line to re-enable validation.
        return 200
        
        try:
            status_code = requests.get(url_string, timeout = cls.url_validation_timeout).status_code
            num_attempts = 1
            while status_code in (429,503) and num_attempts < cls.max_attempts:
                num_attempts += 1
                time.sleep(cls.retry_wait)
                status_code = requests.get(url_string, timeout = cls.url_validation_timeout).status_code
        except:
            return -1

        return status_code

    @classmethod
    def assign_url_status(cls, url_string, root_url):
        """Generates JSON defining status codes for the given URL and the embedded root URL"""

        url_status_code = cls.get_url_status(url_string)
        output = {'URL_status': url_status_code}

        # if full URL returns 200, then we assume root URL returns 200
        if url_status_code == 200:
            root_url_status_code = 200
        else:
            root_url_status_code = cls.get_url_status(root_url)
        output['root_URL_status'] = root_url_status_code

        return output

    @classmethod
    def assign_string_condition(cls, string, url_matches):
        """Assigns appropriate condition to given string based on URL Regex matches"""

        if url_matches == []: 
            condition = 'String contains no URLs'
        elif url_matches[0] == string:
            condition = 'String is URL'
        elif len(url_matches) == 1:
            condition = 'String is not URL but contains one'
        else:
            condition = 'String contains multiple URLs'
        
        return condition

    ### MAIN METHOD TO RUN SANITIZATION AND GENERATE JSON OUTPUT
    def sanitize_url(self):
        """Extract URLs from given string (using Regex) and logs each URL's status code"""

        url_strings = re.findall(url_regex, self.string)
        condition = self.assign_string_condition(self.string, url_strings)
        
        # construct JSON with each URL regex match, root URL, and status codes
        url_output = []
        for url_string in url_strings:
            root_url = self.extract_root_url(url_string)
            url_status = self.assign_url_status(url_string, root_url)
            url_output.append({'URL': url_string, 'root_URL': root_url})
            url_output[-1].update(url_status)
            
        json_output = {'raw_string': self.string, 
                       'sanitized_string': ', '.join(url_strings), 
                       'condition': condition, 
                       'timestamp': datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S"), 
                       'URLs': url_output}
        return json_output

    

    