import logging
import sys
from sanitization_code.sanitize_emails import *

logging.basicConfig(
                    stream = sys.stdout, 
                    filemode = "w",
                    format = "%(levelname)s %(asctime)s - %(message)s", 
                    level = logging.DEBUG)

logger = logging.getLogger()

from sanitization_code.sanitize_phone_nums import * 

loompa = ('a',1,2,3,4)

exampstr = "1-800-RUN-AWAY/1-800-786-2929 24/7 toll free #"

exampstr.split('/') 

def tester():
    tested_numbers = []
    for chunk in exampstr.split('/'):

        tested_numbers.append(pluck_phone_num(phone_regex, chunk, logger))

raw_phone_str = '(360) 398-0223 or 888-360-0223'


