
# number of threads to run sanitize URLs in parallel
num_threads_default = 200 

# number of seconds for requests.get() to validate if a given URL exists
requests_timeout_default = 20 

# of seconds to reattempt validation of URLs that initially return 429 or 503
retry_after_default = 10 

# of maximum attempts to retrieve a valid status code for URLs initially returnning 429 or 503
max_attempts_default = 3 