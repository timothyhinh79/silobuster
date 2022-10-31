# as_completed and ThreadPoolExecutor used to run sanitize_urls() method on multiple strings in parallel for efficiency
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

from sanitization_code.url_sanitization.url_sanitization_params import num_threads_default, requests_timeout_default, retry_after_default, max_attempts_default
from sanitization_code.url_sanitization.sanitize_urls import sanitize_urls


def sanitize_urls_parallel(strings, num_threads = num_threads_default, timeout = requests_timeout_default, retry_after = retry_after_default, max_attempts = max_attempts_default):
    """Runs sanitize_urls() on each string in a given list in a parallelized procedure
    
    Parameters:
        strings (list): List of raw strings with URL(s) 
        num_threads (int): number of workers to run sanitize_urls simultaneously 
            (e.g. if num_threads = 100, this method will process 100 strings at a time)

    Returns:
        List of JSONs returned for each string by the sanitize_urls() method 
    """

    executor = setup_executor(strings, num_threads)  
    sessions = run_executor(executor, strings, timeout, retry_after, max_attempts)
    sanitized_urls = get_results(sessions)
    return sanitized_urls

# set up executor with a certain number of threads to run multiple sessions for sanitizing URLs
def setup_executor(strings, num_threads):
    n_threads = min(num_threads, len(strings))
    executor = ThreadPoolExecutor(n_threads)
    return executor

# run sessions to sanitize URLs
def run_executor(executor, strings, timeout, retry_after, max_attempts):
    sessions = {}

    # establishing parallel sessions/processes to run the sanitize_urls method, assigned to an index number to keep track of the ordering of each result
    for index, string in enumerate(strings):
        session = executor.submit(sanitize_urls, string, timeout, retry_after, max_attempts)
        sessions[session] = index

    return sessions

# extract (ordered) results from completed sessions
def get_results(sessions):
    responses = [] # responses are appended more or less randomly depending on when the corresponding session completes
    indices = [] # used to keep track of original ordering of each response
    for session in as_completed(sessions):
        responses.append(session.result())
        indices.append(sessions[session])

    # sorting the responses based on the original ordering of the corresponding strings
    return [response for _, response in sorted(zip(indices, responses))] 
