# as_completed and ThreadPoolExecutor used to run sanitize_urls() method on multiple strings in parallel for efficiency
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

class Multithreader:

    def __init__(self, method, inputs, num_threads):
        self._method = method
        self._inputs = inputs
        self._num_threads = num_threads

    @property
    def method(self):
        return self._method

    @property
    def inputs(self):
        return self._inputs

    @property
    def num_threads(self):
        return self._num_threads

    
    def setup_executor(self):
        """Set up executor with a certain number of threads to run multiple sessions"""
        n_threads = min(self.num_threads, len(self.inputs))
        executor = ThreadPoolExecutor(n_threads)
        return executor

    def run_executor(self, executor):
        """Run sessions in parallel"""
        sessions = {}

        # establishing parallel sessions/processes to run the sanitize_urls method, assigned to an index number to keep track of the ordering of each result
        for index, input in enumerate(self.inputs):
            session = executor.submit(self.method, input)
            sessions[session] = index

        return sessions

    def get_results(self, sessions):
        """Extract (ordered) results from completed sessions"""
        responses = [] # responses are appended more or less randomly depending on when the corresponding session completes
        indices = [] # used to keep track of original ordering of each response
        for session in as_completed(sessions):
            responses.append(session.result())
            indices.append(sessions[session])

        # sorting the responses based on the original ordering of the corresponding strings
        return [response for _, response in sorted(zip(indices, responses))] 

    def run(self):
        """Run method on inputs concurrently across a given number of sessions"""
        executor = self.setup_executor()  
        sessions = self.run_executor(executor)
        results = self.get_results(sessions)
        return results
    


