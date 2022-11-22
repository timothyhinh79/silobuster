import time
from classes.multithreader import Multithreader

def test_multithreader():
    # create method that takes 5 seconds
    def test_method(input):
        time.sleep(5)
        return 'return value: ' + input

    inputs = ['a','b','c','d','e','f','g','h','i','j']
    multithreader = Multithreader(test_method, inputs, 10)

    start = time.time()
    results = multithreader.run()
    end = time.time()
    runtime = end - start

    # normally, would take 5 * 10 = 50 seconds to run test_method on all 10 inputs
    # with Multithreader working on 10 sessions, runtime should be around 5 seconds
    assert runtime < 6
    assert results == ['return value: ' + input for input in inputs]