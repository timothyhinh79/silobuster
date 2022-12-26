# Helper Classes

## Description
Infokind encodes specific fields that can be sanitized (e.g. URL, email, phone)

Multithreader and Src2Dest were designed to facilitate abstraction of the below tasks:
    - Multithreader: parallelizing defined methods across a designated number of threads for faster execution
        - primarily used for URL validation (making requests.get() calls for hundreds/thousands of URLs is time-consuming)
    - Src2Dest: applying sanitization logic to data in a given Postgres database
        - defined around source-dest-mapping argument for run_sanitization.py
        - flexibly deals with updating original table, or creating a new table with the sanitized data, based on user input
