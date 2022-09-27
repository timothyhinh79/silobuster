# Sanitize URLs

## Assigned to:
- Shahabul Khan, Timothy Hinh

## Description
The task is to extract URLs in a given string and to log whether each URL is valid. This will be used to sanitize data containing URLs.

## Requirements

### Objectives
- Extracts all URL patterns within a given string
- Retrieves the status code (e.g. 200, 404, etc.) for each URL and the embedded root URL. 
- Also logs the following scenarios:
    - Entire string is URL
    - String contains one URL
    - String contains no URLs
    - String contains multiple URLs

## Constraints
- The code does not identify if part of a URL match exists, other than the root URL. It therefore fails to validate full URL matches with extra characters that might be appended after a the URL (e.g. a comma, paranthesis, period, etc.).
    - Ex: if a URL match is 'https://google.com/imghp,' (with an extra comma) then only the root URL is confirmed as a valid address.

## Resources
- Regex pattern used for identifying valid URLs comes from here: https://gist.github.com/dperini/729294
- Logic for parallelizing the sanitize_urls() method comes from here: https://superfastpython.com/threadpoolexecutor-validate-links/#Validate_Multiple_Links_Concurrently
- Libraries:
    - re
    - requests
    - urlparse
    - concurrent.futures


