# Sanitize URLs

## Assigned to:
- Shahabul Khan, Timothy Hinh

## Description
The task is to sanitize contact info (phone numbers and emails) and URLs in a given string and to log errors that occur during the sanitization.

## Requirements

### Objectives
- URL sanitization:
    - Extracts all URL patterns within a given string
    - Retrieves the status code (e.g. 200, 404, etc.) for each URL and the embedded root URL. 
    - Also logs the following scenarios:
        - Entire string is URL
        - String contains one URL
        - String contains no URLs
        - String contains multiple URLs
- Email and phone number sanitization
    - Recognize data in many different formats.
    - Be "on-the-fly" configurable to watch for custom formats (ie, pass in regex as args?).
    - Recognize multiple phone numbers in an entry and parse them into separate rows in HSDS.
    - Recognize supplemental data and store in appropriate fields, ie phone extensions in  `phone.extention`.
    - If the `type` for a phone record cannot be determined, flag for review by data manager.
    - Recognize multiple emails or urls in an entry and pull out the first one to store.
    - Coerce all data into ITU E.123 format for international numbers. EDIT: we decided at the event to use domestic notation.

## Constraints
- The code does not identify if part of a URL match exists, other than the root URL. It therefore fails to validate full URL matches with extra characters that might be appended after a the URL (e.g. a comma, paranthesis, period, etc.).
    - Ex: if a URL match is 'https://google.com/imghp,' (with an extra comma) then only the root URL is confirmed as a valid address.

## Resources
- Regex pattern used for identifying valid URLs comes from here: https://gist.github.com/dperini/729294
- Logic for parallelizing the sanitize_urls() method comes from here: https://superfastpython.com/threadpoolexecutor-validate-links/#Validate_Multiple_Links_Concurrently
- Libraries:
    - re
    - requests
    - urllib.parse
    - concurrent.futures
    - dnspython==2.2.1
    - email-validator==1.2.1
    - greenlet==1.1.3
    - idna==3.3
    - phonenumbers==8.12.54
    - psycopg2==2.9.3
    - SQLAlchemy==1.4.40


