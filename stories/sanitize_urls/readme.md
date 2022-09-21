# Sanitize URLs

## Assigned to:
- Shahabul Khan, Timothy Hinh

## Description
Extracts valid URLs that exist within a given string. Returns an empty list if there is no valid URL.

## Requirements
Libraries:
- re
- requests

## Constraints
- The code runs slowly if there are many non-space characters following the end of a valid URL string (e.g. "https://www.google.com/imghp________")

## Resources
- Regex pattern used for identifying valid URLs comes from here: https://gist.github.com/dperini/729294


