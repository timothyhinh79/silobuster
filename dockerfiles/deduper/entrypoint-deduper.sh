#! /bin/bash

cd /var/www/deduper
python3 deduper.py

# Tail will keep the container open for testing purposes
# tail -f /dev/null

