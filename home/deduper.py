'''
The dedupe library will initiate training via terminal is settings are not supplied. the settings files are
ALWAYS named:
1. dedupe_dataframe_learned_settings
2. dedupe_dataframe_training.json

By including these files in this folder, you are shipping your learned settings. Then the library will properly
do its job. In the event of an error, check that these files are included first.

PostgresFeed.from_main will be reading data from the:
1. table: organization

The columns shape the data.
1. String cannot have missing values. uses Levenstein string distance.
2. Text can have missing values. Uses cosine similarity metric.

You will most likely get errors if the data is not the same labels.

'''

import sys
import os

# For testing purposes. Running from this file add import locations
if __name__ == "__main__":
    sys.path.append('.')
    sys.path.append('..')

import pandas as pd
import pandas_dedupe
import csv

from feeds.postgres_feed import PostgresFeed

pg_feed = PostgresFeed.from_main()
print(pg_feed.query)
print (pg_feed.df.head(5))

# The Levenshtein distance is used for fields identified as 'String'. However, it appears we 
# can't use "has missing" with Strings. Where fields may be empty, use type of "Test" instead.
df_final = pandas_dedupe.dedupe_dataframe(pg_feed.df, [
    ('o_name', 'String'),
    ('address_1', 'String'), 
    ('address_2','Text', 'has missing'), 
    ('city','Text','has missing'), 
    ('state_province','Text','has missing'), 
    ('postal_code','Text','has missing'), 
    ('o_url','Text','has missing'), 
    #('l_description','Text','has missing')
])

# Write the deduplicated data
df_final.to_csv('deduplication_results.csv')

'''
The overall concept is to read the deduplicated data line by line and write to the db.
Along the way, enrich the data.
'''

with open('deduplication_results.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    next(reader) # skip header row

    for row in reader:
        primary_key, organization_id, o_name, o_url, location_id, l_name, latitude, longitude, address_1, address_2, city, state_province, postal_code, cluster_id, confidence = row
        print (row)        



