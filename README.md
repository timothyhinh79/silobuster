# silobuster-dedupe

JUPYTER-NOTEBOOK
DONT USE test_connector!
USE deduper_class_test


Python ##########################################
From feeds.postgres_feed:
You will want to import the PostgresConnector from connectors. Initialize a connection. You can check the connection with (mym_conn.is_alive)
Pass that to the PostgresFeed class. Only use the MyFeed.from_manual() alternative initialization for now. Nothing else has been tested.
'''
This is the main object to feed to dedupe. It does everything for now and is experimental

Initialization: Currently only use alternative constructor: from_manual_definition.
PostgresFeed.from_manual_definition:
REQUIRES: connector: PostgresConnector, query: string, column_definition: list of dictionaries, primary_key: dictionary

column_definition EXAMPLE:
'''
col_def = [
    {"field": "name", "type": "String"},
    {"field": "address_1", "type": "String", "has missing": True},
    {"field": "address_2", "type": "Exists", "has missing": True},
    {"field": "city", "type": "String", "has missing": True},
    {"field": "state_province", "type": "String", "has missing": True},
    {"field": "postal_code", "type": "Exists", "has missing": True},
    {"field": "url", "type": "Exists", "has missing": True}
]
'''
You need to have "field" and "type" keys.
primary_key: is a string of the column name for id


**EXAMPLE USAGE with dedupe library**
'''
from connectors.postgres_connector import PostgresConnector
from feeds.postgres_feed import PostgresFeed

import dedupe

pg_conn = PostgresConnector(db='jameycdb', username='jameyc', password='UXZSXXXSFZeU8XKw', host='silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com', port=25060)

select_qry = "SELECT id, name, address_1, address_2, city, state_province, postal_code, url, description, duplicate_id, duplicate_type FROM organizations_normalized WHERE name IS NOT NULL AND NOT name = '' AND address_1 IS NOT NULL AND NOT address_1 = '' AND city IS NOT NULL AND NOT city = ''"
raw_def = [
    {"field": "name", "type": "String"},
    {"field": "address_1", "type": "String", "has missing": True},
    {"field": "address_2", "type": "Exists", "has missing": True},
    {"field": "city", "type": "String", "has missing": True},
    {"field": "state_province", "type": "String", "has missing": True},
    {"field": "postal_code", "type": "Exists", "has missing": True},
    {"field": "url", "type": "Exists", "has missing": True}
]


pg_feed = PostgresFeed.from_manual(connector=pg_conn, query=select_qry, column_definition=raw_def, primary_key='id')

deduper = dedupe.Dedupe(pg_feed.variable_definition)
deduper.prepare_training(pg_feed.formatted_data)
dedupe.convenience.console_label(deduper)

'''

At the point, you will enter training mode. The dedupe instance will give you pairs it thinks is duplicates and you will train the model from your answers.

You are then to train the dedupe with:

'''
deduper.train()
'''

This is the hickup. According to the error thrown, an "index" needs to be created. From online examples, that is not the use case. Any help is appreciated.

**WORKING EXAMPLE**

The working example is using the pandas_dedupe library. With the exact same setup, it does not throw an "index" error.

'''
import pandas as pd
import pandas_dedupe

from connectors.postgres_connector import PostgresConnector
from feeds.postgres_feed import PostgresFeed


pg_conn = PostgresConnector(db='yourdb', username='user', password='pass', host='host', port=25060)

select_qry = "SELECT id, name as company_name, address_1, address_2, city, state_province, postal_code, url, description, duplicate_id, duplicate_type FROM organizations_normalized WHERE name IS NOT NULL AND NOT name = '' AND address_1 IS NOT NULL AND NOT address_1 = '' AND city IS NOT NULL AND NOT city = ''"
raw_def = [
    {"field": "company_name", "type": "String"},
    {"field": "address_1", "type": "String", "has missing": True},
    {"field": "address_2", "type": "Exists", "has missing": True},
    {"field": "city", "type": "String", "has missing": True},
    {"field": "state_province", "type": "String", "has missing": True},
    {"field": "postal_code", "type": "Exists", "has missing": True},
    {"field": "url", "type": "Exists", "has missing": True}
]


pg_feed = PostgresFeed.from_manual(connector=pg_conn, query=select_qry, column_definition=raw_def, primary_key='id')

print (pg_feed.df.head(5))

df_final = pandas_dedupe.dedupe_dataframe(pg_feed.df, [
    ('company_name', 'String'),
    ('address_1', 'String'), 
    ('address_2','String', 'has missing'), 
    ('city','String','has missing'), 
    ('state_province','String','has missing'), 
    ('postal_code','String','has missing'), 
    ('url','String','has missing'), 
    ('description','String','has missing')
])

df_final.to_csv('deduplication_output.csv')
'''