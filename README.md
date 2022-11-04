# silobuster-dedupe

JUPYTER-NOTEBOOK

NEW
====

## Installation Notes:

Python3.8 is not compatible. I did not test 3.9. However, it runs on 3.10. Install Pandas. I am using 1.5.1. This should be enough to install pandas-dedupe. If not, just use the container provided in the dockerfiles. Navigate to the dockerfiles/deduper folder and edit entrypoint-deduper.sh. Uncomment out the tail -f /dev/null command and that will keep the container open so you can use it. Sorry about jupyter in this case. You can still use the terminal to label your data.

## Training

### pandas-dedupe_main.ipynb

Use this jupyter notebook file to train data. It will read from the "main" feed (you will see that in the code), which is the "organization" table. It will prompt you to label the data as
duplicates. This will output the following files in the same directory:

1. dedupe_dataframe_learned_settings
2. dedupe_dataframe_training.json

## Production

1. Copy the above files into the "deduper" folder.
2. Navigate to the dockerfiles folder. Issue "docker-compose up -d --build" or "docker compose up -d --build", depending on your version of Docker.
3. To run the container and have it shut down in Airflow, make sure to comment out the "tail -f /dev/null" command. It is in dockerfiles/deduper/entrypoint-deduper.sh file.



OLD
====
## How to use
The main files you should know about are:

### build_training_data.ipynb

### deduper_class_test.ipynb

### deduper_train_from_file.ipynb


Python ##########################################
From feeds.postgres_feed:
You will want to import the PostgresConnector from connectors. Initialize a connection. You can check the connection with (my_conn.is_alive)
Pass that to the PostgresFeed class. Only use the MyFeed.from_manual() alternative initialization for now. Nothing else has been tested.
'''
This is the main object to feed to dedupe. It does everything for now and is experimental

Initialization: Currently only use alternative constructor: from_manual_definition.
PostgresFeed.from_manual_definition:
REQUIRES: connector: PostgresConnector, query: string, column_definition: list of dictionaries, primary_key: dictionary

column_definition EXAMPLE:
```python

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

```


At the point, you will enter training mode. The dedupe instance will give you pairs it thinks is duplicates and you will train the model from your answers.

You are then to train the dedupe with:

```python

deduper.train()

```

This is the hickup. According to the error thrown, an "index" needs to be created. From online examples, that is not the use case. Any help is appreciated.

**WORKING EXAMPLE**

The working example is using the pandas_dedupe library. With the exact same setup, it does not throw an "index" error.

```python

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

```