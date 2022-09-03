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
[
    {"field": "organization", "type": "String},
    ...etc
]
You need to have "field" and "type" keys.
primary_key: is a string of the column name for id


EXAMPLE USAGE############################################################################################################################

from connectors.postgres_connector import PostgresConnector
from feeds.postgres_feed import PostgresFeed

import dedupe


pg_conn = PostgresConnector(db='defaultdb', username='jameyc', password='UXZSXXXSFZeU8XKw', host='silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com', port=25060)
select_qry = "SELECT id, name, address FROM what_location WHERE address is not null and not address = '' and name is not null and not name = ''"
# select_qry2 = "SELECT id, name, address FROM what_location"


raw_def = [
    {"field": "name", "type": "String"},
    {"field": "address", "type": "String"},
    
]


pg_feed = PostgresFeed.from_manual(connector=pg_conn, query=select_qry, column_definition=raw_def, primary_key='id')

# select_qry = "SELECT name, address FROM what_location WHERE address is not null and not address = '' and name is not null and not name = ''"
deduper = dedupe.Dedupe(pg_feed.variable_definition)

deduper.prepare_training(pg_feed.formatted_data, sample_size=100000, blocked_proportion=1)

distinct_pairs = []
while True:
    try:
        pair = deduper.uncertain_pairs()
        distinct_pairs.extend(pair)
    except IndexError as e:
        break
    except Exception as e:
        raise

deduper.mark_pairs({
    "match": [],
    "distinct": distinct_pairs
})

print (distinct_pairs)
deduper.train() # This is a big ol fat error! Good luck

############################################################################################################################
'''
