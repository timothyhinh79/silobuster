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

import sys

import psycopg2
import dedupe
from psycopg2 import extras
import csv
import pandas as pd

# abs_dir = input('Enter the absolute directory to the db_wrapper library')
abs_dir = '/home/jamey/hackathon/microservice/source/service'
if abs_dir[-1] != '/':
    abs_dir += '/'
sys.path.append(abs_dir)
sys.path.append(f'{abs_dir}connectors/')
sys.path.append(f'{abs_dir}feeds/')


from feeds.abstract_feed import AbstractFeed
from connectors.postgres_connector import PostgresConnector


class PostgresFeed(AbstractFeed):
    
    main_key = 'organization_id'

    main_qry = select_qry = '''
        SELECT
            o.id as organization_id,
            o.name as o_name,
            o.url as o_url,
            l.id as location_id,
            l.name as l_name,
            l.latitude,
            l.longitude,
            a.address_1,
            a.address_2,
            a.city,
            a.state_province,
            a.postal_code
        FROM organization o
        JOIN location l on l.organization_id = o.id
        JOIN address a on a.location_id = l.id
        WHERE type = 'physical'
        '''

    def __init__(self, connector: object, query: str, column_definition: list=None, primary_key: str=None, lib_definition: list=None):
        self.__connector = connector
        self.__query = query
        self.__column_definition = column_definition
        self.__primary_key = primary_key
        # self.__lib_definition = lib_definition
        self.__primary_key = primary_key


        with connector.connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(query)
            self.__raw_data = cursor.fetchall() # list of tuples


        with connector.connection.cursor() as cursor:
            cursor.execute(query)
            self.__raw_data_tuples = cursor.fetchall() # list of tuples
        
        
    @classmethod
    def from_manual(cls, connector, query: str, column_definition: list, primary_key: str):
        if not isinstance(primary_key, str):
            raise TypeError("PostgresFeed: Initialization error (from_manual_definition): primary_key must be a string")

        return cls(connector=connector, query=query, column_definition=column_definition, primary_key=primary_key)
    
    
    @classmethod
    def from_main(cls, query: str=None, primary_key: str=None):
        my_conn = PostgresConnector(db='defaultdb', username='doadmin', password='AVNS_2Lh_hY8r7RVKSfLwbJM', host='silobuster-dev-db-do-user-12298230-0.b.db.ondigitalocean.com', port=25060)
        my_qry = None
        my_key = None    
        if query is not None and primary_key is None:
            raise ValueError("PostgresFeed.from_main: You passed a query without specifying a primary key. Please pass a primary key as your second argument or primary_key keyword argument.")
        elif query is not None and primary_key is not None:
            my_qry = query
            my_key = primary_key
        else:
            my_qry = cls.main_qry
            my_key = cls.main_key

        return cls(connector=my_conn, query=my_qry, column_definition=None, primary_key=my_key)


    @property
    def columns(self):
        my_columns = [key for key in self.__raw_data[0].keys()]
        return my_columns


    def mangle_data(self, staging_table_name: str, callback: object, delete_old_data: bool=True):

        # create table
        columns_str = ", ".join(self.columns)

        columns_create_arr = [f"{s} text" for s in self.columns]
        columns_create_str = ", ".join(columns_create_arr)
        print (f"Creating table ({staging_table_name})...")
        create_qry = f'CREATE TABLE IF NOT EXISTS {staging_table_name} ({columns_create_str})'
        print ("Table created")

        with self.__connector.connection.cursor() as cursor:
            cursor.execute(create_qry)
            self.__connector.connection.commit()
        
        new_data = callback(self.__raw_data)

        parms_arr = ["%s" for s in self.columns]
        parms_str = ", ".join(parms_arr)
        insert_qry = f"INSERT INTO {staging_table_name} ({columns_str}) VALUES ({parms_str})"
        
        print ("Deleting previous data...")
        if delete_old_data:
            with self.__connector.connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {staging_table_name}")
                self.__connector.connection.commit()
            
        
        print (f"Inserting new data ({len(new_data)} rows)...")
        with self.__connector.connection.cursor() as cursor:
            for count, row in enumerate(new_data):
                
                cursor.execute(insert_qry, list(row.values()))
                self.__connector.connection.commit()

                #print (len(new_data) // count)
                #print (len(new_data) % count)
                if not count == 0:
                    if count % 20 == 0:
                        prog = count / len(new_data)
                        print (f"Progress: {prog}")
            
        return True
            


    @property
    def connector(self):
        return self.__connector


    @property
    def query(self):
        return self.__query


    @property
    def primary_key(self):
        return self.__primary_key


    @property
    def column_definition(self):
        return self.__column_definition


    @property
    def variable_definition(self):
        if self.__column_definition is not None and self.__primary_key is not None:
            return self.column_definition
            v_def = self.column_definition
            v_def.append(self.__primary_key)
            return v_def
        else: 
            raise TypeError("postgres_feed: PostgresFeed: requires that a column_definition and  must be passed")


    @property
    def raw_data(self):
        return self.__raw_data    
        

    @property
    def raw_data_tuples(self):
        return self.__raw_data_tuples


    @property
    def df(self):
        df = pd.DataFrame.from_records(self.raw_data)
        return df


    @property
    def data_file(self):
        for row in self.raw_data_tuples:

            with open('temp_data.txt') as csvfile:
                w = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                w.writerow(row)

        return w
        


    @property
    def formatted_data(self):
        new_dict = dict()
        for row in self.raw_data:
            primary_key = None
            vals_dict = dict()

            # get the primary key
            for row_name, row_value in row.items():
                if row_name == self.__primary_key:
                    primary_key = row_value
                    break
        
            for row_name, row_value in row.items():
                if row_name == self.__primary_key: # skip the primary key
                    continue
                for col_def in self.column_definition:
                    if row_name == col_def['field']:
                        vals_dict[col_def['field']] = row_value

            new_dict[primary_key] = vals_dict

        return new_dict

