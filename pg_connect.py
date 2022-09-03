import psycopg2
import os

#establishing the connection
def get_session():
    conn = psycopg2.connect(
        database=os.environ.get('PG_DB'), user=os.environ.get('PG_USER'), password=os.environ.get('PG_PASSWORD'), host=os.environ.get('PG_HOST'), port=os.environ.get('PG_PORT')
    )
    return conn
    
def get_source_session():
    conn = psycopg2.connect(
        database='jameycdb',
        user='jameyc',
        password='UXZSXXXSFZeU8XKw',
        host='silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com',
        port=25060
    )

    return conn


def get_test_session():
    conn = psycopg2.connect(
        database='defaultdb',
        user='jameyc',
        password='UXZSXXXSFZeU8XKw',
        host='silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com',
        port=25060
    )

    return conn


    
def check_test_connection():
    #Creating a cursor object using the cursor() method
    with get_test_session().cursor() as cursor:

        #Executing function using the execute() method
        cursor.execute("select version()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        print("Connection established to: ",data)

    return f"Connection established to: {data}"

    
def check_source_connection():
    #Creating a cursor object using the cursor() method
    with get_source_session().cursor() as cursor:

        #Executing function using the execute() method
        cursor.execute("select version()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        print("Connection established to: ",data)

    return f"Connection established to: {data}"


def check_connection():
    #Creating a cursor object using the cursor() method
    with get_session().cursor() as cursor:

        #Executing function using the execute() method
        cursor.execute("select version()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        print("Connection established to: ",data)

    return f"Connection established to: {data}"
