psql -h silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com -U jameyc defaultdb -p 25060
UXZSXXXSFZeU8XKw



from postgres_connector import PostgresConnector

pg_conn = PostgresConnector(db='jameycdb', username='jameyc', password='UXZSXXXSFZeU8XKw', host='silobuster-db-do-user-12298230-0.b.db.ondigitalocean.com', port=25060)

select_qry = "SELECT name, address FROM what_location WHERE address is not null and not address = '' and name is not null and not name = ''"

