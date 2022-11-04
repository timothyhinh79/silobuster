import sys

# abs_dir = input('Enter the absolute directory to the db_wrapper library')
abs_dir = '/home/jamey/hackathon/microservice/source/service'
if abs_dir[-1] != '/':
    abs_dir += '/'
sys.path.append(abs_dir)
sys.path.append(f'{abs_dir}connectors/')
sys.path.append(f'{abs_dir}feeds/')



from libs.connectors.abstract_connector import AbstractConnector

import psycopg2

class PostgresConnector(AbstractConnector):
    
    def __init__(self, db: str, username: str, password: str, host: str, port: int):
        self.__db = db
        self.__username = username
        self.__password = password
        self.__host = host

        self.__conn = psycopg2.connect(
            database=db,
            user=username,
            password=password,
            host=host,
            port=port
        )


    @property
    def db(self) -> str:
        return self.__db


    @property
    def username(self) -> str:
        return self.__username


    @property
    def password(self) -> str:
        return self.__password


    @property
    def host(self) -> str:
        return self.__host


    @property
    def port(self) -> str:
        return self.__port


    @property
    def connection(self) -> object:
        return self.__conn


    @property
    def is_alive(self) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute("select version()")
            data = cursor.fetchone()
        
        if data is not None or data != '':
            return True

        return False
