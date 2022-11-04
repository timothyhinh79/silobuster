import sys

# abs_dir = input('Enter the absolute directory to the db_wrapper library')
abs_dir = '/home/jamey/hackathon/microservice/source/service'
if abs_dir[-1] != '/':
    abs_dir += '/'
sys.path.append(abs_dir)
sys.path.append(f'{abs_dir}connectors/')



from abc import ABC, abstractmethod


class AbstractFeed(ABC):


    @property
    @abstractmethod
    def connector(self):
        pass


    @property
    @abstractmethod
    def variable_definition():
        pass