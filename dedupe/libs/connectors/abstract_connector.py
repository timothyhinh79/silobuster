from abc import ABC, abstractmethod


class AbstractConnector(ABC):
    

    @property
    @abstractmethod
    def db(self) -> str:
        pass


    @property
    @abstractmethod
    def username(self) -> str:
        pass


    @property
    @abstractmethod
    def password(self) -> str:
        pass


    @property
    @abstractmethod
    def host(self) -> str:
        pass


    @property
    @abstractmethod
    def port(self) -> str:
        pass


    @abstractmethod
    def connection(self) -> object:
        pass


    @property
    @abstractmethod
    def is_alive(self) -> bool:
        pass