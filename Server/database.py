from abc import ABC, abstractmethod 

class Database(ABC):
    
    @abstractmethod
    def has(self, bucket, obj=None):
        pass

    @abstractmethod
    def list(self):
        pass
    
    @abstractmethod
    def get(self, bucket, obj, extension="txt"):
        pass

    @abstractmethod
    def make(self, bucket):
        pass

    @abstractmethod       
    def store(self, bucket, obj, data, extension = "txt"):
        pass

    @abstractmethod
    def delete(self, bucket, obj=None, extension="txt"):
        pass
        