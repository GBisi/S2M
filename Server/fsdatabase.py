import os
import json
from shutil import rmtree

class FSDatabase:

    def __init__(self, path):

        self._path = path 

        if not os.path.isdir(path):
            os.mkdir(path)
        

    def has(self, bucket, obj=None):

        try:

            path = self._path+"/"+bucket

            if not os.path.isdir(path):
                return False

            if obj is not None:

                path+="/"+obj+".txt"

                try:
                    open(path)
                    return True
                except:
                    return False

            return True
        except:
            return False

    def list(self):
        try:
            return os.listdir(self._path)
        except:
            return None

    def get(self, bucket, obj, extension="txt"):

        path = self._path+"/"+bucket+"/"+obj+"."+extension

        try:
            file = open(path)
            return file.read()
        except:
            return None

    def make(self, bucket):
        try:
            path = self._path+"/"+bucket
            os.mkdir(path)
            return True
        except:
            False

        
    def store(self, bucket, obj, data, extension = "txt"):
    
        try:
            path = self._path+"/"+bucket+"/"+obj+"."+extension
            if type(data) == bytes:
                file = open(path,'wb')
            else:
                file = open(path,'w')
            
            file.write(data)

            return len(data)
        except:
            return None

    def delete(self, bucket, obj=None, extension="txt"):

        try:
            if not self.has(bucket,obj):
                return False

            path = self._path+"/"+bucket

            if obj is not None:
                path += "/"+obj+"."+extension
                os.remove(path)
            else:
                rmtree(path, ignore_errors=True)

            return True

        except:
            return False
        