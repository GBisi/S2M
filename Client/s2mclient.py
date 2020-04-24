import requests

class S2MClient:

    def __init__(self, url):

        self._url = url

    def list(self):
        r = requests.get(self._url)
        if r.status_code == requests.codes.ok:
            return r.json(),r.status_code
        return None,r.status_code

    def get_obj(self, bucket, obj):

        url = self._url+"/"+bucket+"/"+obj
        r = requests.get(url)
        if r.status_code == requests.codes.ok:
            return r.json(),r.status_code
        return None,r.status_code

    def get_metadata(self, bucket,obj=None):
        
        url = self._url+"/"+bucket
        if obj is not None:
            url += "/"+obj+"/metadata"
        r = requests.get(url)
        if r.status_code == requests.codes.ok:
            return r.json(),r.status_code
        return None,r.status_code

    def add_bucket(self, bucket):
        url = self._url+"/"+bucket
        r = requests.post(url)
        if r.status_code == requests.codes.ok:
            return r.json(),r.status_code
        return None,r.status_code

    def add_obj(self, bucket, obj, data):
        url = self._url+"/"+bucket+"/"+obj
        if type(data) == dict:
            r = requests.post(url,json=data)
        else:
            r = requests.post(url,data=data)
        if r.status_code == requests.codes.created:
            return r.json(),r.status_code
        return None,r.status_code

    def add(self, bucket, obj=None, data=None):
        if obj is None:
            return self.add_bucket(bucket)
        return self.add_obj(bucket,obj,data)

    def update_obj(self, bucket, obj, data):
        url = self._url+"/"+bucket+"/"+obj
        if type(data) == dict:
            r = requests.put(url,json=data)
        else:
            r = requests.put(url,data=data)
        if r.status_code == requests.codes.ok:
            return r.json(),r.status_code
        return None,r.status_code

    def delete(self, bucket,obj=None):
        url = self._url+"/"+bucket
        if obj is not None:
            url += "/"+obj

        code = requests.delete(url).status_code 
        
        return code==204,code
        