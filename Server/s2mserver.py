import sys

from flask import Flask, request, abort, jsonify
from flask_cors import CORS

import json
import time
from datetime import datetime

import configparser

from fsdatabase import FSDatabase

IP = "127.0.0.1"
PORT = 5233
DEBUG = True
SRC = './data'

app = Flask(__name__)
CORS(app)

db = FSDatabase(SRC)

def debug(string):

    if DEBUG:
        print(string)

def make_metadata(id_,bucket,obj=None,size=0):
    
    now = str(datetime.now())
    path = IP+":"+str(PORT)+"/"+bucket
    metadata =  {
        "created":now,
        "last_update":now,
        "id":id_,
        "size":size,
        "href":path,
        "metadata":path
    }
    if obj is not None:
        metadata["href"]+="/"+obj
        metadata["metadata"]+="/"+obj+"/metadata"
        metadata["bucket"] = path
    else:
        metadata["objects"] = []

    return set_metadata(metadata,bucket,obj)

def delete_metadata(bucket,obj=None):
    if obj is None:
        obj = "metadata"
    else:
        metadata = get_metadata(bucket,obj)
        bucket_metadata = get_metadata(bucket)
        bucket_metadata["size"] -= metadata["size"]
        bucket_metadata["objects"].remove(metadata["id"])
        set_metadata(bucket_metadata,bucket)
    if db.delete(bucket,obj,"metadata"):
        return True
    else:
        return False

def get_metadata(bucket, obj=None):
    if obj is None:
        obj = "metadata"
    data = db.get(bucket,obj,"metadata")
    if data is not None:
        return json.loads(data)
    else:
        return None

def set_metadata(metadata,bucket,obj=None):

    metadata["last_update"] = str(datetime.now())

    if obj is None:
        if db.store(bucket,"metadata",json.dumps(metadata),"metadata") is None:
            return None
        else:
            return metadata
    else:
        bucket_metadata = get_metadata(bucket)

        if bucket_metadata is None:
            return None

        bucket_metadata["size"] += metadata["size"]
        bucket_metadata["objects"].append({"id":metadata["id"],"href":metadata["href"]})

        if not set_metadata(bucket_metadata,bucket):
            return None

        if db.store(bucket,obj,json.dumps(metadata),"metadata") is None:
            return None
        else:
            return metadata

def update_metadata(bucket,obj,size):

    now = str(datetime.now())

    metadata = get_metadata(bucket,obj)
    bucket_metadata = get_metadata(bucket)

    if bucket_metadata is None:
        return None

    bucket_metadata["size"] -= metadata["size"]
    metadata["size"] = size
    bucket_metadata["size"] += metadata["size"]

    bucket_metadata["last_update"] = now
    metadata["last_update"] = now

    if db.store(bucket,"metadata",json.dumps(bucket_metadata),"metadata") is None:
        return None

    if db.store(bucket,obj,json.dumps(metadata),"metadata") is None:
        return None
    else:
        return metadata

@app.route('/', methods=['GET'])
def get_all_buckets():
    debug("GET ALL")

    l = db.list()
    data = []

    for e in l:
        data.append({"id":e,"href":get_metadata(e)["href"]})

    if data is None:
        debug("GET ALL Error")
        abort(500)
    
    return jsonify(data)

@app.route('/<bucket>', methods=['GET'])
def get_bucket(bucket):

    debug("GET "+bucket)

    data = get_metadata(bucket)

    if data is None:
        debug("GET "+bucket+" not found")
        abort(404)
    
    return jsonify(data)

@app.route('/<bucket>/<obj>', methods=['GET'])
def get_obj(bucket,obj):

    debug("GET "+bucket+"/"+obj)

    if obj == "metadata":
        metadata = get_metadata(bucket)
        if metadata is not None:
            debug("GET "+bucket+"/"+obj+": metadata error")
            return metadata
        else:
            abort(500)

    data = db.get(bucket,obj)

    if data is None:
        debug("GET "+bucket+"/"+obj+" not found")
        abort(404)

    metadata = get_metadata(bucket,obj)

    metadata["data"] = data

    return metadata

@app.route('/<bucket>/<obj>/metadata', methods=['GET'])
def get_obj_metadata(bucket,obj):

    debug("GET "+bucket+"/"+obj+"/metadata")

    metadata = get_metadata(bucket,obj)

    if metadata is None:
        debug("GET "+bucket+"/"+obj+"/metadata not found")
        abort(404)
    
    return metadata

@app.route('/<bucket>', methods=['POST'])
def add_bucket(bucket):

    debug("POST "+bucket)

    if db.has(bucket):
        debug("POST "+bucket+": exist")
        abort(400)

    if not db.make(bucket):
        debug("POST "+bucket+" mkdir error")
        abort(500)

    metadata = make_metadata(bucket,bucket)

    if metadata is None:
        debug("POST "+bucket+"/metadata store error")
        abort(500)

    return jsonify(metadata),201

@app.route('/<bucket>/<obj>', methods=['POST'])
def add_obj(bucket,obj):

    debug("POST "+bucket+"/"+obj)

    if not db.has(bucket):
        debug("POST "+bucket+": not exist")
        abort(404)

    if obj == "metadata":
        abort(403)

    if db.has(bucket,obj):
        debug("POST "+bucket+"/"+obj+": exist")
        abort(400)

    data = request.data

    if data is None:
        data = ""

    size = db.store(bucket,obj,data)

    if size is None:
        abort(500)

    metadata = make_metadata(obj,bucket,obj,size)

    if metadata is None:
        debug("POST "+bucket+"/"+obj+": metadata store error")
        abort(500)

    return jsonify(metadata),201

@app.route('/<bucket>/<obj>', methods=['PUT'])
def update_obj(bucket,obj):

    debug("PUT "+bucket+"/"+obj)

    if not db.has(bucket):
        debug("PUT "+bucket+": not exist")
        abort(404)

    if obj == "metadata":
        abort(403)

    if not db.has(bucket,obj):
        debug("PUT "+bucket+"/"+obj+": not exist")
        abort(404)

    data = request.data

    if data is None:
        data = ""

    size = db.store(bucket,obj,data)

    if size is None:
        abort(500)

    metadata = update_metadata(bucket,obj,size)

    return jsonify(metadata),200

@app.route('/<bucket>/<obj>', methods=['DELETE'])
def delete_obj(bucket,obj):

    debug("DELETE "+bucket+"/"+obj)

    if not db.has(bucket):
        debug("DELETE "+bucket+": not exist")
        abort(404)

    if obj == "metadata":
        abort(403)

    if not db.has(bucket,obj):
        debug("DELETE "+bucket+"/"+obj+": not exist")
        abort(404)

    delete_metadata(bucket,obj)

    if db.delete(bucket,obj):
        return '',204
    else:
        abort(500)

@app.route('/<bucket>', methods=['DELETE'])
def delete_bucket(bucket):

    debug("DELETE "+bucket)

    if not db.has(bucket):
        debug("DELETE "+bucket+": not exist")
        abort(404)

    if db.delete(bucket):
        return '',204
    else:
        abort(500)


def config():
    global IP
    global PORT
    global SRC
    global DEBUG
    parser = configparser.ConfigParser()
    if parser.read('./config.ini') != []:
        config = parser["DEFAULT"]["CONFIG"]
        IP = parser[config]["IP"]
        PORT = parser[config]["PORT"]
        SRC = parser[config]["SRC"]
        DEBUG = parser[config]["DEBUG"]

if __name__ == '__main__':
    config()
    print("S2M ONLINE @ "+IP+":"+str(PORT))
    app.run(host=IP,port=PORT,debug=DEBUG)
