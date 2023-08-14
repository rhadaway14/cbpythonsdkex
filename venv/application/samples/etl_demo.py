# internal modules
import datetime
import calendar
from datetime import timedelta
from pprint import pprint
import json
from time import time

# local modules
from resources.cluster import cluster_item

# external modules
from flask import request
from flask_restful import Resource
from http import HTTPStatus
import json
import requests

import couchbase.search as search



def items_etl(data):
    document_stuct == {
        "id": data.id,
        "desc": data.desc,
        "price": data.price,
        "name": data.name,
        "default_color": "red",
        "price_in_USD": data.price/ 'conversion number',
        "available_colors": [get_data_from_mysql("""Select 'colors' from some_table""")]
    }
    return document_struct

def upload_to_couchbase(bucket, scope, collection, doc):
    cluster_item.bucket(bucket).scope(scope).collection(collection).insert(doc)
    return {"status": "ok", "id": doc.id}, HTTPStatus.OK

def get_data_from_mysql(query):
    conn = myodb.obj(server("server1").odb("a_sample_db").table("sample_table"))
    result = conn.query(query)
    return result

query = """Select id, desc, price, name  from odb.a_sample_db.sample_table"""

mysqldata = get_data_from_mysql(query)

for data in mysqldata:
    upload_to_couchbase(items_etl(data))

