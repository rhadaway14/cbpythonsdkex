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


def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

class Root(Resource):
    """Root resource class to confirm root connection"""

    def get(self):
        try:
            message = {"status": "connected"}
            return message, HTTPStatus.OK
        except Exception as e:
            return e, HTTPStatus.NOT_FOUND


class Buckets(Resource):
    """Bucket resource class to return list of buckets within a cluster"""

    def get(self):
        try:
            buckets = [{'flush_enabled': bucket['flush_enabled'],
                        'max_ttl': bucket['max_ttl'],
                        'name': bucket['name'],
                        'num_replicas': bucket['num_replicas'],
                        'ram_quota_mb': bucket['ram_quota_mb'],
                        'replica_index': bucket['replica_index']
                        } for bucket in cluster_item.buckets().get_all_buckets()]
            return {"buckets": buckets}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Scopes(Resource):
    """Scope resource class to return list of scopes within a bucket"""

    def get(self, bucket):
        try:
            scope_items = cluster_item.bucket(bucket)
            scopes = scope_items.collections().get_all_scopes()
            scope_names = []
            for _scope in scopes:
                # pprint(_scope.__dict__)
                scope_names.append(_scope.__dict__['_name'])
            # print(scope_names)
            return {"scopes": scope_names}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Collections(Resource):
    """Collection resource class to return list of collections within a scope"""

    def get(self, bucket, scope):
        try:
            bucket_item = cluster_item.bucket(bucket)
            scope_items = bucket_item.collections().get_all_scopes()
            for scope_item in scope_items:
                collections = [item.name for item in scope_item.collections if item.__dict__['_scope_name'] == scope]
                if len(collections) != 0:
                    break
            return {"collections": collections}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Documents(Resource):
    """Document resource class to return list of documents within a collection"""

    def get(self, bucket, scope, collection):
        try:
            bucket_item = cluster_item.bucket(bucket)
            scope_items = bucket_item.collection(collection)
            print(scope_items.__dict__)
            docs = scope_items.__dict__
            # for scope_item in scope_items:
            #     collections = [item.name for item in scope_item.collections if item.__dict__['_scope_name'] == scope]
            #     if len(collections) != 0:
            #         break
            return {"documents": docs}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND




class City(Resource):
    """Search resource class to return list of cities given a wildcard search value"""
    
    @timer_func
    def get(self, query):
        try:
            result = cluster_item.search_query('search_city', search.WildcardQuery(f"{query}*"))

            # for row in result.rows():
            #     pprint(row)
            all = [
                cluster_item.bucket('travel-sample').scope('inventory').collection('airport').get(row.id).value
                for row in result.rows()]
            all = [item for item in all if item['city'] != 'Null']
            # pprint(all)
            # cities = {
            #     cluster_item.bucket('travel-sample').scope('inventory').collection('airport').get(row.id).value["city"]
            #     for row in result.rows()}

            # print(calendar.weekday(2023, 3, 9))
            # cities.discard('Null')
            # return {"cities": list(cities)}, HTTPStatus.OK
            return {"cities": sorted(all, key=lambda x : x['city'])}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Route(Resource):
    """Route resource class to return list of routes given a date, destination_id, and arrival_id """

    @timer_func
    def get(self, departure, destination, date):
        try:
            result = cluster_item.bucket('travel-sample').scope('inventory').query(f"""
                SELECT * 
                FROM `travel-sample`.inventory.route 
                WHERE route.sourceairport = '{departure}' 
                AND route.destinationairport = '{destination}'
                """)
            routes = [row for row in result.rows()]
            for route in routes:
                # route['route']['schedule'] = [p for p in route['route']['schedule'] if p['day'] == date]
                route['route']['schedule'] = [p for p in route['route']['schedule'] if p['day'] == date]
            # print(date)

            return {"routes": routes}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Airline(Resource):
    """Airline resource class to return airline info given a airline id"""

    @timer_func
    def get(self, id):
        try:
            airline = cluster_item.bucket('travel-sample').scope('inventory').query(f"""
            SELECT * 
            FROM `travel-sample`.inventory.airline 
            WHERE airline.iata = '{id}'
            """)
            airline = [row for row in airline.rows()]

            return {"airline": airline}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class Airport(Resource):
    """Airport resource class to return airport info given a airport id"""

    @timer_func
    def get(self, id):
        try:
            airport = cluster_item.bucket('travel-sample').scope('inventory').query(f"""
            SELECT * 
            FROM `travel-sample`.inventory.airport 
            WHERE airport.faa = '{id}'
            """)
            airport = [row for row in airport.rows()]

            return {"airport": airport}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


class History(Resource):
    """Histor resource class to return historical flight info given a tennent_agent scope and users email

    CREATE COVERING INDEX!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    """

    @timer_func
    def get(self, tenant, email):
        try:
            flights = cluster_item.bucket('travel-sample').scope(tenant).query(f"""
            SELECT bookings.past_flights 
            FROM `travel-sample`.{tenant}.bookings 
            WHERE bookings.userid = '{email}'
            """)
            flights = [row for row in flights.rows()]


            return {"flights": flights[0]}, HTTPStatus.OK
        except Exception as e:
            return {"error": e}, HTTPStatus.NOT_FOUND


