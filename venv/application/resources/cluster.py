
# internal modules
import collections
from datetime import timedelta
from pprint import pprint
import logging

# local modules
from auth.auth import config

# external modules
import ssl
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions, ClusterTracingOptions)
import couchbase.search as search

logging.basicConfig(level=logging.INFO)
logging.info("log out!!!")



def clusterAccess():
    print("connect")
    tracing_opts = ClusterTracingOptions(
        tracing_threshold_queue_size=1,
        tracing_threshold_kv=timedelta(milliseconds=1))
    auth = PasswordAuthenticator(config["USERNAME"], config["PASSWORD"]) # check this
    options = ClusterOptions(auth, tracing_options=tracing_opts)
    options.apply_profile('wan_development')
    print(options)
    # cluster = Cluster('couchbases://{}'.format(config["ENDPOINT"]), options)
    cluster = Cluster(config["ENDPOINT"], options)
    cluster.wait_until_ready(timedelta(seconds=5))
    bucket_names = []
    for bucket in cluster.buckets().get_all_buckets():
        pprint(bucket['name'])
        bucket_names.append(bucket["name"])
    print(bucket_names)

    return cluster

def access():
    endpoint = "couchbases://<endpoint of one of your machines>"  # Replace this with Connection String
    username = "admin"  # Replace this with  username from database access credentials
    password = "Password!"  # Replace this with password from database access credentials
    # User Input ends here.
    # Connect options - authentication
    auth = PasswordAuthenticator(username, password)
    # Get a reference to our cluster
    options = ClusterOptions(auth)
    # Use the pre-configured profile below to avoid latency issues with your connection.
    options.apply_profile("wan_development")

    try:
        cluster = Cluster(endpoint, options)
        # Wait until the cluster is ready for use.
        cluster.wait_until_ready(timedelta(seconds=5))
        print(cluster)
    except Exception as e:
        print(e)
    bucket_names = []
    # for bucket in cluster.buckets().get_all_buckets():
    #     pprint(bucket['name'])
    #     bucket_names.append(bucket["name"])
    # print(bucket_names)

def scopeAccess(bucket):
    scopes = bucket.collections().get_all_scopes()
    print(scopes)
    scope_names = []
    for _scope in scopes:
        # print(_scope.__dict__['_name'])
        scope_names.append(_scope.__dict__['_name'])
        # print(scope_names)
        try:
            for collect in _scope.__dict__['_collections']:
                # pprint(collect)
        #       pprint(bucket.scope(scope.__dict__['_name']).collection('airline').get("airline_10"))
        #         pprint(bucket.collection(scope.__dict__['_name']))
        #         pprint(collect.__dict__)
        #         collection_name = bucket.scope(_scope.__dict__['_name']).collection(collect.__dict__['_name']).name
        #         for row in bucket.scope(_scope.__dict__['_name']).query(f'select * from {collection_name} LIMIT 2').rows():
        #             pprint(row)
                pass
        #     # break
        except Exception as e:
            print(e)
    return bucket


def citySearch(cluster, index, query):
    try:
        # result = cluster.search_query(index, search.QueryStringQuery("adak"))
        result = cluster.search_query(index, search.WildcardQuery(f"*{query}*"))
        cities = []
        for row in result.rows():
            id = row.id
            res = cluster.bucket('travel-sample').scope('inventory').collection('airport').get(id)
            cities.append(res.value["city"])
            # pprint(res.value['city'])
        pprint({"cities": cities})
    except Exception as e:
        pprint(e)


cluster_item = clusterAccess()
# cluster_item = access()
# scope_item = scopeAccess(cluster_item.bucket('travel-sample'))
# print('INDEX: def_city')
# citySearch(cluster_item, 'search_city', 'isla')
# print('INDEX: def_inventory_airport_city')
# citySearch(cluster_item, 'def_inventory_airport_city')
