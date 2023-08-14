# internal modules
from datetime import timedelta
from pprint import pprint

# local modules

# external modules
from flask import Flask
from flask_restful import Api
from flask_cors import CORS

# resources
from resources.inventory import Root, Buckets # Scopes, Collections, Documents, City, Route, Airline, Airport, History


# server setup
print("starting")
app = Flask(__name__)
api = Api(app)
CORS(app)

# APIs
api.add_resource(Root, '/')
# api.add_resource(Mimic, '/mimic/<string:word>')
# api.add_resource(Buckets, '/buckets')
# api.add_resource(Scopes, '/scopes/<string:bucket>')
# api.add_resource(Collections, '/collections/<string:bucket>/<string:scope>')
# api.add_resource(Documents, '/documents/<string:bucket>/<string:scope>/<string:collection>')
# api.add_resource(City, '/city/<string:query>')
# api.add_resource(Route, '/route/<string:departure>/<string:destination>/<int:date>')
# api.add_resource(Airline, '/airline/<string:id>')
# api.add_resource(Airport, '/airport/<string:id>')
# api.add_resource(History, '/history/<string:tenant>/<string:email>')


# main



# server start
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
