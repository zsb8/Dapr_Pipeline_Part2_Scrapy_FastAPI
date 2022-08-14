import os

# It is the Maching listing_id Docker address in K8S, it will be changed in Prod. $minikube service da-services
DA_SERVICES_URL = os.environ.get('DA_SERVICES_URL')

# It is for connecting MongoDB
DB_INFO = dict()
DB_INFO['port'] = os.environ.get('MONGO_SERVER_PORT')

if DB_INFO['port']:
   DB_INFO['host'] = os.environ.get('MONGO_SERVER_LOCATION')
else:
   DB_INFO['host'] = "mongodb+srv://" + \
                     os.environ.get("MONGO_USERNAME") + ":" + \
                     os.environ.get("MONGO_PASSWORD") + \
                     "@" + \
                     os.environ.get('MONGO_SERVER_LOCATION')
print(f"The host addree is {DB_INFO['host']}")
DB_INFO['database'] = os.environ.get('MONGO_DATABASE')
DB_INFO['collection'] = os.environ.get('MONGO_COLLECTION')