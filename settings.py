import os
import platform


operation = platform.uname()
DB_INFO = dict()
DB_INFO['port'] = ""
if operation[0] == "Linux":
    # It is the Maching listing_id Docker address in K8S, it will be changed in Prod. $minikube service da-services
    DA_SERVICES_URL = os.environ.get('DA_SERVICES_URL')
    # It is for connecting MongoDB
    if os.environ.get('MONGO_SERVER_PORT'):
        DB_INFO['port'] = int(os.environ.get('MONGO_SERVER_PORT'))
    if DB_INFO['port']:
       DB_INFO['host'] = os.environ.get('MONGO_SERVER_LOCATION')
    else:
       DB_INFO['host'] = "mongodb+srv://" + \
                         os.environ.get("MONGO_USERNAME") + ":" + \
                         os.environ.get("MONGO_PASSWORD") + \
                         "@" + \
                         os.environ.get('MONGO_SERVER_LOCATION')
    print(f"The host address is {DB_INFO['host']}")
    DB_INFO['database'] = os.environ.get('MONGO_DATABASE')
    DB_INFO['collection'] = os.environ.get('MONGO_COLLECTION')

if operation[0] == "Windows":
    # It is only for my WIN10 to test
    DA_SERVICES_URL = "http://192.168.1.100/listings/matching/"
    DB_INFO['port'] = 27017
    DB_INFO['host'] = "192.168.1.101"
    DB_INFO['database'] = "waybase"
