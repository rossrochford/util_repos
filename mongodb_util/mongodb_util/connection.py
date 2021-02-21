import os

import pymongo


MONGODB_HOSTNAME = os.environ.get('MONGODB_HOSTNAME', '127.0.0.1')
MONGODB_PORT = os.environ.get('MONGODB_PORT', 27017)
MONGODB_USERNAME = os.environ.get('MONGODB_USERNAME', 'mongodb_user')
MONGODB_PASSWORD = os.environ.get('MONGODB_PASSWORD', 'password123')


def connect_to_mongodb(db_name):
    hostname, port = MONGODB_HOSTNAME, MONGODB_PORT
    username, password = MONGODB_USERNAME, MONGODB_PASSWORD
    connection_str = f"mongodb://{username}:{password}@{hostname}:{port}/{db_name}"
    client = pymongo.MongoClient(connection_str)
    return client[db_name]
