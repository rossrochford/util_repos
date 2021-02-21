import json
import uuid

from bson.objectid import ObjectId
from pymongo import InsertOne


def new_obj_id():
    return ObjectId(uuid.uuid4().hex[:24])


def create_many(db, collection_name, document_dicts):

    insertions = []
    docs_by_id = {}
    for doc in document_dicts:
        for key, val in doc.items():
            if type(val) in (dict, list):
                doc[key] = json.dumps(val)
        if '_id' in doc:
            if type(doc['_id']) is str:
                doc['_id'] = ObjectId(doc['_id'])
        else:
            doc['_id'] = new_obj_id()
        docs_by_id[doc['_id'].__str__()] = doc
        insertions.append(InsertOne(doc))

    db[collection_name].bulk_write(insertions)
    return docs_by_id  # return {id -> dict} to be consistent with find_many()
