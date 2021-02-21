from bson.objectid import ObjectId


def find_one(db, collection_name, field, value):
    if field == '_id' and type(value) is str:
        value = ObjectId(value)
    return db[collection_name].find_one({field: value})


def find_all(db, collection_name, result_fields=None, batch_size=0):

    if result_fields and '_id' not in result_fields:
        result_fields.append('_id')

    docs = db[collection_name].find(
        projection=result_fields, batch_size=batch_size
    )
    return {d['_id'].__str__(): d for d in docs}


def find_many(db, collection_name, ids, result_fields=None, batch_size=0):

    if result_fields and '_id' not in result_fields:
        result_fields.append('_id')

    docs = db[collection_name].find(
        {"_id": {"$in": [ObjectId(i) for i in ids]}},
        projection=result_fields, batch_size=batch_size
    )
    return {d['_id'].__str__(): d for d in docs}


def find_many__by_field(db, collection_name, field, values, result_fields=None):

    if result_fields and '_id' not in result_fields:
        result_fields.append('_id')

    docs = db[collection_name].find(
        {field: {"$in": values}}, projection=result_fields
    )
    return {d['_id'].__str__(): d for d in docs}


def find_many__or(db, collection_name, dicts):
    '''
    db.baz.find({
        $or: [
            { foo: 'a', bar: 'b' },
            { foo: 'b', bar: 'c' },
            { foo: 'a', bar: 'd' },
            { foo: 'b', bar: 'b' },
        ]
    })
    '''
    docs = db[collection_name].find({"$or": dicts})
    return {d['_id'].__str__(): d for d in docs}
