from bson.objectid import ObjectId


def delete_many__by_field(db, collection_name, field, values):

    if field == '_id':
        for i, val in enumerate(values):
            if type(val) is str:
                values[i] = ObjectId(val)

    db[collection_name].delete_many({field: {"$in": values}})
