import json

from bson.objectid import ObjectId
from pymongo import UpdateOne


def update_many__single_field(db, collection_name, data, field_name):
    updates = []
    for id, result in data.items():
        if type(result) in (dict, list):
            result = json.dumps(result)
        up = UpdateOne(
            {'_id': ObjectId(id)}, {'$set': {field_name: result}}
        )
        updates.append(up)

    return db[collection_name].bulk_write(updates)


def update_many__multi_field(db, collection_name, data):
    updates = []
    for id, result_dict in data.items():
        result_dict = result_dict.copy()
        if '_id' in result_dict:
            del result_dict['_id']  # just in case, avoid updating ids
        for field_name, value in result_dict.items():
            if type(value) in (dict, list):
                result_dict[field_name] = json.dumps(value)
        up = UpdateOne(
            {'_id': ObjectId(id)}, {'$set': result_dict}
        )
        updates.append(up)

    return db[collection_name].bulk_write(updates)


'''
def append_to_json__many(db, collection_name, updates_by_id, field_name):

    docs_by_id = _find_many(
        db, collection_name, updates_by_id.keys(),
        result_fields=[field_name]
    )

    updates = {}
    for id, doc in docs_by_id.items():
        if not doc.get(field_name):
            updates[id] = json.dumps(updates_by_id[id][field_name])
            continue

        try:
            curr_value = json.loads(doc[field_name])
        except Exception:
            print(f'error: failed to parse json value at: {id}.{field_name}')
            continue
        if type(curr_value) is dict:
            curr_value.update(updates_by_id[id])
        elif type(curr_value) is list:
            curr_value.extend(updates_by_id[id])
        else:
            print(f'error: json value at: {id}.{field_name} should be list or dict, found: {curr_value}')
            continue
        updates[id] = json.dumps(curr_value)

    _update_many__single_field(db, collection_name, updates, field_name)


def _append_string_many(db, collection_name, results_by_id, field_name):
    # based on: https://stackoverflow.com/questions/23868963/append-a-string-to-the-end-of-an-existing-field-in-mongodb
    # { $set: { a: { $concat: [ "$a", "World" ] } } }

    updates = []
    for id, string_value in results_by_id.items():
        up = UpdateOne(
            {'_id': ObjectId(id)},
            {'$concat': {field_name: [f"${field_name}", string_value]}}
            #{'$set': {field_name: {'$concat': [f"${field_name}", string_value]}}}
        )
        updates.append(up)

    return db[collection_name].bulk_write(updates)
'''
