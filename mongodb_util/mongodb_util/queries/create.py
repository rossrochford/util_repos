import json
import uuid

from bson.objectid import ObjectId
from pymongo import InsertOne

from mongodb_util.queries.delete import delete_many__by_field
from mongodb_util.queries.find import find_many__or
from mongodb_util.queries.update import update_many__multi_field


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


def get_or_create_from_key(
    db, collection_name, key, field_values,
    projection_fields=None, defaults=None
):
    field_values = [s for s in field_values if s is not None]  # sigh... happens sometimes

    if not field_values:
        return {}, {}, []

    projection_fields = projection_fields or []
    if '_id' not in projection_fields:
        projection_fields.append('_id')
    if key not in projection_fields:
        projection_fields.append(key)

    existing_docs = db[collection_name].find(
        {key: {"$in": field_values}}, projection=projection_fields
    )
    existing_docs = [doc for doc in existing_docs]  # avoids cursor silliness
    existing_values = [doc[key] for doc in existing_docs]
    new_values = set(field_values) - set(existing_values)

    docs_by_key, docs_by_id = {}, {}
    for doc in existing_docs:
        docs_by_id[doc['_id'].__str__()] = doc
        docs_by_key[doc[key]] = doc

    if not new_values:
        return docs_by_id, docs_by_key, []

    # create new_docs
    if defaults:
        new_docs = [{**{key: val}, **defaults} for val in new_values]
    else:
        new_docs = [{key: val} for val in new_values]
    new_docs_by_id = create_many(db, collection_name, new_docs)

    # consolidate results
    new_doc_ids = []
    for id, doc in new_docs_by_id.items():
        docs_by_id[id] = doc
        docs_by_key[doc[key]] = doc
        new_doc_ids.append(id)

    return docs_by_id, docs_by_key, new_doc_ids


def get_or_create_rels(db, collection_name, rel_docs):

    if not rel_docs:
        return {}

    id_keys = [
        k for k in rel_docs[0].keys() if k != '_id' and k.endswith('_id')
    ]
    assert len(id_keys) == 2
    id_keys.sort()
    id_key1, id_key2 = id_keys

    rel_docs = rel_docs.copy()
    for doc in rel_docs:
        # remove _id, it only confuses matters (can't tell if it's new or existing)
        if '_id' in doc:
            del doc['_id']

    rel_docs__ids_only = []
    for doc in rel_docs:
        if type(doc[id_key1]) is str:
            doc[id_key1] = ObjectId(doc[id_key1])
        if type(doc[id_key2]) is str:
            doc[id_key2] = ObjectId(doc[id_key2])
        rel_docs__ids_only.append({
            id_key1: doc[id_key1], id_key2: doc[id_key2]
        })

    existing_rels = find_many__or(db, collection_name, rel_docs__ids_only)

    existing_id_pairs, existing_docs_by_key, duplicates = [], {}, []
    for id, doc in existing_rels.items():
        key = (doc[id_key1], doc[id_key2])
        if key in existing_id_pairs:
            duplicates.append(id)
            continue
        existing_id_pairs.append(key)
        existing_docs_by_key[key] = doc

    if duplicates:
        delete_many__by_field(db, collection_name, '_id', duplicates)

    updates, creations = {}, []
    for doc in rel_docs:
        key = (doc[id_key1], doc[id_key2])
        if key in existing_id_pairs:
            existing_doc = existing_docs_by_key[key]
            additional_values = {
                k: v for (k, v) in doc.items()
                if k not in ('_id', id_key1, id_key2) and existing_doc.get(k) != v
            }
            if not additional_values:
                continue
            updates[existing_doc['_id'].__str__()] = additional_values
            existing_doc.update(updates)  # so new values are also returned
        else:
            creations.append(doc)

    if updates:
        update_many__multi_field(db, collection_name, updates)

    rel_docs_new = {}
    if creations:
        rel_docs_new = create_many(db, collection_name, creations)

    '''
    rel_docs_new = [
        d for d in rel_docs if (d[id_key1], d[id_key2]) not in existing_id_pairs
    ]
    if not rel_docs_new:
        return existing_rels

    rel_docs_new = create_many(db, collection_name, rel_docs_new)'''
    return {**existing_rels, **rel_docs_new}
