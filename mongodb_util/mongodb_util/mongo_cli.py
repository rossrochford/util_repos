
from eliot import log_call, start_action
import trio

from mongodb_util.connection import connect_to_mongodb
from mongodb_util.queries.create import create_many, get_or_create_from_key, get_or_create_rels
from mongodb_util.queries.delete import delete_many__by_field
from mongodb_util.queries.find import find_all, find_many__by_id, find_many__by_field, find_one, find_many__or
from mongodb_util.queries.update import update_many__single_field, update_many__multi_field

# mongodb://mongodb_user:password123@127.0.0.1:27017/twitter_db
# mongodb://root:password123@127.0.0.1:27017/


class MongoDBClient(object):

    def __init__(self):
        self.db = None

    async def init(self, db_name):
        self.db = await trio.to_thread.run_sync(connect_to_mongodb, db_name)

    async def create_many(self, collection_name, document_dicts):
        """
        Args:
            collection_name (str):   name of mongo collection
            document_dicts (list):   document contents
        """
        if not document_dicts:
            return
        action_args = dict(
            action_type='create_many', collection_name=collection_name,
            num_docs=len(document_dicts)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                create_many, self.db, collection_name, document_dicts
            )

    async def get_or_create_from_key(
        self, collection_name, key, field_values,
        projection_fields=None, defaults=None
    ):
        action_args = dict(
            action_type='get_or_create_from_key',
            collection_name=collection_name,
            num_values=len(field_values)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                get_or_create_from_key, self.db, collection_name,
                key, field_values, projection_fields, defaults
            )

    async def get_or_create_rels(self, collection_name, rel_docs):
        action_args = dict(
            action_type='get_or_create_rels', collection_name=collection_name,
            num_rels=len(rel_docs)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                get_or_create_rels, self.db, collection_name, rel_docs
            )

    async def update_many__single_field(self, collection_name, results_by_id, field_name):
        """
        Args:
            collection_name (str):   name of mongo collection
            results_by_id (dict):    map of id -> field_value
            field_name (str):        name of field to save to document
        """
        if not results_by_id:
            return
        return await trio.to_thread.run_sync(
            update_many__single_field, self.db, collection_name,
            results_by_id, field_name
        )

    async def update_many__multi_field(self, collection_name, data):
        if not data:
            return
        action_args = dict(
            action_type='update_many__multi_field', collection_name=collection_name,
            num_updates=len(data)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                update_many__multi_field, self.db, collection_name, data
            )

    async def find_one(self, collection_name, field, value):
        return await trio.to_thread.run_sync(
            find_one, self.db, collection_name, field, value
        )

    async def find_many__by_id(self, collection_name, ids, result_fields=None):
        action_args = dict(
            action_type='find_many__by_id', collection_name=collection_name,
            num_ids=len(ids)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                find_many__by_id, self.db, collection_name, ids, result_fields
            )

    async def find_many__by_field(self, collection_name, field, values, result_fields=None):
        action_args = dict(
            action_type='find_many__by_field', collection_name=collection_name,
            field=field, num_queries=len(values)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                find_many__by_field, self.db, collection_name,
                field, values, result_fields
            )

    async def find_many__or(self, collection_name, dicts):
        action_args = dict(
            action_type='find_many__or', collection_name=collection_name,
            num_queries=len(dicts)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                find_many__or, self.db, collection_name, dicts
            )

    async def find_all(self, collection_name, result_fields=None, batch_size=0):
        return await trio.to_thread.run_sync(
            find_all, self.db, collection_name, result_fields, batch_size
        )

    async def delete_many__by_field(self, collection_name, field, values):
        action_args = dict(
            action_type='delete_many__by_field', collection_name=collection_name,
            field=field, num_queries=len(values)
        )
        with start_action(**action_args):
            return await trio.to_thread.run_sync(
                delete_many__by_field, self.db, collection_name, field, values
            )
