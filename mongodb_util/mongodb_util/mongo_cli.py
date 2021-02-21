
import trio

from mongodb_util.connection import connect_to_mongodb
from mongodb_util.queries.create import create_many
from mongodb_util.queries.delete import delete_many__by_field
from mongodb_util.queries.find import find_all, find_many, find_many__by_field, find_one, find_many__or
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
        return await trio.to_thread.run_sync(
            create_many, self.db, collection_name, document_dicts
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
        return await trio.to_thread.run_sync(
            update_many__multi_field, self.db, collection_name, data
        )

    async def find_one(self, collection_name, field, value):
        return await trio.to_thread.run_sync(
            find_one, self.db, collection_name, field, value
        )

    async def find_many(self, collection_name, ids, result_fields=None):
        return await trio.to_thread.run_sync(
            find_many, self.db, collection_name, ids, result_fields
        )

    async def find_many__by_field(self, collection_name, field, values, result_fields=None):
        return await trio.to_thread.run_sync(
            find_many__by_field, self.db, collection_name,
            field, values, result_fields
        )

    async def find_many__or(self, collection_name, dicts):
        return await trio.to_thread.run_sync(
            find_many__or, self.db, collection_name, dicts
        )

    async def find_all(self, collection_name, result_fields=None, batch_size=0):
        return await trio.to_thread.run_sync(
            find_all, self.db, collection_name, result_fields, batch_size
        )

    async def delete_many__by_field(self, collection_name, field, values):
        return await trio.to_thread.run_sync(
            delete_many__by_field, self.db, collection_name, field, values
        )
