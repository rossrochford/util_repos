import json
import os
import uuid

import redis
from redis_util.redis_trio import redis__get_pubsub_recv_iterator
import trio

REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


# to publish an event use:
# from redis_util.redis_trio import redis_publish


class RedisEventBus(object):

    def __init__(self, redis_cli, redis_topic):
        self.event_buffer = {}
        if redis_cli is None:
            redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)
        self.redis_cli = redis_cli
        self.redis_topic = redis_topic
        self.exit_daemon_flag = trio.Event()

    def wait_for_existing_event(self, uid):
        if uid not in self.event_buffer:
            self.event_buffer[uid] = [trio.Event(), None]
        await self.event_buffer[uid][0].wait()

    def create_pending_event(self):
        uid = 'pending-' + uuid.uuid4().hex[:16]
        event = trio.Event()
        #                        [trio_event, event_payload]
        self.event_buffer[uid] = [event, None]
        return uid, event

    async def event_received(self, event_uid, event_type, payload):
        pass

    async def _msg_received(self, msg_dict):
        if not msg_dict.get('event_uid'):
            print(f"error: msg_dict missing event_uid: {msg_dict}")
            return

        event_uid = msg_dict['event_uid']  # todo: use 'event_type' instead of 'pending-' prefix?
        if event_uid.startswith('pending-') and event_uid not in self.event_buffer:
            return  # no local interest in this event

        if event_uid in self.event_buffer:
            self.event_buffer[event_uid][1] = msg_dict.get('event_payload')
            self.event_buffer[event_uid][0].set()

        await self.event_received(
            event_uid, msg_dict['event_type'], msg_dict.get('event_payload')
        )

        if len(self.event_buffer) > 1000:
            # flush old events. Note: keys are ordered by insertion time in python 3.7
            keys_to_remove = [k for k in self.event_buffer.keys()][:300]
            for k in keys_to_remove:
                del self.event_buffer[k]

    async def receive_worker_loop(self):
        pubsub_iter = await redis__get_pubsub_recv_iterator(
            self.redis_cli, self.redis_topic
        )
        while True:
            if self.exit_daemon_flag.is_set():
                break
            raw_message = await trio.to_thread.run_sync(pubsub_iter.__next__)
            if raw_message["type"] != "message":
                continue
            try:
                data = raw_message['data'].decode()
                msg_dict = json.loads(data)
            except:
                print(f'error: failed to parse event msg: {raw_message}')
                continue
            await self._msg_received(msg_dict)
