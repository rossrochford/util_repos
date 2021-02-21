import datetime
import json
import os
import uuid

import pytz
import redis
from redis_util.redis_trio import redis__get_pubsub_recv_iterator
import trio

REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


def _get_utc_now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)


class RedisEventBus(object):

    def __init__(self, redis_cli, redis_topic):
        self.pending_events = {}
        self.event_payloads = {}
        if redis_cli is None:
            redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)
        self.redis_cli = redis_cli
        self.redis_topic = redis_topic
        self.prev_flush_dt = None
        self.exit_daemon_flag = trio.Event()

    def wait_for_existing_event(self, uid):
        if uid in self.pending_events:
            return self.pending_events[1]
        event = trio.Event()
        self.pending_events[uid] = (_get_utc_now(), event)
        return event

    def create_pending_event(self):
        uid = 'pending-' + uuid.uuid4().hex[:16]
        event = trio.Event()
        self.pending_events[uid] = (_get_utc_now(), event)
        return uid, event

    async def event_received(self, event_uid, event_type, payload):
        pass

    async def _msg_received(self, msg_dict):
        if not msg_dict.get('event_uid'):
            print(f"error: msg_dict missing event_uid: {msg_dict}")
            return

        event_uid = msg_dict['event_uid']  # todo: use 'event_type' instead of 'pending-' prefix?
        if event_uid.startswith('pending-') and event_uid not in self.pending_events:
            return  # no local interest in this event

        now = _get_utc_now()

        if msg_dict.get('event_payload'):
            self.event_payloads[event_uid] = (now,  msg_dict['event_payload'])
        if event_uid in self.pending_events:
            self.pending_events[event_uid][1].set()

        await self.event_received(
            event_uid, msg_dict['event_type'], msg_dict.get('event_payload')
        )

        if len(self.pending_events) > 100 or len(self.event_payloads) > 100:
            if self.prev_flush_dt is None or (now-self.prev_flush_dt).hours > 2:
                self._flush_old_events()

    def _flush_old_events(self):
        print('flushing events')
        now = _get_utc_now()
        twelve_hours_ago = now - datetime.timedelta(hours=12)

        to_remove = set()
        for uid, (dt, trio_event) in self.pending_events.items():
            if dt < twelve_hours_ago:
                to_remove.add(uid)
        for uid, (dt, payload) in self.event_payloads.items():
            if dt < twelve_hours_ago:
                to_remove.add(uid)
        for uid in to_remove:
            if uid in self.pending_events:
                del self.pending_events[uid]
            if uid in self.event_payloads:
                del self.event_payloads[uid]
        self.prev_flush_dt = now

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
