import json
import uuid

import trio

# existing libraries for Redis and Trio:
#     https://github.com/Tronic/redio
#     trio-asyncio + aioredis:  https://github.com/python-trio/trio-asyncio   https://github.com/aio-libs/aioredis
#     trio-redis: https://github.com/alekseyev/trio-redis


async def redis__get_pubsub_recv_iterator(redis_cli, topic):
    if not topic.startswith('__keyspace'):
        topic = '__keyspace@*:' + topic
    ps = redis_cli.pubsub()
    ps.subscribe(topic)
    return ps.listen().__iter__()


async def redis_publish(redis_cli, topic, value):
    if not topic.startswith('__keyspace'):
        topic = '__keyspace@*:' + topic
    return await trio.to_thread.run_sync(
        redis_cli.publish, topic, value
    )
    # note trio-redis impl is:  await redis_client.conn.process_command(b'publish', channel, value)


async def redis_subscribe(redis_cli, topic, trio_channel):

    iter = await redis__get_pubsub_recv_iterator(redis_cli, topic)

    while True:
        raw_message = await trio.to_thread.run_sync(
            iter.__next__
        )
        if raw_message["type"] != "message":
            continue
        data = raw_message['data'].decode()
        di = json.loads(data)
        await trio_channel.send(di)


# see: https://redis.io/commands/rpoplpush#pattern-reliable-queue
# note: it's more reliable the "processing list" to be persisted (e.g. keyed by worker_id-queue_name) and
# the item to only be removed AFTER it has been processed, that way we messages aren't lost if a worker fails
def _redis__queue_pop(redis_cli, queue_name, temp_list=None):

    if temp_list is None:  # the 'processing list'
        temp_list = 'wrk-' + queue_name + uuid.uuid4().hex[:6]

    redis_cli.brpoplpush(queue_name, temp_list)
    _, body = redis_cli.blpop([temp_list])

    return body


async def redis__queue_pop(redis_cli, queue_name, temp_list=None):
    return await trio.to_thread.run_sync(
        _redis__queue_pop, redis_cli, queue_name, temp_list
    )


async def redis__queue_push(redis_cli, queue_name, msg_string):
    # for pure async version see webextractor.util_new.trio_util
    return await trio.to_thread.run_sync(
        redis_cli.lpush, queue_name, msg_string
    )
