import json
import os
import signal
import uuid

import redis
import trio

from redis_util.redis_trio import redis__queue_pop, redis_publish


QUEUE_NAME = 'twint_twitter_items'
DEFAULT_PRIORITY = 2

REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


async def daemon__receive_items_from_redis_queue(worker_groups, queue_name):

    redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    groups_by_work_type = {}
    for group in worker_groups:
        for work_type in group.work_types:
            groups_by_work_type[work_type] = group

    while True:

        body = await redis__queue_pop(redis_cli, queue_name)

        if body == b'exit':  # also maybe an 'exit-soon' message?
            break

        try:
            msg = json.loads(body.decode())
        except:
            print(f'error parsing msg: {body}')
            continue

        work_type = msg.get('work_type')
        if work_type is None or work_type not in groups_by_work_type:
            print(f"invalid work_type: {work_type}")
            continue

        q = groups_by_work_type[work_type].priority_queue
        priority = int(msg.get('priority', DEFAULT_PRIORITY))
        items = msg.get('items', [])

        print(f"received batch of {len(items)} {work_type} items")

        for item_dict in items:
            q.put_nowait(priority, item_dict)


async def _flush_group(worker_group):
    # print(f"{worker_group.work_types} received flush-group")
    for worker in worker_group.workers:
        # print(f"sending 'flush' to worker: {worker.uid}:{worker.funcname}")
        await worker.send_channel.send('flush')


async def _exit_all(worker_group):
    print(f"sending 'exit' to: {worker_group.work_types} channels")
    for worker in worker_group.workers:
        await worker.send_channel.send('exit')


async def notify_queue_exhausted(redis_cli, work_type, redis_event_topic):
    event_msg = {
        'event_uid': uuid.uuid4().hex[:12],
        'event_payload': {
            'work_type': work_type,
        },
        'event_type': 'queue-exhausted'
    }
    await redis_publish(
        redis_cli, redis_event_topic, json.dumps(event_msg)
    )


async def daemon__route_items_to_worker_channels(worker_group):

    redis_cli = None
    if worker_group.notify_exhausted:
        redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    empty_count = 0
    q = worker_group.priority_queue

    while True:
        try:
            item = q.get_nowait()
        except trio.WouldBlock:
            empty_count += 1
            if empty_count == 10 and worker_group.notify_exhausted:
                for work_type in worker_group.work_types:
                    # print(f"notify_queue_exhausted: {work_type}")
                    await notify_queue_exhausted(
                        redis_cli, work_type, 'twint_events'
                    )
            await trio.sleep(0.8)

            if empty_count == 50:
                await _flush_group(worker_group)
            continue

        empty_count = 0

        if item == 'exit-all':
            await _exit_all(worker_group)
        elif item == 'flush-group':
            await _flush_group(worker_group)
        else:
            worker = worker_group.choose_worker(item)
            await worker.send_channel.send(item)


async def start_workers_and_daemons(
    worker_groups, redis_queue_name, global_ctx, nursery
):
    await _cancel_on_controlC(nursery, redis_queue_name)

    # start workers
    for group in worker_groups:
        for worker in group.workers:
            nursery.start_soon(worker.worker_loop, global_ctx)

    for group in worker_groups:
        nursery.start_soon(
            daemon__route_items_to_worker_channels, group
        )

    nursery.start_soon(
        daemon__receive_items_from_redis_queue, worker_groups, redis_queue_name
    )


async def _cancel_on_controlC(nursery, redis_queue_name):
    def signal_handler(sig, frame):
        print('You pressed control-C!')
        nursery.cancel_scope.cancel()
        # redis__queue_pop gets stuck, so we need to send an 'exit' message
        redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)
        redis_cli.lpush(redis_queue_name, 'exit')

    signal.signal(signal.SIGINT, signal_handler)
