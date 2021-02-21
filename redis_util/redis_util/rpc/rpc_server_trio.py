import os

import redis
import trio

from trio_util.pynng.serialization import msgpack_decode, msgpack_encode


REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


async def _reply(redis_cli, result_list_key, msg_bytes):
    print(f"reply: {msg_bytes}")
    await trio.to_thread.run_sync(
        redis_cli.rpush, result_list_key, msg_bytes
    )


async def _handle_call(redis_cli, func_name, function, call_uid, payload_bytes):

    result_list_key = f"{func_name}:result:{call_uid}"

    succ, call_args = msgpack_decode(payload_bytes)
    if succ is False:
        await _reply(redis_cli, result_list_key, b'error_deserializing_call')
        return

    if type(call_args) is not dict:
        await _reply(redis_cli, result_list_key, b'error_unexpected_call_data')
        return

    args = call_args.get('args') or []
    kwargs = call_args.get('kwargs') or {}

    try:
        result = await function(*args, **kwargs)
    except:
        await _reply(redis_cli, result_list_key, b'exception_thrown')
        return

    succ, result_bytes = msgpack_encode(result)
    if succ is False:
        await _reply(redis_cli, result_list_key, b'error_serializing_result')
        return

    await _reply(redis_cli, result_list_key, result_bytes)


async def _rpc_worker(functions_by_name, receive_channel):

    redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    async with receive_channel:
        async for (func_name, call_uid, payload_bytes) in receive_channel:
            function = functions_by_name[func_name]
            try:
                await _handle_call(
                    redis_cli, func_name, function, call_uid, payload_bytes
                )
            except Exception as e:
                print('error: uncaught exception thrown by _handle_call()')
                import pdb; pdb.set_trace()
                continue


async def _poll_queues_and_send_calls_to_workers(redis_queues, worker_send_channel):
    redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    not_found_count = 0
    while True:
        item_found = False
        for redis_queue_name, func_name in redis_queues:
            print(f"popping from {redis_queue_name}")
            msg_bytes = await trio.to_thread.run_sync(
                redis_cli.lpop, redis_queue_name
            )
            if msg_bytes is None:
                continue
            item_found = True

            if b':' not in msg_bytes:
                print("error: msg_bytes missing ':' ")
                continue

            call_uid, payload_bytes = msg_bytes.split(b':', 1)
            await worker_send_channel.send((func_name, call_uid, payload_bytes))

        if item_found:
            not_found_count = 0
        else:
            not_found_count += 1
            if not_found_count <= 3:
                await trio.sleep(0.2)
            elif not_found_count <= 10:
                await trio.sleep(0.8)
            else:
                await trio.sleep(2)


class AsyncRedisRpcServerDaemon(object):

    def __init__(self, rpc_apps, concurrency):
        assert concurrency > 0
        self.concurrency = concurrency
        self.functions_by_name = {}
        self.redis_queues = []
        self.worker_send_channel = None

        for rpc_app in rpc_apps:
            for function in rpc_app.functions:
                func_name = f"{rpc_app.namespace}.{function.__name__}"
                if func_name in self.functions_by_name:
                    print(f'error: duplicate function found: {func_name}')
                    continue
                self.functions_by_name[func_name] = function

                # queue_name, func_name
                self.redis_queues.append(
                    (f"{func_name}:calls", func_name)
                )

    async def run_daemon(self):
        async with trio.open_nursery() as nursery:
            send_channel, receive_channel = trio.open_memory_channel(20)
            self.worker_send_channel = send_channel
            for i in range(self.concurrency):
                recv_channel = receive_channel if i == 0 else receive_channel.clone()
                nursery.start_soon(
                    _rpc_worker, self.functions_by_name, recv_channel
                )

            await _poll_queues_and_send_calls_to_workers(
                self.redis_queues, self.worker_send_channel
            )
