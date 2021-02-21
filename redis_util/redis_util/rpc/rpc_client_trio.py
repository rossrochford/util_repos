import os
import uuid

import redis
import trio

from trio_util.pynng.serialization import msgpack_decode, msgpack_encode


REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


class AsyncRpcClient(object):

    def __init__(self):
        self.redis_cli = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT)

    async def _send_call(self, func_name, payload_bytes):
        call_queue_name = f"{func_name}:calls"
        call_uid = uuid.uuid4().hex[:8]
        msg_bytes = bytes(call_uid, 'utf-8') + b':' + payload_bytes
        await trio.to_thread.run_sync(
            self.redis_cli.rpush, call_queue_name, msg_bytes
        )
        return call_uid

    async def _wait_for_result(self, func_name, call_uid, timeout):
        result_list_key = f"{func_name}:result:{call_uid}"
        result_bytes = None
        if timeout:
            with trio.move_on_after(timeout) as cancel_scope:
                result_bytes = await trio.to_thread.run_sync(
                    self.redis_cli.blpop, result_list_key
                )
            if cancel_scope.cancelled_caught:
                print('timeout!')
                return False, None
        else:
            # wait without timeout
            result_bytes = await trio.to_thread.run_sync(
                self.redis_cli.blpop, result_list_key
            )
        succ, result = msgpack_decode(result_bytes)
        import pdb; pdb.set_trace()
        return succ, result

    async def call(self, func_name, args, kwargs, timeout=30):

        if '.' not in func_name:
            print(f'error: func_name missing namespace: {func_name}')
            return False, None

        succ, payload_bytes = msgpack_encode({
            'args': args, 'kwargs': kwargs
        })
        if succ is False:
            print(f'error: failed to serialize args: {func_name} {args} {kwargs}')
            return False, None

        call_uid = await self._send_call(func_name, payload_bytes)

        return await self._wait_for_result(func_name, call_uid, timeout)


async def test():
    rpc_cli = AsyncRpcClient()
    for i in range(10):
        await trio.sleep(1)
        msg = f"wooo{i}"
        await rpc_cli.call('rpc_test.print_msg', [msg], None, 5)


if __name__ == '__main__':
    trio.run(test)
