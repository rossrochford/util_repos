import trio

from redis_util.rpc.rpc_server_trio import AsyncRedisRpcServerDaemon


class RpcApp(object):

    def __init__(self, namespace):
        self.namespace = namespace
        self.functions = []

    def expose(self, func):
        self.functions.append(func)


rpc_app = RpcApp('rpc_test')


@rpc_app.expose
async def print_msg(string):
    await trio.sleep(1)
    print(string)
    return string


if __name__ == '__main__':
    daemon = AsyncRedisRpcServerDaemon([rpc_app], 1)
    trio.run(daemon.run_daemon)
