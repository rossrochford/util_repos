import pynng
import trio

from trio_util.pynng.serialization import msgpack_decode, msgpack_encode

# todo: convert to Req/Req sockets:
# https://github.com/codypiersall/pynng/blob/master/examples/reqprep.py


SEND_TIMEOUT = 6 * 1000


class PipelineReceiveDaemon(object):

    def __init__(self, address):
        self.address = address

    async def receive_loop(self):
        with pynng.Pull0(dial=self.address) as pull_sock:
            #pull_sock.listen(self.address)
            while True:
                msg = await pull_sock.arecv_msg()
                msg_bytes = msg.bytes
                if msg_bytes == b'exit':
                    print('exiting')
                    break
                await self._bytes_received(msg_bytes)

    async def _bytes_received(self, data):
        try:
            msg = msgpack_decode(data)
        except:
            print('error: failed to deserialize bytes')
            return
        await self.msg_received(msg)

    async def msg_received(self, msg):
        pass


class PipelineSendSocket(object):

    def __init__(self, address):
        self.address = address
        self.push_sock = None

    async def connect(self):
        if self.push_sock:
            return
        self.push_sock = pynng.Push0(send_timeout=SEND_TIMEOUT)
        self.push_sock.dial(self.address, block=True)

    async def send_msg(self, msg):
        try:
            data = msgpack_encode(msg)
        except:
            print(f'error: failed to serialize: {msg}')
            return
        await self._send_data(data)

    async def _send_data(self, msg):
        try:
            await self.push_sock.asend(msg)
        except pynng.exceptions.Timeout:
            print(f"PipelineSendSocket.send() to {self.address} timed-out")

    async def close(self):
        if self.push_sock:
            self.push_sock.close()


async def test():
    class PipelineReceiveDaemon2(PipelineReceiveDaemon):
        async def msg_received(self, msg):
            print(f"received: {msg}")

    import trio
    addr = "ipc:///tmp/test2.ipc"
    addr = 'tcp://127.0.0.1:31313'

    recv1 = PipelineReceiveDaemon2(addr)
    recv2 = PipelineReceiveDaemon2(addr)
    send_sock = PipelineSendSocket(addr)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(recv1.receive_loop)
        await trio.sleep(0.2)
        nursery.start_soon(recv2.receive_loop)

        await trio.sleep(1)
        await send_sock.connect()
        for i in range(20):
            await trio.sleep(1)
            await send_sock.send_msg({'index': i})


if __name__ == '__main__':
    trio.run(test)
