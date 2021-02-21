import pynng
import trio

from trio_util.pynng.serialization import msgpack_decode, msgpack_encode

SEND_TIMEOUT = 6 * 1000


class AsyncReqRepReceiveDaemon(object):

    def __init__(self, address):
        self.address = address
        self.pending_resp = False
        self.rep_sock = None

    async def receive_loop(self):
        with pynng.Rep0(listen=self.address) as sock:
            self.rep_sock = sock
            while True:
                msg = await self.rep_sock.arecv_msg()
                msg_bytes = msg.bytes
                if msg_bytes == b'exit':
                    await self.rep_sock.asend(b'ACK')
                    print('exiting')
                    break
                await self._handle_bytes_received(msg_bytes)

    async def _handle_bytes_received(self, msg_bytes):
        succ, msg = msgpack_decode(msg_bytes)

        if succ is False:
            await self._reply(b'FAILED')
            return

        self.pending_resp = True  # ensure a response is always returned
        try:
            await self.req_received(msg)  # typically sends the reply
        except:
            await self._reply(b'FAILED')

        if self.pending_resp:
            # in case a reply wasn't sent by req_received()
            await self._reply(b'ACK')

    async def _reply(self, resp):
        if type(resp) is not bytes:
            succ, resp = msgpack_decode(resp)
            if succ is False:
                await self.rep_sock.asend(b'FAILED')
                self.pending_resp = False
                return

        self.pending_resp = False
        await self.rep_sock.asend(b'ACK')

    async def req_received(self, msg):
        pass


class AsyncRpcDaemon(AsyncReqRepReceiveDaemon):

    def __init__(self, address):
        super(AsyncReqRepReceiveDaemon, self).__init__(address)
        self.functions_by_name = {}

    async def req_received(self, msg):
        func_name = msg.get('function_name')
        if func_name not in self.functions_by_name:
            print(f"error: unexpected RPC function_name: {func_name}")
            return

        for app in self.apps:
            await app.req_received__async(msg)


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
