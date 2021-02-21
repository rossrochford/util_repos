import sys

from pynng import Push0, Pull0, Timeout
import trio


async def receive_loop(worker_name, address):
    with Pull0(listen=address) as pull_sock:
        while True:
            msg = await pull_sock.arecv_msg()
            msg_bytes = msg.bytes
            print(f"{worker_name} received: {msg_bytes}")


async def send_loop(worker_name, addresses):
    push_sock = Push0(send_timeout=5 * 1000)
    for addr in addresses:
        push_sock.dial(addr)

    for i in range(30):
        await trio.sleep(1)
        await push_sock.asend(f"{worker_name} sent msg {i}".encode())


async def testOLD(keys):

    addr = "ipc:///tmp/test-pipeline.ipc"

    async with trio.open_nursery() as nursery:
        for key in keys:
            if key.startswith('receiver'):
                nursery.start_soon(receive_loop, key, addr)
            elif key.startswith('sender'):
                nursery.start_soon(send_loop, key, addr)


async def test():

    addr = "ipc:///tmp/test-pipeline.ipc"
    addresses = [addr + str(i) for i in range(1, 4)]

    async with trio.open_nursery() as nursery:
        for i, addr in enumerate(addresses):
            key = f'receiver{i+1}'
            nursery.start_soon(receive_loop, key, addr)

        nursery.start_soon(send_loop, 'sender1', addresses)


if __name__ == '__main__':
    trio.run(test)
    exit()
    keys = sys.argv[1:]
    trio.run(test, keys)
