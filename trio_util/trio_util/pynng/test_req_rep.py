import random
import sys
import uuid

# from pynng import Push0, Pull0, Timeout
import pynng
import trio


'''
async def server_loop(worker_name, address):
    with pynng.Rep0(listen=address) as rep_sock:
        while True:
            msg = await rep_sock.arecv_msg()
            msg_bytes = msg.bytes
            print(f"        {worker_name} received: {msg_bytes}")
            await rep_sock.asend(b'ack:' + msg_bytes)
'''


async def server_loop(nursery, worker_name, address):

    async def handle_req(sock, bytes):
        await trio.sleep(random.randint(1, 60)/10)
        print(f"        {worker_name} received: {bytes}")
        await sock.asend(b'ack:' + bytes)

    with pynng.Rep0(listen=address) as rep_sock:
        while True:
            msg = await rep_sock.arecv_msg()
            msg_bytes = msg.bytes
            nursery.start_soon(
                handle_req, rep_sock, msg_bytes
            )


async def client_loop(worker_name, addresses):
    with pynng.Req0() as req_sock:
        for addr in addresses:
            req_sock.dial(addr)

        for i in range(30):
            uid = uuid.uuid4().hex[:8]
            await trio.sleep(0.5)
            print(f'{worker_name}: sending message')
            await req_sock.asend(f"msg {i}-{uid}".encode())
            msg = await req_sock.arecv_msg()
            print(f"{worker_name} received response: {msg.bytes.decode()}")
            print()


async def test():

    addr = "ipc:///tmp/test-pipeline.ipc"
    addresses = [addr + str(i) for i in range(1, 2)]

    async with trio.open_nursery() as nursery:
        for i, addr in enumerate(addresses):
            key = f'receiver{i+1}'
            nursery.start_soon(server_loop, nursery, key, addr)

        for n in range(5):
            await trio.sleep(0.3)
            nursery.start_soon(client_loop, f'sender{n}', addresses)


if __name__ == '__main__':

    trio.run(test)
    exit()
    keys = sys.argv[1:]
    trio.run(test, keys)
