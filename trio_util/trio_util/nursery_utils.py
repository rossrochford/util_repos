import signal


async def cancel_on_controlC(nursery):
    def signal_handler(sig, frame):
        print('You pressed Ctrl+C!')
        nursery.cancel_scope.cancel()

    signal.signal(signal.SIGINT, signal_handler)


'''
# how to use:

async def test_cntl():
    async with trio.open_nursery() as nursery:
        await cancel_on_cntl_c(nursery)
        await trio.sleep(9999)
'''