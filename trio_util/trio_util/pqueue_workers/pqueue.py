from heapq import heappush, heappop

import trio


class PriorityQueue(object):

    def __init__(self):
        self._not_empty = trio.Event()
        self._queue = []
        self._counter = 0

    @property
    def queue_size(self):
        return len(self._queue)

    def put_nowait(self, prio, item):
        heappush(self._queue, (prio, self._counter, item))
        self._counter += 1
        if not self._not_empty.is_set():
            self._not_empty.set()

    def get_nowait(self):
        if not self._not_empty.is_set():
            raise trio.WouldBlock()

        prio, _, item = heappop(self._queue)

        if not self._queue:
            self._not_empty = trio.Event()

        return prio, item

    async def get(self):
        while True:
            await self._not_empty.wait()
            try:
                return self.get_nowait()
            except trio.WouldBlock:
                pass
