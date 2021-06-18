from collections import namedtuple
import datetime
from random import randint
import uuid

import trio

from eliot import start_action  # log_call
from trio_util.pqueue_workers.items import ControlItem
from trio_util.pqueue_workers.pqueue import PriorityQueue


class PqueueWorkerGroup(object):

    def __init__(self, work_types, workers, notify_exhausted=False):
        assert type(work_types) in (tuple, list)
        self.work_types = work_types
        self.priority_queue = PriorityQueue()
        self.next_worker_index = 0
        self.prev_index = 0

        self.workers = workers
        for w in workers:
            w.parent_group = self
        self.notify_exhausted = notify_exhausted

    def choose_worker(self, msg_dict, random=False):
        if len(self.workers) == 1:
            return self.workers[0]
        assert len(self.workers) > 1

        if random:
            # when a channel is full, we retry once with another worker
            # see: daemon__route_items_to_worker_channels()
            index = randint(0, len(self.workers)-1)
            while index != self.prev_index:  # ensure no repetition
                index = randint(0, len(self.workers)-1)
        else:
            index = self.choose_worker_index(msg_dict)

        self.prev_index = index
        return self.workers[index]

    def choose_worker_index(self, msg_dict):
        # round-robin
        self.next_worker_index = (self.next_worker_index + 1) % len(self.workers)
        return self.next_worker_index


BatchWorkerConfig = namedtuple('WorkerConfig', [
    'func', 'batch_size', 'batch_delay', 'channel_size', 'num_workers_per_session'
])


class ItemWorker(object):

    def __init__(self, func, **kwargs):
        self.parent_group = None  # set in parent
        self.func = func
        self.extra_kwargs = kwargs

        send_channel, receive_channel = trio.open_memory_channel(3)
        self.send_channel = send_channel
        self.receive_channel = receive_channel

        self.completion_event = trio.Event()

    @property
    def funcname(self):
        return self.func.__name__

    async def setup_worker_resources(self):
        pass

    async def preprocess_item(self, item_dict):
        return True, item_dict

    async def worker_loop(self, global_ctx):

        print(f"starting worker {self.uid}:{self.funcname}")

        await self.setup_worker_resources()

        curr_batch = []
        async for item in self.receive_channel:
            print(f"{self.uid}:{self.funcname} received item: {item} (curr_batch size: {len(curr_batch)})")
            if item.get('exit'):
                break

            succ, item = await self.preprocess_item(item)
            if not succ:
                continue

            await self.func(self, global_ctx, item)

        print(f"worker {self.uid}:{self.funcname} finished")


class BatchWorker(object):

    def __init__(self, worker_name, worker_config, **kwargs):
        self.parent_group = None  # set in parent

        self.worker_name = worker_name
        self.worker_config = worker_config
        self.func = worker_config.func
        self.extra_kwargs = kwargs
        self.batch_size = worker_config.batch_size
        self.batch_delay = worker_config.batch_delay
        self.send_channel, self.receive_channel = trio.open_memory_channel(
            worker_config.channel_size
        )
        self.completion_event = trio.Event()

    @property
    def funcname(self):
        return self.func.__name__

    async def setup_worker_resources(self, global_ctx):
        pass

    async def preprocess_item(self, item_dict):
        return True, item_dict

    async def _process_batch(self, global_ctx, curr_batch):
        bf = datetime.datetime.now()

        action_args = dict(
            action_type="execute_batch", batch_size=len(curr_batch),
            worker_name=self.worker_name
        )
        with start_action(**action_args):
            await self.func(self, global_ctx, curr_batch)

        af = datetime.datetime.now()
        duration = round((af - bf).total_seconds())
        print(f"BATCH COMPLETE: {self.worker_name}.{self.funcname}: {len(curr_batch)} profiles, took {duration}s")

        await self._postprocess_batch(global_ctx, curr_batch)

    async def _postprocess_batch(self, global_ctx, curr_batch):
        pass

    async def worker_loop(self, global_ctx):

        print(f"starting worker {self.worker_name}:{self.funcname}")

        await self.setup_worker_resources(global_ctx)

        curr_batch = []
        async for msg in self.receive_channel:
            # print(f"{self.worker_name} received msg: {msg} (curr_batch size: {len(curr_batch)})")

            succ, item = await self.preprocess_item(msg)
            if not succ:
                continue

            if type(item) is ControlItem or len(curr_batch) >= self.batch_size:
                print(f"flushing: {self.worker_name}, num items: {len(curr_batch)}")

                if curr_batch:
                    await self._process_batch(global_ctx, curr_batch)
                    curr_batch = []

                if type(item) is ControlItem:
                    if item.flush_group:
                        continue
                    elif item.exit:
                        self.completion_event.set()
                        break

            curr_batch.append(item)

        print(f"worker {self.worker_name}:{self.funcname} finished")
