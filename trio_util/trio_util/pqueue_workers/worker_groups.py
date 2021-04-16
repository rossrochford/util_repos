from collections import namedtuple
import datetime
import uuid

import trio

from eliot import start_action  # log_call
from trio_util.pqueue_workers.pqueue import PriorityQueue


class PqueueWorkerGroup(object):

    def __init__(self, work_types, workers, notify_exhausted=False):
        assert type(work_types) in (tuple, list)
        self.work_types = work_types
        self.priority_queue = PriorityQueue()
        self.next_worker_index = 0

        self.workers = workers
        for w in workers:
            w.parent_group = self
        self.notify_exhausted = notify_exhausted

    def choose_worker(self, msg_dict):
        if len(self.workers) == 1:
            return self.workers[0]
        index = self.choose_worker_index(msg_dict)
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
        self.uid = uuid.uuid4().hex[:16]
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

    @property
    def funcname2(self):
        return str(self.uid) + '-' + self.func.__name__

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
            if item == 'exit':
                break
            succ, item = await self.preprocess_item(item)
            if not succ:
                continue
            await self.func(self, global_ctx, item)

        print(f"worker {self.uid}:{self.funcname} finished")


class BatchWorker(object):

    def __init__(self, worker_config, **kwargs):
        self.uid = uuid.uuid4().hex[:16]
        self.parent_group = None  # set in parent

        self.func = worker_config.func
        self.extra_kwargs = kwargs
        self.batch_size = worker_config.batch_size
        self.batch_delay = worker_config.batch_delay

        send_channel, receive_channel = trio.open_memory_channel(
            worker_config.channel_size
        )
        self.send_channel = send_channel
        self.receive_channel = receive_channel

        self.completion_event = trio.Event()

    @property
    def funcname(self):
        return self.func.__name__

    @property
    def funcname2(self):
        return str(self.uid) + '-' + self.func.__name__

    async def setup_worker_resources(self, global_ctx):
        pass

    async def preprocess_item(self, item_dict):
        return True, item_dict

    async def _process_batch(self, global_ctx, curr_batch):
        bf = datetime.datetime.now()
        action_args = dict(
            action_type="execute_batch", batch_size=len(curr_batch),
            work_type=self.parent_group.work_types[0],
        )
        with start_action(**action_args):
            await self.func(self, global_ctx, curr_batch)
        af = datetime.datetime.now()
        duration = round((af - bf).total_seconds())
        print(f"BATCH COMPLETE: {self.uid}.{self.funcname}: {len(curr_batch)} profiles, took {duration}s")

    async def worker_loop(self, global_ctx):

        print(f"starting worker {self.uid}:{self.funcname}")

        await self.setup_worker_resources(global_ctx)

        curr_batch = []
        async for msg in self.receive_channel:
            # print(f"{self.uid}:{self.funcname} received msg: {msg} (curr_batch size: {len(curr_batch)})")
            if msg in ('exit', 'flush') or len(curr_batch) >= self.batch_size:
                if curr_batch:
                    await self._process_batch(global_ctx, curr_batch)
                    curr_batch = []
                if msg == 'flush':
                    continue
                elif msg == 'exit':
                    self.completion_event.set()
                    break

            succ, item = await self.preprocess_item(msg)
            if not succ:
                continue
            curr_batch.append(item)

        print(f"worker {self.uid}:{self.funcname} finished")
