import asyncio
from dataclasses import dataclass
from uuid import UUID, uuid4
from datetime import datetime
from typing import Any, Callable, Coroutine
from src.api.bases import Logger
from functools import partial

LOGGER = Logger.logger()
REFRESH_MS = 100


@dataclass
class Task:
    priority: int
    task: asyncio.Task

    def __post_init__(self):
        self.id = uuid4()
        self.created = datetime.now()

class Belt(object):
    def __init__(self):
        self.task_queue: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
        self.results: dict[UUID, Any] = {}
        self.stop = False

    async def cycle(self) -> tuple[datetime, UUID]:
        task = await self.task_queue.get()
        await task.task
        if task.task.done():
            result = task.task.result()
            self.results[task.id] = result
        return task.created, task.id

    async def start(self):
        while not self.stop:
            start_time = datetime.now()
            if self.task_queue.empty():
                await asyncio.sleep(REFRESH_MS/1000)
                LOGGER.info('task queue empty')
            else:
                created, id_ = await self.cycle()
                finish_time = datetime.now()
                LOGGER.info(f'task {id_} finished - {finish_time - created} from creation -\
                 {finish_time - start_time} to execute')





class Controller(object):
    def __init__(self, belt: Belt):
        self.loop = asyncio.new_event_loop()
        self.belt = belt

    async def schedule(self, func: Callable | Coroutine, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            ...
        else:
            func =


