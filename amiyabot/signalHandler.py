import signal
import inspect
import asyncio

from typing import List, Callable


class SignalHandler:
    on_shutdown: List[Callable] = []
    shutdown_event = asyncio.Event()
    shutdown_tasks: List[asyncio.Task] = []

    @classmethod
    def exec_shutdown_handlers(cls):
        cls.shutdown_tasks.clear()
        for action in cls.on_shutdown:
            if inspect.iscoroutinefunction(action):
                cls.shutdown_tasks.append(asyncio.create_task(action()))
            else:
                action()


def sigint_handler(*args):
    if getattr(sigint_handler, '_handled', False):
        return
    sigint_handler._handled = True
    SignalHandler.exec_shutdown_handlers()
    SignalHandler.shutdown_event.set()


signal.signal(signal.SIGINT, sigint_handler)
