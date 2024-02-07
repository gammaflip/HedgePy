import asyncio
import threading
from src.tui import tui
from src.api import client, server


async def start(password: str, **kwargs):
    tui_inst = tui.HedgePyApp()
    tui_thread = threading.Thread(target=tui_inst.run)

    await client.connect(password, **kwargs)
    kwargs['password'] = password
    api_thread = threading.Thread(target=server.run, args=(kwargs,))
