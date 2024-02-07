import asyncio
import aiohttp
import psycopg
import psycopg_pool
from src import config
from src.api import vendors
from src.api.bases import IO, Message, Vendor
from typing import Optional
from functools import partial
from pathlib import Path


VENDOR_DIR = Vendor.ResourceMap(vendors)
MESSAGE_FACTORY = Message.MessageFactory()
ROOT = Path(config.PROJECT_ENV['SERVER_ROOT'])


async def read_one_message(reader: asyncio.StreamReader) -> Message.MessageType:
    header = await reader.readuntil(Message.DELIM)
    id, message_type, message_length = Message.unpack_header(header)

    content = await reader.readexactly(message_length)
    message = Message.pack(id, message_type, message_length, content)

    return MESSAGE_FACTORY(message_type, message)


# TODO: implement process_request and process_response after client
async def process_request(message: Message.Request) -> Message.Response:
    ...


async def process_response(message: Message.Response) -> Optional[Message.MessageType]:
    ...


async def process_one_message(
        db_pool: IO.DBPool,
        http_pool: IO.HTTPPool,
        message: Message.MessageType
) -> Optional[Message.MessageType]:
    match message.message_type:
        case 3:
            response = await process_request(message)
        case 4:
            response = await process_response(message)
        case _:
            raise NotImplementedError(f"No logic defined for message type: {message.message_type}")

    if response:
        return response


async def write_one_message(writer: asyncio.StreamWriter, msg: Message.MessageType):
    writer.write(msg.encode())
    await writer.drain()
    writer.close()


async def handle_connection(
        db_pool: IO.DBPool,
        http_pool: IO.HTTPPool,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
):
    while True:
        try:
            message = await read_one_message(reader)
            response = await process_one_message(db_pool, http_pool, message)

            if response:
                await write_one_message(writer, response)

        except Exception as e:
            print(e)
            break


async def run(db_conn_info: dict):
    controller = IO.Controller(debug=False)
    db_pool = psycopg_pool.AsyncConnectionPool(
        conninfo=psycopg.conninfo.make_conninfo(**db_conn_info),
        open=False)


