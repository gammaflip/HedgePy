import asyncio
import struct
from src.api.bases import Database, IO
from src import config
from types import NoneType


async def read_one_message(reader: asyncio.StreamReader) -> IO.MessageType:
    header = await reader.readuntil(IO.DELIM)
    id, message_type, message_length = struct.unpack(IO.FMT_HEADER, header)
    content = await reader.readexactly(message_length)
    msg = struct.pack(
        f"{IO.FMT_HEADER}{IO.DELIM}{message_length}s",
        id,
        message_type,
        message_length,
        IO.DELIM,
        content
    )

    return IO.MESSAGES[message_type](msg)


async def process_one_message(message: IO.MessageType) -> NoneType | IO.MessageType:
    ...


async def write_one_message(writer: asyncio.StreamWriter, msg: IO.MessageType):
    writer.write(msg.encode())
    await writer.drain()


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        try:
            message = await read_one_message(reader)
            response = await process_one_message(message)

            if response:
                await write_one_message(writer, response)

        except Exception as e:
            print(e)
            break


async def start():
    server = await asyncio.start_server(
        handle_connection,
        host=config.PROJECT_ENV['SERVER_HOST'],
        port=config.PROJECT_ENV['SERVER_PORT']
    )



    with server:
        ...

