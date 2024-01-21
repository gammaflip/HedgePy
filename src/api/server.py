import asyncio
from src.api.bases import IO, Message
from src import config
from types import NoneType
from functools import partial


MESSAGE_FACTORY = Message.MessageFactory()


async def read_one_message(reader: asyncio.StreamReader) -> Message.MessageType:
    header = await reader.readuntil(Message.DELIM)
    id, message_type, message_length = Message.unpack_header(header)

    content = await reader.readexactly(message_length)
    message = Message.pack(id, message_type, message_length, content)

    return MESSAGE_FACTORY(message_type, message)


async def process_request(message: Message.Request) -> Message.Response:
    ...


async def process_response(message: Message.Response) -> NoneType | Message.MessageType:
    ...


async def process_one_message(
        db_pool: IO.DBPool,
        http_pool: IO.HTTPPool,
        message: Message.MessageType
) -> NoneType | Message.MessageType:
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


async def main(db_conn_info: dict):
    db_pool = IO.DBPool(conn_info=db_conn_info)
    http_pool = IO.HTTPPool()

    async with db_pool, http_pool:
        handler = partial(handle_connection, db_pool, http_pool)
        host, port = config.PROJECT_ENV['SERVER_HOST'], config.PROJECT_ENV['SERVER_PORT']
        server = await asyncio.start_server(handler, host=host, port=port)

        async with server:
            await server.serve_forever()
