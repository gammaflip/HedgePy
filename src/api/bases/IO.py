import asyncio
import json
import jsonschema
import uuid
import struct
from abc import ABC, abstractmethod
from typing import TypeVar


FMT_HEADER = "!16s2I"
FMT_DELIM = "x"
DELIM = b"\x00"


"""
MESSAGE CLASSES
"""


class _ABCMessage(ABC, object):
    FMT_HEADER: str = ""
    FMT_DELIM: str = ""
    DELIM: bytes = b""

    @abstractmethod
    def __init__(self, msg: bytes):
        header_slc, content_slc = self.header_size, self.header_size + self.delim_size
        header, delim, content = msg[:header_slc], msg[header_slc:content_slc], msg[content_slc:]

        self._header = struct.unpack(self.FMT_HEADER, header)
        self._content = content
        self._format = f"{self.FMT_HEADER}{self.FMT_DELIM}{len(self._content)}s"

    @property
    def header_size(self) -> int:
        return struct.calcsize(self.FMT_HEADER)

    @property
    def delim_size(self) -> int:
        return struct.calcsize(self.FMT_DELIM)

    @property
    def header(self) -> tuple:
        return self._header

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def format(self) -> str:
        return self._format

    def encode(self) -> bytes:
        return struct.pack(self.format, *self._header, self._content)


class Message(_ABCMessage):
    FMT_HEADER = FMT_HEADER
    FMT_DELIM = FMT_DELIM
    DELIM = DELIM
    MESSAGE_TYPE: int = 0

    def __init__(self, msg: bytes):
        super().__init__(msg)
        self._id, self._message_type, self._message_length = self._header

    @property
    def id(self) -> uuid.UUID:
        return uuid.UUID(bytes=self._id)

    @property
    def message_type(self) -> int:
        return self._message_type

    @property
    def message_length(self) -> int:
        return self._message_length


class StringMessage(Message):
    ENCODING: str = "utf-8"
    MESSAGE_TYPE: int = 1

    @property
    def content(self) -> str:
        return self._content.decode(self.ENCODING)


class JsonMessage(StringMessage):
    SCHEMA: dict = dict()
    VALIDATOR: jsonschema.protocols.Validator = jsonschema.Draft202012Validator
    MESSAGE_TYPE: int = 2

    def __init__(self, msg: bytes):
        super().__init__(msg)

        self.VALIDATOR.check_schema(self.SCHEMA)

        jsonschema.validate(
            instance=self.content,
            schema=self.SCHEMA,
            cls=self.VALIDATOR,
            format_checker=self.VALIDATOR.FORMAT_CHECKER
        )

    @property
    def content(self) -> dict:
        return json.loads(super().content)


"""
REQUEST/RESPONSE CLASSES
"""


class Request(JsonMessage):
    MESSAGE_TYPE = 3
    SCHEMA = {
      "type": "object",
      "properties": {
          "fields": {"type": "array", "items": {"type": "string"}},
          "symbols": {"type": "array", "items": {"type": "string"}},
          "start": {"type": "string", "format": "date-time"},
          "end": {"type": "string", "format": "date-time"},
          "resolution": {"type": "string", "format": "duration"}
      }
    }


class Response(JsonMessage):
    MESSAGE_TYPE = 4
    SCHEMA = {
        "type": "object",
        "properties": {
            "content": {"type": "array", "items": {"type": "array"}},
            "error": {"type": "string"}
        }
    }


MESSAGES = (Message, StringMessage, JsonMessage, Request, Response)
MessageType = TypeVar("MessageType", *MESSAGES)


def _make_string_message(message: str):
    id = uuid.uuid4().bytes
    message_type = 0
    message = bytes(message, StringMessage.ENCODING)
    message_length = len(message)
    fmt = f"!16s2Ix{message_length}s"
    return struct.pack(fmt, id, message_type, message_length, message)


def _make_json_message(message: dict):
    id = uuid.uuid4().bytes
    message_type = 0
    message = bytes(json.dumps(message), StringMessage.ENCODING)
    message_length = len(message)
    fmt = f"!16s2Ix{message_length}s"
    return struct.pack(fmt, id, message_type, message_length, message)
