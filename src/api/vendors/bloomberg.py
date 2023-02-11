import os
import config

with os.add_dll_directory(config.ENV['LIB']):
    import blpapi
    from blpapi import Event, DataType, SessionOptions

from typing import Any, Optional, Union
from functools import singledispatchmethod

from data.bases.data import Packet


#######################################################################################################################
### GLOBALS
#######################################################################################################################


EVENTS = {1: Event.ADMIN, 2: Event.SESSION_STATUS, 3: Event.SUBSCRIPTION_STATUS,
          4: Event.REQUEST_STATUS, 5: Event.RESPONSE, 6: Event.PARTIAL_RESPONSE,  # NOTE: there is no 7
          8: Event.SUBSCRIPTION_STATUS, 9: Event.SERVICE_STATUS, 10: Event.TIMEOUT,
          11: Event.AUTHORIZATION_STATUS, 12: Event.RESOLUTION_STATUS, 13: Event.TOPIC_STATUS,
          14: Event.TOKEN_STATUS, 15: Event.REQUEST}
TYPES = {1: DataType.BOOL, 2: DataType.CHAR, 3: DataType.BYTE, 4: DataType.INT32,
         5: DataType.INT64, 6: DataType.FLOAT32, 7: DataType.FLOAT64, 8: DataType.STRING,
         9: DataType.BYTEARRAY, 10: DataType.DATE, 11: DataType.TIME, 12: DataType.DECIMAL,
         13: DataType.DATETIME, 14: DataType.ENUMERATION, 15: DataType.SEQUENCE, 16: DataType.CHOICE,
         17: DataType.CORRELATION_ID}
SERVICES = {'refdata': ['HistoricalDataRequest', 'IntraDayTickRequest', 'IntraDayBarRequest', 'ReferenceDataRequest'],
            'apiflds': ['FieldInfoRequest', 'FieldSearchRequest', 'CategorizedFieldSearch Request'],
            'instruments': ['InstrumentListRequest', 'CurveListRequest', 'GovtListRequest']}
SESSION_OPTIONS = SessionOptions()


#######################################################################################################################
### BASES
#######################################################################################################################


class Client:
    def __init__(self):
        self._session = blpapi.Session(SESSION_OPTIONS)
        self._session.start()
        self._event_handler = EventHandler(self)

        self.services = {}
        for service in SERVICES:
            self._session.openService('//blp/' + service)
            service_object = self._session.getService('//blp/' + service)
            self.services[service] = service_object

        # self._flush_events()

    @property
    def session(self): return self._session

    @property
    def event_handler(self): return self._event_handler

    def _flush_events(self):
        flushed = False
        while not flushed:
            event = self.session.nextEvent()

    def send(self, service: str, request_type: str, payload: Packet) -> blpapi.CorrelationId:
        request = Request(self)
        return request(service=service, request_type=request_type, payload=payload)

    def receive(self):
        ...


class Request:
    def __init__(self, client: Client):
        self.client = client

    def __call__(self, service: str, request_type: str, payload: Packet) -> blpapi.CorrelationId:
        request = self.create_request(service, request_type)
        request = self.format_request(request, payload)
        cid = self.send_request(request)
        return cid

    def create_request(self, service: str, request_type: str) -> blpapi.Request:
        assert (service in SERVICES.keys() and request_type in SERVICES[service])
        service_object = self.client.services[service]
        request_object = service_object.createRequest(request_type)
        return request_object

    def format_request(self, request_object: blpapi.Request, payload: Packet) -> blpapi.Request:
        for [key], value in payload.flatten():
            elem = request_object.getElement(key)
            self._format_elem(value, elem)
        return request_object

    def send_request(self, request_object: blpapi.Request) -> blpapi.CorrelationId:
        cid = request_object.getRequestId()
        self.client.session.sendRequest(request_object)
        return blpapi.CorrelationId(cid)

    @singledispatchmethod
    def _format_elem(self, value, elem: blpapi.Element):
        elem.setValue(value)

    @_format_elem.register
    def _(self, value: list, elem: blpapi.Element):
        for _ in value:
            elem.appendValue(_)

    @_format_elem.register
    def _(self, value: tuple, elem: blpapi.Element):
        elem.appendValue(list(value))


class EventHandler:
    def __init__(self, client: Client):
        self.client = client

    def __iter__(self):
        ...

    def __next__(self):
        event = self.client.session.nextEvent()
        res = self._handle_event(event)

    def _handle_event(self, event: blpapi.Event):
        event_type, handled = event.eventType(), False
        for msg in event:
            self._handle_message(msg)

    def _handle_message(self, msg: blpapi.Message):
        msg_type = str(msg.messageType())
        msg = Packet(msg.toPy())
        for ix, val in msg.flatten():
            ...

    def _flush(self):
        finished = False
        while not finished:
            event = self.__next__()


#######################################################################################################################
### FUNCTIONS
#######################################################################################################################

def get(): ...

def format(): ...
