import asyncio
from ibapi import client as _client, wrapper as _wrapper, account_summary_tags
from dataclasses import dataclass


@dataclass(frozen=True)
class Endpoint:
    client: _client.EClient
    wrapper: _wrapper.EWrapper


def connect(host="127.0.0.1", port=7496) -> Endpoint:
    server = _wrapper.EWrapper()
    client = _client.EClient(server)
    client.connect(host, port, clientId=0)
    return Endpoint(client, server)


def get_account_information(app: Endpoint):
    app.client.reqAccountSummary(
        reqId=9001, groupName="All", tags=account_summary_tags.AccountSummaryTags.AllTags)


def main():
    server = _wrapper.EWrapper()
    client = _client.EClient(server)
    client.connect(host="127.0.0.1", port=7496, clientId=0)
    app = Endpoint(client, server)
    get_account_information(app)
    app.wrapper.accountSummary(9001, "U13973690", account_summary_tags.AccountSummaryTags.AllTags, "ALL", "USD")
    app.wrapper.nextValidId(9001)
    app.client.run()
