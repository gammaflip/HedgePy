from textual.app import ComposeResult
from textual.widgets import TabPane, Tree
from textual.message import Message
from textual.reactive import reactive

from src import api

IX = 2
SYMBOL = ":database:"


class DatabaseTree(Tree):
    def __init__(self):
        # get snapshot from api
        super().__init__(label='/')


class DatabaseTab(TabPane):

    results_queue = reactive(dict())

    class DatabaseQuery(Message):
        def __init__(self, msg: api.bases.Database.Query, corr_id: str):
            super().__init__()
            self.msg = msg
            self.corr_id = corr_id

    def __init__(self):
        super().__init__('database')

    def compose(self) -> ComposeResult:
        yield DatabaseTree()

    def watch_results_queue(self, results: dict):
        while results:
            corr_id, res = results.popitem()

