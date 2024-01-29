from textual.app import ComposeResult
from textual.widgets import Tree, DataTable, Static
from textual.message import Message
from src.api.bases import IO, Query
from typing import Optional, Callable


IX = 2
SYMBOL = ":database:"


class Request(Message):
    def __init__(self, request: IO.DBRequest, callback: Optional[Callable] = None):
        super().__init__()
        self.request = request
        self.callback = callback


class DataView(DataTable):
    ...


class DataTree(Tree):
    def __init__(self):
        super().__init__("/")
        self.show_root = False
        self.guide_depth = 2

    async def init(self):
        request = Query.snapshot()
        request = Request(request)
        request.callback = self.populate_tree
        self.post_message(request)

    async def populate_tree(self, result: IO.Result):
        df = result.content.df
        db_li = df['database'].unique()
        for db in db_li:
            db_node = self.root.add(db, expand=True)
            sch_li = df[df['database']==db]['schema'].unique()
            for sch in sch_li:
                sch_node = db_node.add(sch, expand=True)
                tbl_li = df[(df['database']==db) & (df['schema']==sch)]['table'].unique()
                for tbl in tbl_li:
                    sch_node.add_leaf(tbl)
