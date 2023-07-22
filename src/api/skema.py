import yaml
from datetime import datetime
from config import PATH
from typing import Literal, Optional, Self, Any, TypeVar
from types import NoneType
from dataclasses import dataclass
from collections import UserList
from bases import db


LEVELS = {'database': db.Database, 'schema': db.Schema, 'table': db.Table, 'column': db.Column}


"""
BASE OBJECTS
"""


class Attrs:




class Node(db.SQLObject):
    def __init__(self, name: str, parent: Optional[Self] = None):
        super().__init__(name, parent)
        self._attrs: Attrs = Attrs()

def load_yml(name: str) -> dict:
    with open(f'{PATH["skema"]}/{name}.yml', 'r') as file:
        return yaml.safe_load(file)


class Tree(UserList):
    def _process_selected_column(self, selected: dict, parent_attrs: Attrs):
        name = selected.pop('name')
        attrs = Attrs(**selected.pop('attrs')) if 'attrs' in selected.keys() else Attrs()
        dtype = selected.pop('dtype')
        pkey = selected.pop('pkey') if 'pkey' in selected.keys() else None
        fkey = selected.pop('fkey') if 'fkey' in selected.keys() else None

    def _process_selected_other(self, selected: dict, parent_attrs: Attrs):
        name = selected.pop('name')
        attrs = Attrs(**selected.pop('attrs')) if 'attrs' in selected.keys() else Attrs()

    def _process_level(self, obj: dict, parent_attrs: Attrs):
        level_type, level_obj = obj.popitem()
        for selected in level_obj:
            match level_type:
                case 'database' | 'schema' | 'table':
                    self._process_selected_other(level_obj)
                case 'column':
                    self._process_selected_column(level_obj)
                case _:
                    raise ValueError(f'invalid level: {level_type} is not in ("database", "schema", "table", "column")')


        selected_name = obj.pop('name')
        selected_attrs_dict = parent_attrs.__dict__.copy()
        if 'attrs' in obj.keys():
            selected_attrs_dict.update(obj.pop('attrs'))

    def _process_branch(self, branch: str):
        root = load_yml(branch)
        root_attrs = Attrs()
        self._process_level(root, root_attrs)

        for db in root:
            name = db.pop('name')
            schema = db.pop('schema')
            db_obj = Database(name)
            attrs = Attrs(**db.pop('attrs')) if 'attrs' in db else Attrs()
            db_attrs = Attrs(**db)

            for sch in schema:
                name = sch.pop('name')
                table = sch.pop('table')
                sch_obj = Schema(name, db_obj)
                sch_attrs = Attrs(**sch)

                for tab in table:
                    name = tab.pop('name')
                    column = tab.pop('column')
                    tab_obj = Table(name, sch_obj)
                    tab_attrs = Attrs(**tab)

                    for col in column:
                        name = col.pop('name')
                        dtype = col.pop('dtype')
                        pkey = col.pop('pkey') if 'pkey' in col else None
                        fkey = col.pop('fkey') if 'fkey' in col else None
                        col_obj = Column(name, dtype, tab_obj, pkey, fkey)
                        col_attrs = Attrs(**col)

                        final_attrs = dict()
                        for attr in (db_attrs, sch_attrs, tab_attrs, col_attrs):
                            for k, v in attr.__dict__.items():
                                if v:
                                    final_attrs[k] = v
                        final_attrs = Attrs(**final_attrs)

                        self.data.append(Node(db_obj, sch_obj, tab_obj, col_obj, final_attrs))
