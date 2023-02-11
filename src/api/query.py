from pandas import Series
from numpy import array
from enum import Enum
from psycopg.cursor import Cursor
from psycopg.sql import SQL, Identifier, Composed
from psycopg.rows import RowMaker, tuple_row, dict_row, class_row, args_row, kwargs_row
from src.data.data import Database, Schema, Table, Column, Index, Packet
from typing import Literal, Sequence, Any, Callable


"""
ROW FACTORIES
"""


def np_row(cursor: Cursor) -> RowMaker:

    def row_maker(values: Sequence) -> np.ndarray:
        return array(values)

    return row_maker


def pd_row(cursor: Cursor) -> RowMaker:
    cols = [c.name for c in cursor.description]

    def row_maker(values: Sequence) -> pd.Series:
        return Series(data=values, index=cols)

    return row_maker


def value_row(cursor: Cursor) -> RowMaker:

    def row_maker(values: Sequence) -> Any:
        assert len(values) == 1
        return values[0]

    return row_maker


class RowFactories(Enum):
    tuple_row = tuple_row
    dict_row = dict_row
    class_row = class_row
    args_row = args_row
    kwargs_row = kwargs_row
    np_row = np_row
    pd_row = pd_row
    value_row = value_row


"""
COMPONENT FUNCTIONS
"""


def _create_database()

"""
QUERY
"""

ACTIONS = Enum('actions', ['create', 'read', 'update', 'delete', 'list'])
CLASSES = Enum('classes', ['database', 'schema', 'table', 'column', 'index'])

def _resolve_func(pair: tuple[int, int]) -> Callable:
    i, j = pair
    action, cls = CLASSES(i), ACTIONS(j)
    ...


def query(*args, **kwargs) -> Any:
    """

    Parameters
    ----------
    cls: corresponds with SQLObject subclass from bases module
    action: one of the standard CRUD(L) actions
    args: passed to underlying function
    kwargs: passed to underlying function

    Returns
    -------

    """

    # the product of enum indices provides the function to be called
    ix = getattr(CLASSES, cls).value * getattr(ACTIONS, action).value



