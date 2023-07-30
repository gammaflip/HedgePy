import inspect
from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtWidgets import QWidget, QLineEdit, QPushButton, QLabel, QGridLayout, QVBoxLayout, QHBoxLayout
from api import session
from psycopg import OperationalError
from typing import Callable, Optional, Any, _GenericAlias
from collections import OrderedDict, namedtuple


class PyFunctionIO(QWidget):
    res = pyqtSignal(object)

    def __init__(self, function: Callable):
        self._f = function
        self._signature: inspect.Signature = inspect.signature(function)

        self._pos_args: Optional[OrderedDict] = None
        self._varargs: Optional[namedtuple] = None
        self._kwargs: Optional[namedtuple] = None

        self.process_signature()

        self._argspec: namedtuple = inspect.getfullargspec(function)

        if self._argspec.args:
            self._pos_args = OrderedDict(zip(self._argspec.args, [None for _ in self._argspec.args]))

        if self._argspec.varargs:
            self._varargs =

        super().__init__()

        self.setWindowTitle(self._f.__name__)

        self.execute_button = QPushButton('Execute')
        self.execute_button.clicked.connect(self.execute)
        self.clear_button = QPushButton('Clear')
        self.clear_button.clicked.connect(self.clear_fields)
        self.add_kwarg_button = QPushButton('+kwarg')
        self.add_kwarg_button.clicked.connect(self.add_kwarg)
        self.add_kwarg_button.setDisabled(True)
        self.remove_kwarg_button = QPushButton('-kwarg')
        self.remove_kwarg_button.clicked.connect(self.remove_kwarg)
        self.remove_kwarg_button.setDisabled(True)
        self.add_arg_button = QPushButton('+arg')
        self.add_arg_button.clicked.connect(self.add_arg)
        self.add_arg_button.setDisabled(True)
        self.remove_arg_button = QPushButton('-arg')
        self.remove_arg_button.clicked.connect(self.remove_arg)
        self.remove_arg_button.setDisabled(True)

        self.outer_layout = QVBoxLayout()
        self.setLayout(self.outer_layout)
        self.inner_layout_fields = QVBoxLayout()
        self.outer_layout.addLayout(self.inner_layout_fields)

        self.fields = self.populate_fields()

        self.inner_layout_buttons = QVBoxLayout()
        row1, row2 = QHBoxLayout(), QHBoxLayout()



        self.inner_layout_buttons.addWidget(self.execute_button)
        self.inner_layout_buttons.addWidget(self.clear_button)
        self.outer_layout.addLayout(self.inner_layout_buttons)

        if 'return' in self.annotations:
            self.ret = QLabel(f'returns {self.annotations["return"]}')
            self.ret.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.outer_layout.addWidget(self.ret)


    def process_signature(self) -> None:
        for param in self._signature.parameters.values():
            match param.kind:
                case inspect.Parameter.POSITIONAL_ONLY:
                    self._pos_args[param.name] = (None, param.annotation)
                case inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    self._pos_args[param.name] = (None, param.annotation)
                case inspect.Parameter.VAR_POSITIONAL:
                    ...
                case inspect.Parameter.KEYWORD_ONLY:
                    self._kwargs =
                case inspect.Parameter.VAR_KEYWORD:
                    ...

    @property
    def args(self) -> list[str]: return self._argspec.args

    @property
    def annotations(self) -> OrderedDict[str, Any]: return OrderedDict(self._argspec.annotations)

    @property
    def defaults(self) -> OrderedDict[str, Any]:
        try:
            n = len(self._argspec.defaults)
            return OrderedDict(zip(self.args[-n:], self._argspec.defaults))
        except TypeError:
            return OrderedDict()

    def populate_fields(self) -> dict:
        res = dict()

        for arg in self.args:

            lbl, ann = arg, None

            if arg in self.annotations:
                ann = self.annotations[arg]
                lbl += f': {ann}'

            lbl += ' ='

            if arg in self.defaults:
                default = self.defaults[arg]
            else:
                default = None

            row = QHBoxLayout()

            label = QLabel(lbl)
            label.setAlignment(Qt.AlignmentFlag.AlignLeft)

            field = QLineEdit()
            field.setAlignment(Qt.AlignmentFlag.AlignRight)

            row.addWidget(label)
            row.addWidget(field)

            self.inner_layout_fields.addLayout(row)

            if default:
                field.setText(str(default))

            res[arg] = field, ann

        return res

    def clear_fields(self) -> None:
        for arg, (field, ann) in self.fields.items():
            field.clear()

    def execute(self) -> Any:
        entered_values = self.get_entered_values()
        res = self.f(**entered_values)
        self.res.emit(res)
        return res

    def add_kwarg(self) -> None:
        ...

    def remove_kwarg(self) -> None:
        ...

    def add_arg(self) -> None:
        ...

    def remove_arg(self) -> None:
        ...

    def get_entered_values(self) -> dict:
        res = dict()

        for arg, (field, ann) in self.fields.items():
            value = field.text()

            if ann:
                if isinstance(ann, type):
                    value = ann(value)
                elif issubclass(ann, _GenericAlias):
                    typs = ann.__args__
                    for typ in typs:
                        try:
                            value = typ(value)
                            break
                        except ValueError:
                            continue

            res[arg] = value

        return res


class Launcher(QWidget):

    auth = pyqtSignal(str, str)

    def __init__(self, callback_on_close: Optional[Callable]):
        super().__init__()
        self.callback_on_close = callback_on_close

        self.user_field, self.password_field = QLineEdit(), QLineEdit()
        self.password_field.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)

        user_label, password_label = QLabel('User:'), QLabel('Pass:')

        login_button = QPushButton('Go')
        login_button.clicked.connect(self.on_auth)

        layout = QGridLayout()
        layout.addWidget(user_label, 0, 0)
        layout.addWidget(self.user_field, 0, 1)
        layout.addWidget(password_label, 1, 0)
        layout.addWidget(self.password_field, 1, 1)
        layout.addWidget(login_button, 2, 0, 1, 2)

        self.setLayout(layout)
        self.setWindowTitle('Launcher')
        self.setGeometry(100, 100, 300, 150)

    def on_auth(self):
        profile = session.Profile(user=self.user_field.text())

        try:
            session_object = session.Session(profile=profile, password=self.password_field.text())
        except OperationalError:
            self.handle_bad_connection()

        self.handle_good_connection()

    def handle_bad_connection(self):
        self.user_field.clear()
        self.password_field.clear()
        self.user_field.setFocus()

    def handle_good_connection(self):
        self.auth.emit(self.user_field.text(), self.password_field.text())

        if isinstance(self.callback_on_close, Callable):
            self.callback_on_close()

        self.close()
