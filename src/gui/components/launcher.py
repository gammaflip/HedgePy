from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QWidget, QLineEdit, QPushButton, QLabel, QGridLayout

import api.bases.Database
from psycopg import OperationalError
from typing import Callable, Optional


class Launcher(QWidget):

    auth_send = pyqtSignal(api.bases.Database.Session)

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
        profile = api.bases.Database.Profile(user=self.user_field.text())
        session_object = api.bases.Database.Session(profile=profile)

        try:
            conn = session_object.new_connection(profile=profile, password=self.password_field.text())
            self.handle_good_auth(session_object)
        except OperationalError:
            self.handle_bad_auth()

    def handle_good_auth(self, session_object: api.bases.Database.Session):
        self.auth_send.emit(session_object)
        if isinstance(self.callback_on_close, Callable):
            self.callback_on_close()
        self.close()

    def handle_bad_auth(self):
        self.user_field.clear()
        self.password_field.clear()
        self.user_field.setFocus()

