from textual import events
from textual.app import ComposeResult
from textual.message import Message
from textual.screen import Screen
from textual.widgets import Input, Label, Static


class LoginPrompt(Static):
    _DBNAME = Input(value='hedgepy', placeholder='hedgepy', id='DBNameInput')
    _DBPORT = Input(value='5432', placeholder='5432', id='DBPortInput')
    _DBUSER = Input(value='postgres', placeholder='postgres', id='DBUserInput')
    _DBPASS = Input(password=True, id='DBPasswordInput')

    class LoginMessage(Message):
        def __init__(self, dbname: str, dbport: str, dbuser: str, dbpass: str):
            super().__init__()
            self.dbname = dbname
            self.dbport = dbport
            self.dbuser = dbuser
            self.dbpass = dbpass

    def compose(self) -> ComposeResult:
        yield Label("Login", id="LoginLabel")
        yield Label("Database:", classes="LoginFieldLabel")
        yield self._DBNAME
        yield self._DBPORT
        yield Label("User:", classes="LoginFieldLabel")
        yield self._DBUSER
        yield Label("Password:", classes="LoginFieldLabel")
        yield self._DBPASS

    async def on_key(self, event: events.Key) -> None:
        if event.key == "enter":
            self.post_message(
                self.LoginMessage(
                    dbname=self._DBNAME.value,
                    dbport=self._DBPORT.value,
                    dbuser=self._DBUSER.value,
                    dbpass=self._DBPASS.value
                )
            )


class LoginWindow(Screen):
    def compose(self) -> ComposeResult:
        yield LoginPrompt()
