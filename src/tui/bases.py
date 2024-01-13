from pathlib import Path

from src import config
from src.api import api
from src.api.bases.Database import Profile, Session, Connection

from src.tui import content

from textual import events
from textual.app import App, ComposeResult
from textual.widget import Widget
from textual.containers import Vertical, Horizontal
from textual.widgets import Header, Input, TabbedContent, Static, Label
from textual.screen import Screen, ModalScreen
from textual.reactive import reactive
from textual.message import Message


ROOT = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'tui'


def load_css() -> list[str]:
    folder, ext, res = "css/", ".tcss", []
    for filename in (ROOT / 'css').glob(f"*{ext}"):
        res.append(folder + filename.stem + ext)
    return res


def load_content() -> list[Widget]:
    from src.tui import content
    res = []
    for mod_name in dir(content):
        if not mod_name.startswith('_'):
            mod = getattr(content, mod_name)
            if hasattr(mod, 'Content'):
                res.append(getattr(mod, 'Content'))
            else:
                raise ValueError(f"{mod_name} does not implement Content")
    return res




class InputAreaStatusIcon(Widget):
    ...


class InputAreaTextBox(Input):
    ...


class InputArea(Static):
    def compose(self) -> ComposeResult:
        with Horizontal(id="AppInputAreaLayout"):
            yield InputAreaTextBox()
            yield InputAreaStatusIcon()


class ContentAreaContainer(TabbedContent):
    ...


class ContentAreaScreen(Screen):
    def compose(self) -> ComposeResult:
        with Vertical(id="ContentAreaLayout"):
            yield Label(f"{config.PROJECT_ENV['NAME']} Client", id="AppTopBarLabel")
            with ContentAreaContainer():
                yield content.database.DatabaseTab()
            yield InputArea()


class LoginPrompt(Widget):
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


class HedgePyApp(App):
    CSS_PATH = load_css()
    TITLE = config.PROJECT_ENV['NAME']

    logged_in = reactive(False)

    def __init__(self):
        super().__init__()
        self.db_profile: Profile = None
        self.db_session: Session = None
        self.db_primary_connection_handle: str = None

    @property
    def main_db_connection(self) -> Connection:
        return self.db_session.get_connection(self.db_profile.uuid, self.db_primary_connection_handle)

    def compose(self) -> ComposeResult:
        yield ContentAreaScreen()

    def watch_logged_in(self, logged_in: bool) -> None:
        if logged_in:
            self.pop_screen()
        else:
            self.push_screen(LoginWindow())

    def on_login_prompt_login_message(self, message: LoginPrompt.LoginMessage):
        profile, session, connection = api.initialize(
            dbname=message.dbname, dbuser=message.dbuser, dbpass=message.dbpass, port=message.dbport
        )

        self.db_profile = profile
        self.db_session = session
        self.db_primary_connection_handle = connection.handle
        self.logged_in = True

    def on_database_tab_database_query(self, message: content.database.DatabaseTab.DatabaseQuery):
        res = api.execute_db_query(message.msg, conn=self.main_db_connection)
        self.query_one("DatabaseTab").results_queue.append(res)
