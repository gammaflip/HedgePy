import asyncio
from pathlib import Path

from textual import work
from textual.app import ComposeResult, App
from textual.reactive import reactive
from textual.widgets import TabbedContent, Label, Footer, TabPane

from src import config
from src.api import client
from src.api.bases import IO
from src.tui.bases import content
from src.tui.bases.widgets import LoginPrompt, LoginWindow

ROOT = Path(config.PROJECT_ENV['ROOT']) / 'src' / 'tui'


def load_css() -> list[str]:
    folder, ext, res = "css/", ".tcss", []
    for filename in (ROOT / 'css').glob(f"*{ext}"):
        res.append(folder + filename.stem + ext)
    return res


class HedgePyApp(App):
    CSS_PATH = load_css()
    TITLE = config.PROJECT_ENV['NAME']

    logged_in = reactive(False)

    async def ping(self):
        self.logged_in = await client.pong()

    def compose(self) -> ComposeResult:
        yield Label(f"{config.PROJECT_ENV['NAME']} Client", id="AppHeader")
        with TabbedContent(id="ContentArea"):
            with TabPane("database"):
                yield content.data.DataTree()
            with TabPane("templates"):
                yield content.templates.TemplateTab()
        yield Footer()

    def watch_logged_in(self, logged_in: bool) -> None:
        if logged_in:
            self.pop_screen()
        else:
            self.push_screen(LoginWindow())

    async def on_login_prompt_login_message(self, message: LoginPrompt.LoginMessage):
        self.logged_in = await client.connect(
            password=message.dbpass,
            user=message.dbuser,
            dbname=message.dbname,
            port=message.dbport
        )
        await self.query_one(content.data.DataTree).init()

    @work
    async def db_transaction(self, request: IO.DBRequest) -> IO.Result:
        return await client.db_transaction(request)

    async def on_request(self, request):
        request, callback = request.request, request.callback
        worker = self.db_transaction(request)
        if callback:
            await worker.wait()
            await callback(worker.result)


if __name__ == "__main__":
    asyncio.run(
        HedgePyApp().run_async()
    )
