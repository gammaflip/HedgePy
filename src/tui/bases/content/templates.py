import json
from src import config
from src.api.bases import Template
from pathlib import Path
from typing import Iterable
from textual import on
from textual.reactive import reactive
from textual.app import ComposeResult
from textual.widgets import TextArea, DirectoryTree, Button, Label, Static
from textual.containers import Horizontal, Vertical, Grid, Container


class _TemplateButton(Button):
    def __init__(self, name):
        super().__init__(name, classes="template-button")


class TemplateButtonNew(_TemplateButton):
    def __init__(self):
        super().__init__("New")


class TemplateButtonSave(_TemplateButton):
    def __init__(self):
        super().__init__("Save")


class TemplateButtonReset(_TemplateButton):
    def __init__(self):
        super().__init__("Reset")


class TemplateTree(DirectoryTree):
    def __init__(self):
        super().__init__(path=Path(config.PROJECT_ENV['ROOT']) / 'src' / 'api' / 'templates')

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return filter(lambda p: ((p.suffix == '.json') and not (str(p.stem).startswith('_'))), paths)


class TemplateTab(Container):

    selected = reactive(str)

    def compose(self) -> ComposeResult:
        yield TemplateTree()
        yield Label("", id="TemplateTabTextAreaLabel")
        yield TextArea(language="json", id="TemplateTabTextArea")
        yield TemplateButtonNew()
        yield TemplateButtonSave()
        yield TemplateButtonReset()

    def watch_selected(self, selected: str | None):
        text_area = self.query_one(TextArea)
        if selected:
            label = self.query_one(Label)
            label.update(selected)
            template = Template.get_template(selected)
            text = json.dumps(template, indent=4)
            text_area.load_text(text)
        else:
            text_area.load_text("")

    @on(TemplateButtonNew.Pressed)
    def new_template(self):
        ...

    @on(DirectoryTree.FileSelected)
    def load_template(self, message: DirectoryTree.FileSelected):
        self.selected = message.path.stem

    @on(TemplateButtonSave.Pressed)
    def save_template(self):
        text_area = self.query_one(TextArea)
        template = json.loads(text_area.text)
        Template.put_template(self.selected, template)

    @on(TemplateButtonReset.Pressed)
    def reset_template(self):
        selected, self.selected = self.selected, None
        self.selected = selected
